"""
Databricks Notebook: Complete Pipeline - Parse Logs + Enrich with Cache
Pipeline completo para análise de uso de notebooks do Databricks.

Este notebook combina todas as funcionalidades em um único lugar:
1. Parse de logs do Volume
2. Criação de cache de notebooks
3. Enrichment com informações dos notebooks

IMPORTANTE: Este código deve ser executado em um notebook Databricks.
"""

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

# Configurações do Volume e Tabelas
VOLUME_PATH = "/Volumes/hs_franquia/logs/hs-community-logs"
CACHE_TABLE = "hs_franquia.analytics.notebooks_cache"
OUTPUT_TABLE = "hs_franquia.analytics.notebook_usage_enriched"

# ============================================================================
# IMPORTS
# ============================================================================

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================================
# PARTE 1: PARSE DE LOGS DO VOLUME
# ============================================================================

class NotebookUsageParser:
    """Parser para extrair informações de uso de notebooks dos logs do cluster."""

    def __init__(self, cluster_log_path: str):
        self.cluster_log_path = cluster_log_path
        self.cluster_id = os.path.basename(cluster_log_path)
        self.cluster_name = None
        self.notebook_sessions = {}

    def parse_timestamp(self, line: str) -> Optional[datetime]:
        """Extrai timestamp de uma linha de log."""
        timestamp_pattern = r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
        match = re.search(timestamp_pattern, line)
        if match:
            timestamp_str = match.group(1)
            try:
                return datetime.strptime(timestamp_str, '%y/%m/%d %H:%M:%S')
            except ValueError:
                return None
        return None

    def extract_notebook_id(self, line: str) -> Optional[str]:
        """Extrai o ID do notebook de uma linha de log."""
        pattern = r'databricksnbid=(\d+)'
        match = re.search(pattern, line)
        if match:
            return match.group(1)
        return None

    def parse_cluster_info(self, driver_log_path: str) -> None:
        """Extrai informações básicas do cluster dos logs."""
        try:
            with open(driver_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if 'cluster_name' in line.lower() and not self.cluster_name:
                        name_match = re.search(r'cluster[_-]?name["\s:=]+([^,\s"\']+)', line, re.IGNORECASE)
                        if name_match:
                            self.cluster_name = name_match.group(1)
                            break
        except Exception as e:
            print(f"Warning: Could not parse cluster info: {e}")

    def parse_notebook_usage(self) -> pd.DataFrame:
        """Processa os logs e extrai informações de uso de notebooks."""
        driver_log = os.path.join(self.cluster_log_path, 'driver', 'log4j-active.log')

        print(f"Processing cluster: {self.cluster_id}")

        self.parse_cluster_info(driver_log)
        active_sessions = {}

        try:
            with open(driver_log, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    timestamp = self.parse_timestamp(line)
                    if not timestamp:
                        continue

                    notebook_id = self.extract_notebook_id(line)
                    if not notebook_id:
                        continue

                    session_match = re.search(r'session[_\s]?id["\s:=]+(\d+)', line, re.IGNORECASE)
                    if not session_match:
                        session_match = re.search(r'contextuuid["\s:=]+(\d+)', line, re.IGNORECASE)

                    session_id = session_match.group(1) if session_match else str(hash(notebook_id) % 10**9)

                    if session_id not in active_sessions:
                        active_sessions[session_id] = {
                            'notebook_ids': set(),
                            'start_time': timestamp,
                            'end_time': timestamp
                        }

                    session_info = active_sessions[session_id]
                    session_info['notebook_ids'].add(notebook_id)

                    if session_info['start_time'] and session_info['start_time'] <= timestamp:
                        if session_info['end_time'] is None or timestamp > session_info['end_time']:
                            session_info['end_time'] = timestamp

        except FileNotFoundError:
            print(f"Warning: Log file not found: {driver_log}")
            return pd.DataFrame()

        records = []
        for session_id, info in active_sessions.items():
            notebook_ids = sorted(list(info['notebook_ids']))
            duration = None
            if info['start_time'] and info['end_time']:
                duration = (info['end_time'] - info['start_time']).total_seconds()

            records.append({
                'cluster_id': self.cluster_id,
                'cluster_name': self.cluster_name or 'Unknown',
                'session_id': session_id,
                'notebook_ids': ', '.join(notebook_ids),
                'num_notebooks': len(notebook_ids),
                'start_time': info['start_time'].strftime('%Y-%m-%d %H:%M:%S') if info['start_time'] else None,
                'end_time': info['end_time'].strftime('%Y-%m-%d %H:%M:%S') if info['end_time'] else None,
                'duration_seconds': duration
            })

        df = pd.DataFrame(records)
        print(f"  Found {len(df)} sessions")
        return df


def parse_logs_from_volume(volume_path: str) -> DataFrame:
    """
    Escaneia o Volume e processa todos os logs de clusters.

    Args:
        volume_path: Caminho do Volume com os logs

    Returns:
        Spark DataFrame com uso de notebooks
    """
    print("="*100)
    print("STEP 1: PARSING LOGS FROM VOLUME")
    print("="*100)
    print(f"Volume path: {volume_path}\n")

    cluster_dirs = []

    # Scan for cluster directories
    try:
        for entry in os.listdir(volume_path):
            entry_path = os.path.join(volume_path, entry)
            if os.path.isdir(entry_path):
                driver_path = os.path.join(entry_path, 'driver')
                if os.path.exists(driver_path) and os.path.isdir(driver_path):
                    cluster_dirs.append(entry_path)
                    print(f"Found cluster: {entry}")
    except Exception as e:
        print(f"Error scanning volume: {e}")
        return None

    print(f"\nTotal clusters found: {len(cluster_dirs)}\n")

    if not cluster_dirs:
        print("No cluster directories found!")
        return None

    # Process each cluster
    all_data = []
    for cluster_dir in cluster_dirs:
        parser = NotebookUsageParser(cluster_dir)
        df = parser.parse_notebook_usage()
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("No usage data found!")
        return None

    # Combine and convert to Spark
    final_df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal sessions found: {len(final_df)}")

    spark_df = spark.createDataFrame(final_df)
    return spark_df


# ============================================================================
# PARTE 2: CRIAÇÃO DE CACHE DE NOTEBOOKS
# ============================================================================

class NotebookCacheFetcher:
    """Cliente para buscar todos os notebooks do workspace."""

    def __init__(self):
        self.w = WorkspaceClient()
        self.notebooks = []

    def search_workspace(self, path: str = '/', level: int = 0) -> None:
        """Lista notebooks recursivamente."""
        indent = "  " * level
        print(f"{indent}Scanning: {path}")

        try:
            objects = list(self.w.workspace.list(path))
            notebook_count = 0
            dir_count = 0

            for obj in objects:
                if obj.object_type == ObjectType.DIRECTORY:
                    dir_count += 1
                    self.search_workspace(obj.path, level + 1)
                elif obj.object_type == ObjectType.NOTEBOOK:
                    notebook_count += 1
                    path_parts = obj.path.split('/')
                    owner = 'Shared'
                    if '/Users/' in obj.path:
                        user_match = obj.path.split('/Users/')
                        if len(user_match) > 1:
                            owner = user_match[1].split('/')[0]

                    self.notebooks.append({
                        'object_id': obj.object_id,
                        'path': obj.path,
                        'name': path_parts[-1] if path_parts else 'Unknown',
                        'language': obj.language.value if obj.language else 'Unknown',
                        'owner': owner,
                        'created_at': obj.created_at,
                        'modified_at': obj.modified_at
                    })

            if notebook_count > 0 or dir_count > 0:
                print(f"{indent}  Found: {notebook_count} notebooks, {dir_count} directories")

        except Exception as e:
            print(f"{indent}  Error: {e}")

    def get_all_notebooks(self) -> pd.DataFrame:
        """Busca todos os notebooks."""
        self.notebooks = []
        self.search_workspace('/')

        df = pd.DataFrame(self.notebooks)
        df['fetched_at'] = datetime.now().isoformat()

        return df


def create_notebooks_cache(save_table: str = None) -> DataFrame:
    """
    Cria cache de todos os notebooks do workspace.

    Args:
        save_table: Nome da tabela para salvar o cache

    Returns:
        Spark DataFrame com cache
    """
    print("\n" + "="*100)
    print("STEP 2: CREATING NOTEBOOKS CACHE")
    print("="*100)
    print("Fetching all notebooks from workspace...\n")

    fetcher = NotebookCacheFetcher()
    df_cache = fetcher.get_all_notebooks()

    print(f"\nTotal notebooks found: {len(df_cache)}")

    if df_cache.empty:
        print("No notebooks found!")
        return None

    spark_df = spark.createDataFrame(df_cache)

    if save_table:
        print(f"\nSaving cache to table: {save_table}")
        spark_df.write.format("delta").mode("overwrite").saveAsTable(save_table)

    return spark_df


# ============================================================================
# PARTE 3: ENRICHMENT
# ============================================================================

def enrich_with_cache(usage_df: DataFrame, cache_df: DataFrame) -> DataFrame:
    """
    Enriquece uso de notebooks com informações do cache.

    Args:
        usage_df: DataFrame com uso
        cache_df: DataFrame com cache de notebooks

    Returns:
        DataFrame enriquecido
    """
    print("\n" + "="*100)
    print("STEP 3: ENRICHING WITH NOTEBOOK INFORMATION")
    print("="*100)

    # Explode notebook IDs
    df_exploded = usage_df.withColumn(
        "notebook_id",
        F.explode(F.split(F.col("notebook_ids"), ",\\s*"))
    )

    df_exploded = df_exploded.withColumn("notebook_id", F.trim(F.col("notebook_id")))
    cache_prep = cache_df.withColumn("object_id", F.col("object_id").cast("string"))

    # Join with cache
    df_matched = df_exploded.join(
        cache_prep.select(
            F.col("object_id").alias("notebook_id"),
            F.col("name").alias("notebook_name"),
            F.col("path").alias("notebook_path"),
            F.col("language").alias("notebook_language"),
            F.col("owner").alias("notebook_owner")
        ),
        on="notebook_id",
        how="left"
    )

    # Handle unknowns
    df_matched = df_matched.fillna({
        "notebook_name": "Unknown",
        "notebook_path": "Unknown",
        "notebook_language": "Unknown",
        "notebook_owner": "Unknown"
    })

    df_matched = df_matched.withColumn(
        "notebook_path",
        F.when(
            F.col("notebook_path") == "Unknown",
            F.concat(F.lit("Unknown (ID: "), F.col("notebook_id"), F.lit(")"))
        ).otherwise(F.col("notebook_path"))
    )

    # Aggregate
    df_enriched = df_matched.groupBy(
        "cluster_id", "cluster_name", "session_id", "notebook_ids",
        "num_notebooks", "start_time", "end_time", "duration_seconds"
    ).agg(
        F.concat_ws(" | ", F.collect_list("notebook_name")).alias("notebook_names"),
        F.concat_ws(" | ", F.collect_list("notebook_path")).alias("notebook_paths"),
        F.concat_ws(" | ", F.array_distinct(F.collect_list("notebook_language"))).alias("notebook_languages"),
        F.concat_ws(" | ", F.array_distinct(F.collect_list("notebook_owner"))).alias("notebook_owners")
    )

    # Reorder columns
    df_enriched = df_enriched.select(
        "cluster_id", "cluster_name", "session_id",
        "notebook_ids", "notebook_names", "notebook_paths",
        "notebook_languages", "notebook_owners",
        "num_notebooks", "start_time", "end_time", "duration_seconds"
    )

    total = df_enriched.count()
    matched = df_enriched.filter(~F.col("notebook_paths").contains("Unknown (ID:")).count()
    print(f"\nEnriched {total} sessions")
    print(f"Fully matched sessions: {matched}/{total}")

    return df_enriched


# ============================================================================
# PIPELINE COMPLETO
# ============================================================================

def run_complete_pipeline(
    volume_path: str = VOLUME_PATH,
    cache_table: str = CACHE_TABLE,
    output_table: str = OUTPUT_TABLE,
    skip_cache: bool = False
):
    """
    Executa o pipeline completo.

    Args:
        volume_path: Caminho do Volume com logs
        cache_table: Tabela para cache de notebooks
        output_table: Tabela para resultado final
        skip_cache: Se True, usa cache existente ao invés de criar novo

    Returns:
        DataFrame enriquecido
    """
    print("="*100)
    print("DATABRICKS NOTEBOOK USAGE ANALYSIS - COMPLETE PIPELINE")
    print("="*100)
    print(f"\nConfiguration:")
    print(f"  Volume Path: {volume_path}")
    print(f"  Cache Table: {cache_table}")
    print(f"  Output Table: {output_table}")
    print(f"  Skip Cache Creation: {skip_cache}")

    # Step 1: Parse logs
    df_usage = parse_logs_from_volume(volume_path)
    if df_usage is None:
        print("\nPipeline stopped: No usage data found")
        return None

    # Step 2: Create or load cache
    if skip_cache:
        print("\n" + "="*100)
        print("STEP 2: LOADING EXISTING CACHE")
        print("="*100)
        print(f"Loading from: {cache_table}")
        df_cache = spark.table(cache_table)
    else:
        df_cache = create_notebooks_cache(save_table=cache_table)
        if df_cache is None:
            print("\nPipeline stopped: Could not create cache")
            return None

    # Step 3: Enrich
    df_enriched = enrich_with_cache(df_usage, df_cache)

    # Save result
    print("\n" + "="*100)
    print("SAVING RESULTS")
    print("="*100)
    print(f"Saving to table: {output_table}")
    df_enriched.write.format("delta").mode("overwrite").saveAsTable(output_table)
    print("Results saved successfully!")

    # Summary
    print("\n" + "="*100)
    print("PIPELINE COMPLETED")
    print("="*100)
    print(f"\nResults available at: {output_table}")
    print("\nTo view results:")
    print(f"  df = spark.table('{output_table}')")
    print("  display(df)")

    return df_enriched


# ============================================================================
# INSTRUÇÕES DE USO
# ============================================================================
#
# OPÇÃO 1: Pipeline completo (recomendado)
# -----------------------------------------
# df_result = run_complete_pipeline()
# display(df_result)
#
#
# OPÇÃO 2: Pipeline com cache existente (mais rápido)
# ----------------------------------------------------
# df_result = run_complete_pipeline(skip_cache=True)
# display(df_result)
#
#
# OPÇÃO 3: Executar etapas separadamente
# ----------------------------------------
# # Etapa 1: Parse logs
# df_usage = parse_logs_from_volume(VOLUME_PATH)
# display(df_usage)
#
# # Etapa 2: Criar cache
# df_cache = create_notebooks_cache(save_table=CACHE_TABLE)
# display(df_cache)
#
# # Etapa 3: Enrich
# df_enriched = enrich_with_cache(df_usage, df_cache)
# display(df_enriched)
#
# # Salvar
# df_enriched.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
#
#
# OPÇÃO 4: Customizar configurações
# -----------------------------------
# df_result = run_complete_pipeline(
#     volume_path="/Volumes/seu_catalogo/seu_schema/seus_logs",
#     cache_table="seu_catalogo.seu_schema.notebooks_cache",
#     output_table="seu_catalogo.seu_schema.notebook_usage"
# )
#
# ============================================================================

# Descomente para executar automaticamente:
# df_result = run_complete_pipeline()
# display(df_result)
