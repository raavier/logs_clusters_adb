"""
Databricks Notebook: Parse Notebook Usage from Volume Logs
Versão adaptada para rodar no Databricks consumindo logs do Volume.

IMPORTANTE: Este código deve ser executado em um notebook Databricks.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import re

# Configuração do Volume
VOLUME_PATH = "/Volumes/hs_franquia/logs/hs-community-logs"


class NotebookUsageParser:
    """Parser para extrair informações de uso de notebooks dos logs do cluster."""

    def __init__(self, cluster_log_path: str):
        """
        Inicializa o parser com o caminho dos logs do cluster.

        Args:
            cluster_log_path: Caminho para o diretório de logs do cluster no Volume
        """
        self.cluster_log_path = cluster_log_path
        self.cluster_id = os.path.basename(cluster_log_path)
        self.cluster_name = None
        self.notebook_sessions = {}  # session_id -> session info

    def parse_timestamp(self, line: str) -> Optional[datetime]:
        """
        Extrai timestamp de uma linha de log.

        Args:
            line: Linha do log

        Returns:
            datetime object ou None se não encontrado
        """
        # Format: 26/01/16 13:05:19
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
        """
        Extrai o ID do notebook de uma linha de log.

        Args:
            line: Linha do log

        Returns:
            Notebook ID ou None se não encontrado
        """
        # Look for databricksnbid pattern
        pattern = r'databricksnbid=(\d+)'
        match = re.search(pattern, line)
        if match:
            return match.group(1)
        return None

    def parse_cluster_info(self, driver_log_path: str) -> None:
        """
        Extrai informações básicas do cluster dos logs.

        Args:
            driver_log_path: Caminho para o arquivo de log do driver
        """
        try:
            with open(driver_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    # Extract cluster name
                    if 'cluster_name' in line.lower() and not self.cluster_name:
                        # Try to find cluster name in various formats
                        name_match = re.search(r'cluster[_-]?name["\s:=]+([^,\s"\']+)', line, re.IGNORECASE)
                        if name_match:
                            self.cluster_name = name_match.group(1)
                            break
        except Exception as e:
            print(f"Warning: Could not parse cluster info: {e}")

    def parse_notebook_usage(self) -> pd.DataFrame:
        """
        Processa os logs e extrai informações de uso de notebooks.

        Returns:
            DataFrame com informações de uso
        """
        driver_log = os.path.join(self.cluster_log_path, 'driver', 'log4j-active.log')

        print(f"Processing cluster: {self.cluster_id}")
        print(f"Log file: {driver_log}")

        # Parse cluster info first
        self.parse_cluster_info(driver_log)

        # Track active sessions
        active_sessions = {}  # session_id -> {notebook_ids, start_time, end_time}

        try:
            with open(driver_log, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    timestamp = self.parse_timestamp(line)
                    if not timestamp:
                        continue

                    # Look for notebook activity
                    notebook_id = self.extract_notebook_id(line)
                    if not notebook_id:
                        continue

                    # Extract session ID if present
                    session_match = re.search(r'session[_\s]?id["\s:=]+(\d+)', line, re.IGNORECASE)
                    if not session_match:
                        # Try alternative session patterns
                        session_match = re.search(r'contextuuid["\s:=]+(\d+)', line, re.IGNORECASE)

                    session_id = session_match.group(1) if session_match else str(hash(notebook_id) % 10**9)

                    # Update or create session
                    if session_id not in active_sessions:
                        active_sessions[session_id] = {
                            'notebook_ids': set(),
                            'start_time': timestamp,
                            'end_time': timestamp
                        }

                    session_info = active_sessions[session_id]
                    session_info['notebook_ids'].add(notebook_id)

                    # Update times
                    if session_info['start_time'] and session_info['start_time'] <= timestamp:
                        if session_info['end_time'] is None or timestamp > session_info['end_time']:
                            session_info['end_time'] = timestamp

        except FileNotFoundError:
            print(f"Warning: Log file not found: {driver_log}")
            return pd.DataFrame()

        # Convert to DataFrame
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
        print(f"Found {len(df)} notebook sessions")

        return df


def scan_volume_for_clusters(volume_path: str) -> List[str]:
    """
    Escaneia o Volume em busca de diretórios de logs de clusters.

    Args:
        volume_path: Caminho base do Volume

    Returns:
        Lista de caminhos para diretórios de clusters
    """
    cluster_dirs = []

    print(f"Scanning volume: {volume_path}")

    try:
        # List directories in volume
        for entry in os.listdir(volume_path):
            entry_path = os.path.join(volume_path, entry)
            if os.path.isdir(entry_path):
                # Check if it looks like a cluster directory (has driver/ subdirectory)
                driver_path = os.path.join(entry_path, 'driver')
                if os.path.exists(driver_path) and os.path.isdir(driver_path):
                    cluster_dirs.append(entry_path)
                    print(f"  Found cluster: {entry}")

    except Exception as e:
        print(f"Error scanning volume: {e}")

    print(f"\nTotal clusters found: {len(cluster_dirs)}")
    return cluster_dirs


def main(volume_path: str = VOLUME_PATH, output_table: str = None):
    """
    Função principal para processar todos os clusters no Volume.

    Args:
        volume_path: Caminho do Volume com os logs
        output_table: Nome da tabela Delta para salvar (opcional)
    """
    print("="*100)
    print("DATABRICKS NOTEBOOK USAGE ANALYSIS - VOLUME VERSION")
    print("="*100)
    print(f"Volume path: {volume_path}\n")

    # Scan for clusters
    cluster_dirs = scan_volume_for_clusters(volume_path)

    if not cluster_dirs:
        print("\nNo cluster directories found in volume!")
        return None

    # Process each cluster
    all_data = []
    for cluster_dir in cluster_dirs:
        print(f"\n{'='*100}")
        parser = NotebookUsageParser(cluster_dir)
        df = parser.parse_notebook_usage()
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("\nNo notebook usage data found!")
        return None

    # Combine all data
    final_df = pd.concat(all_data, ignore_index=True)

    print("\n" + "="*100)
    print("COMBINED RESULTS")
    print("="*100)
    print(final_df.to_string(index=False))

    # Save to Delta table if specified
    if output_table:
        print(f"\nSaving to Delta table: {output_table}")
        spark_df = spark.createDataFrame(final_df)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
        print(f"Data saved to table: {output_table}")
    else:
        # Return as Spark DataFrame for further processing
        print("\nConverting to Spark DataFrame...")
        spark_df = spark.createDataFrame(final_df)
        return spark_df

    return spark.table(output_table)


# ============================================================================
# INSTRUÇÕES DE USO NO DATABRICKS NOTEBOOK
# ============================================================================
#
# 1. Cole este código em uma célula Python no Databricks
#
# 2. Execute para processar todos os clusters:
#    df = main()
#    display(df)
#
# 3. Ou salve diretamente em uma tabela Delta:
#    main(output_table="hs_franquia.analytics.notebook_usage")
#
# 4. Para um volume específico:
#    df = main(volume_path="/Volumes/seu_catalogo/seu_schema/seus_logs")
#
# ============================================================================

# Descomente para executar automaticamente:
# df_result = main()
# display(df_result)
