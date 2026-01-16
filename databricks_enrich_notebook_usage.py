"""
Databricks Notebook: Enrich Notebook Usage with Cache
Versão adaptada para rodar no Databricks usando tabelas Delta.

IMPORTANTE: Este código deve ser executado em um notebook Databricks.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def enrich_with_cache(
    usage_df: DataFrame,
    cache_table: str = None,
    cache_df: DataFrame = None
) -> DataFrame:
    """
    Enriquece o DataFrame de uso de notebooks com informações do cache.

    Args:
        usage_df: DataFrame com uso de notebooks (da função parse)
        cache_table: Nome da tabela Delta com cache de notebooks (opcional)
        cache_df: DataFrame direto com cache (opcional, alternativa ao cache_table)

    Returns:
        DataFrame enriquecido com paths, names, languages, owners
    """
    print("\n" + "="*100)
    print("ENRICHING NOTEBOOK USAGE DATA")
    print("="*100)

    # Load cache
    if cache_df is not None:
        df_cache = cache_df
        print("Using provided cache DataFrame")
    elif cache_table:
        print(f"Loading cache from table: {cache_table}")
        df_cache = spark.table(cache_table)
    else:
        raise ValueError("Either cache_table or cache_df must be provided!")

    # Show cache stats
    cache_count = df_cache.count()
    print(f"Loaded {cache_count} notebooks from cache\n")

    print("="*100)
    print("MATCHING NOTEBOOK IDs TO PATHS")
    print("="*100)

    # Explode notebook_ids (split comma-separated values into rows)
    df_exploded = usage_df.withColumn(
        "notebook_id",
        F.explode(F.split(F.col("notebook_ids"), ",\\s*"))
    )

    # Cast to string for joining
    df_exploded = df_exploded.withColumn("notebook_id", F.trim(F.col("notebook_id")))
    df_cache_prep = df_cache.withColumn("object_id", F.col("object_id").cast("string"))

    # Join with cache
    df_matched = df_exploded.join(
        df_cache_prep.select(
            F.col("object_id").alias("notebook_id"),
            F.col("name").alias("notebook_name"),
            F.col("path").alias("notebook_path"),
            F.col("language").alias("notebook_language"),
            F.col("owner").alias("notebook_owner")
        ),
        on="notebook_id",
        how="left"
    )

    # Fill unknown values
    df_matched = df_matched.fillna({
        "notebook_name": "Unknown",
        "notebook_path": "Unknown",
        "notebook_language": "Unknown",
        "notebook_owner": "Unknown"
    })

    # Add "Unknown (ID: xxx)" for paths that weren't found
    df_matched = df_matched.withColumn(
        "notebook_path",
        F.when(
            F.col("notebook_path") == "Unknown",
            F.concat(F.lit("Unknown (ID: "), F.col("notebook_id"), F.lit(")"))
        ).otherwise(F.col("notebook_path"))
    )

    # Aggregate back to original grouping
    df_enriched = df_matched.groupBy(
        "cluster_id",
        "cluster_name",
        "session_id",
        "notebook_ids",
        "num_notebooks",
        "start_time",
        "end_time",
        "duration_seconds"
    ).agg(
        F.concat_ws(" | ", F.collect_list("notebook_name")).alias("notebook_names"),
        F.concat_ws(" | ", F.collect_list("notebook_path")).alias("notebook_paths"),
        F.concat_ws(" | ", F.array_distinct(F.collect_list("notebook_language"))).alias("notebook_languages"),
        F.concat_ws(" | ", F.array_distinct(F.collect_list("notebook_owner"))).alias("notebook_owners")
    )

    # Reorder columns
    df_enriched = df_enriched.select(
        "cluster_id",
        "cluster_name",
        "session_id",
        "notebook_ids",
        "notebook_names",
        "notebook_paths",
        "notebook_languages",
        "notebook_owners",
        "num_notebooks",
        "start_time",
        "end_time",
        "duration_seconds"
    )

    # Show matching statistics
    total_sessions = df_enriched.count()
    matched_sessions = df_enriched.filter(~F.col("notebook_paths").contains("Unknown (ID:")).count()

    print(f"\nSuccessfully enriched {total_sessions} sessions")
    print(f"Sessions with all notebooks matched: {matched_sessions}/{total_sessions}")
    if matched_sessions < total_sessions:
        print(f"Note: Some notebooks could not be matched - this is normal")
        print("The internal notebook ID may differ from object_id in some cases")

    return df_enriched


def main(
    volume_path: str = "/Volumes/hs_franquia/logs/hs-community-logs",
    cache_table: str = "hs_franquia.analytics.notebooks_cache",
    output_table: str = None
):
    """
    Pipeline completo: parse logs do volume + enrich com cache.

    Args:
        volume_path: Caminho do Volume com os logs
        cache_table: Nome da tabela com cache de notebooks
        output_table: Nome da tabela para salvar resultado enriquecido (opcional)

    Returns:
        DataFrame enriquecido
    """
    print("="*100)
    print("COMPLETE PIPELINE: PARSE + ENRICH")
    print("="*100)

    # Step 1: Parse notebook usage from volume logs
    print("\nStep 1: Parsing notebook usage from logs...")
    print("-"*100)

    # Import the parse function (assuming it's available in the notebook)
    # You would need to run databricks_parse_notebook_usage.py first
    # or combine both scripts

    # For now, assume df_usage already exists or is passed
    # df_usage = parse_from_volume(volume_path)

    print("\nNote: Run databricks_parse_notebook_usage.py first to get df_usage")
    print("Then pass the result to this enrichment function:\n")
    print("# df_usage = main_parse()")
    print("# df_enriched = enrich_with_cache(df_usage, cache_table='...')\n")

    return None


# ============================================================================
# INSTRUÇÕES DE USO NO DATABRICKS NOTEBOOK
# ============================================================================
#
# SETUP COMPLETO (Execute as células nesta ordem):
#
# Célula 1: Criar cache de notebooks
# ----------------------------------------
# %run ./databricks_fetch_notebooks_cache
# df_cache = main(output_table="hs_franquia.analytics.notebooks_cache")
#
#
# Célula 2: Parse logs do volume
# ----------------------------------------
# %run ./databricks_parse_notebook_usage
# df_usage = main(
#     volume_path="/Volumes/hs_franquia/logs/hs-community-logs"
# )
#
#
# Célula 3: Enrich com cache
# ----------------------------------------
# df_enriched = enrich_with_cache(
#     usage_df=df_usage,
#     cache_table="hs_franquia.analytics.notebooks_cache"
# )
#
# # Visualizar
# display(df_enriched)
#
# # Ou salvar em tabela
# df_enriched.write.format("delta").mode("overwrite") \
#     .saveAsTable("hs_franquia.analytics.notebook_usage_enriched")
#
#
# ALTERNATIVA: Tudo em uma célula
# ----------------------------------------
# # 1. Criar cache
# %run ./databricks_fetch_notebooks_cache
# cache_df = main(output_table="hs_franquia.analytics.notebooks_cache")
#
# # 2. Parse usage
# %run ./databricks_parse_notebook_usage
# usage_df = main()
#
# # 3. Enrich
# enriched_df = enrich_with_cache(
#     usage_df=usage_df,
#     cache_table="hs_franquia.analytics.notebooks_cache"
# )
#
# # 4. Save result
# enriched_df.write.format("delta").mode("overwrite") \
#     .saveAsTable("hs_franquia.analytics.notebook_usage_enriched")
#
# display(enriched_df)
#
# ============================================================================
