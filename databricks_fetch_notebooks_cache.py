"""
Databricks Notebook: Fetch All Notebooks and Create Cache
Versão adaptada para rodar no Databricks usando Databricks SDK.

IMPORTANTE: Este código deve ser executado em um notebook Databricks.
Não precisa de token - usa autenticação nativa do Databricks.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType
import pandas as pd
from datetime import datetime
from typing import List, Dict


class NotebookCacheFetcher:
    """Cliente para buscar todos os notebooks do workspace usando SDK."""

    def __init__(self):
        """Inicializa o cliente usando autenticação nativa do Databricks."""
        self.w = WorkspaceClient()
        self.notebooks = []

    def search_workspace(self, path: str = '/', level: int = 0) -> None:
        """
        Lista objetos no workspace recursivamente e coleta notebooks.

        Args:
            path: Caminho para listar
            level: Nível de recursão (para indentação)
        """
        indent = "  " * level
        print(f"{indent}Scanning: {path}")

        try:
            # List objects in the path
            objects = list(self.w.workspace.list(path))
            notebook_count = 0
            dir_count = 0

            for obj in objects:
                # If it's a directory, recurse
                if obj.object_type == ObjectType.DIRECTORY:
                    dir_count += 1
                    self.search_workspace(obj.path, level + 1)

                # If it's a notebook, save it
                elif obj.object_type == ObjectType.NOTEBOOK:
                    notebook_count += 1
                    notebook_info = {
                        'object_id': obj.object_id,
                        'path': obj.path,
                        'language': obj.language.value if obj.language else 'Unknown',
                        'created_at': obj.created_at,
                        'modified_at': obj.modified_at
                    }

                    # Extract notebook name from path
                    path_parts = obj.path.split('/')
                    notebook_info['name'] = path_parts[-1] if path_parts else 'Unknown'

                    # Extract user/owner from path if in /Users/ directory
                    if '/Users/' in obj.path:
                        user_match = obj.path.split('/Users/')
                        if len(user_match) > 1:
                            user_part = user_match[1].split('/')[0]
                            notebook_info['owner'] = user_part
                        else:
                            notebook_info['owner'] = 'Unknown'
                    else:
                        notebook_info['owner'] = 'Shared'

                    self.notebooks.append(notebook_info)

            if notebook_count > 0 or dir_count > 0:
                print(f"{indent}   Found: {notebook_count} notebooks, {dir_count} directories")

        except Exception as e:
            print(f"{indent}   Error listing workspace at {path}: {e}")

    def get_all_notebooks(self) -> pd.DataFrame:
        """
        Busca todos os notebooks e retorna como DataFrame.

        Returns:
            DataFrame com informações de todos os notebooks
        """
        print("\n" + "="*100)
        print("FETCHING ALL NOTEBOOKS FROM WORKSPACE")
        print("="*100)
        print("This may take several minutes depending on workspace size...\n")

        # Reset notebooks list
        self.notebooks = []

        # Scan workspace
        self.search_workspace('/')

        print("\n" + "="*100)
        print("CREATING NOTEBOOKS CACHE")
        print("="*100)

        # Create DataFrame
        df = pd.DataFrame(self.notebooks)

        # Add fetch timestamp
        df['fetched_at'] = datetime.now().isoformat()

        # Reorder columns
        columns_order = ['object_id', 'name', 'path', 'language', 'owner',
                        'created_at', 'modified_at', 'fetched_at']

        # Only use columns that exist
        columns_order = [col for col in columns_order if col in df.columns]
        other_columns = [col for col in df.columns if col not in columns_order]
        df = df[columns_order + other_columns]

        print(f"\nTotal notebooks found: {len(df)}")
        if len(df) > 0:
            print(f"Languages: {', '.join(df['language'].unique())}")
            print(f"Owners: {df['owner'].nunique()} unique owners")

        return df


def main(output_table: str = None, return_type: str = 'spark'):
    """
    Função principal.

    Args:
        output_table: Nome da tabela Delta para salvar o cache (opcional)
        return_type: 'spark' para retornar Spark DataFrame, 'pandas' para Pandas DataFrame

    Returns:
        DataFrame com cache de notebooks
    """
    print("="*100)
    print("DATABRICKS NOTEBOOKS CACHE BUILDER")
    print("="*100)

    # Fetch all notebooks
    try:
        fetcher = NotebookCacheFetcher()
        df_notebooks = fetcher.get_all_notebooks()

        if df_notebooks.empty:
            print("\nNo notebooks found in workspace!")
            return None

        print("\n" + "="*100)
        print("CACHE CREATED")
        print("="*100)
        print(f"Total notebooks: {len(df_notebooks)}")

        # Save to Delta table if specified
        if output_table:
            print(f"\nSaving cache to Delta table: {output_table}")
            spark_df = spark.createDataFrame(df_notebooks)
            spark_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
            print(f"Cache saved to table: {output_table}")

            # Return the table
            result_df = spark.table(output_table)
        else:
            # Convert to Spark DataFrame
            result_df = spark.createDataFrame(df_notebooks)

        # Show sample
        print("\n" + "="*100)
        print("SAMPLE OF NOTEBOOKS (first 10)")
        print("="*100)
        display(result_df.limit(10))

        # Return in requested format
        if return_type == 'pandas':
            return df_notebooks
        else:
            return result_df

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# INSTRUÇÕES DE USO NO DATABRICKS NOTEBOOK
# ============================================================================
#
# 1. Cole este código em uma célula Python no Databricks
#
# 2. Execute para criar o cache e visualizar:
#    df_cache = main()
#    display(df_cache)
#
# 3. Ou salve diretamente em uma tabela Delta:
#    df_cache = main(output_table="hs_franquia.analytics.notebooks_cache")
#
# 4. Para usar depois com o enrichment:
#    # Primeiro crie o cache
#    df_cache = main(output_table="hs_franquia.analytics.notebooks_cache")
#
#    # Depois use no enrichment passando a tabela
#    df_enriched = enrich_with_cache(
#        usage_df=df_usage,
#        cache_table="hs_franquia.analytics.notebooks_cache"
#    )
#
# ============================================================================

# Descomente para executar automaticamente:
# df_cache = main(output_table="hs_franquia.analytics.notebooks_cache")
