"""
Script para buscar TODOS os notebooks do workspace Databricks e criar um cache.
Este cache pode ser reutilizado para enriquecer múltiplos relatórios sem precisar
fazer nova chamada à API toda vez.
"""

import os
import pandas as pd
import requests
from typing import Dict, Optional, List
from datetime import datetime

# Try to import python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")


class NotebookFetcher:
    """Cliente para buscar todos os notebooks do workspace Databricks."""

    def __init__(self, workspace_url: str, token: str):
        """
        Inicializa o cliente da API do Databricks.

        Args:
            workspace_url: URL do workspace Databricks
            token: Personal Access Token do Databricks
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        self.notebooks = []

    def search_workspace(self, path: str = '/', level: int = 0) -> None:
        """
        Lista objetos no workspace recursivamente e coleta notebooks.

        Args:
            path: Caminho para listar
            level: Nível de recursão (para indentação)
        """
        url = f'{self.workspace_url}/api/2.0/workspace/list'
        params = {'path': path}

        indent = "  " * level
        print(f"{indent}📂 Scanning: {path}")

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()

            objects = data.get('objects', [])
            notebook_count = 0
            dir_count = 0

            for obj in objects:
                obj_type = obj.get('object_type')

                # If it's a directory, recurse
                if obj_type == 'DIRECTORY':
                    dir_count += 1
                    self.search_workspace(obj['path'], level + 1)

                # If it's a notebook, save it
                elif obj_type == 'NOTEBOOK':
                    notebook_count += 1
                    notebook_info = {
                        'object_id': obj.get('object_id'),
                        'path': obj.get('path'),
                        'language': obj.get('language', 'Unknown'),
                        'created_at': obj.get('created_at'),
                        'modified_at': obj.get('modified_at')
                    }

                    # Extract notebook name from path
                    path_parts = obj.get('path', '').split('/')
                    notebook_info['name'] = path_parts[-1] if path_parts else 'Unknown'

                    # Extract user/owner from path if in /Users/ directory
                    if '/Users/' in obj.get('path', ''):
                        user_match = obj.get('path', '').split('/Users/')
                        if len(user_match) > 1:
                            user_part = user_match[1].split('/')[0]
                            notebook_info['owner'] = user_part
                        else:
                            notebook_info['owner'] = 'Unknown'
                    else:
                        notebook_info['owner'] = 'Shared'

                    self.notebooks.append(notebook_info)

            if notebook_count > 0 or dir_count > 0:
                print(f"{indent}   ✓ Found: {notebook_count} notebooks, {dir_count} directories")

        except requests.exceptions.RequestException as e:
            print(f"{indent}   ✗ Error listing workspace at {path}: {e}")

    def get_all_notebooks(self) -> pd.DataFrame:
        """
        Busca todos os notebooks e retorna como DataFrame.

        Returns:
            DataFrame com informações de todos os notebooks
        """
        print("\n" + "="*100)
        print("📡 FETCHING ALL NOTEBOOKS FROM WORKSPACE")
        print("="*100)
        print("This may take several minutes depending on workspace size...\n")

        # Reset notebooks list
        self.notebooks = []

        # Scan workspace
        self.search_workspace('/')

        print("\n" + "="*100)
        print("📊 CREATING NOTEBOOKS CACHE")
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

        print(f"\n✓ Total notebooks found: {len(df)}")
        if len(df) > 0:
            print(f"✓ Languages: {', '.join(df['language'].unique())}")
            print(f"✓ Owners: {df['owner'].nunique()} unique owners")

        return df


def main():
    """Função principal."""
    print("="*100)
    print("DATABRICKS NOTEBOOKS CACHE BUILDER")
    print("="*100)

    # Get credentials from environment
    workspace_url = os.environ.get('DATABRICKS_WORKSPACE_URL')
    token = os.environ.get('DATABRICKS_TOKEN')

    if not workspace_url:
        workspace_url = input("\nEnter Databricks workspace URL: ").strip()

    if not token:
        print("\nEnter Databricks Personal Access Token:")
        token = input("Token: ").strip()

    if not workspace_url or not token:
        print("\nError: Workspace URL and Token are required!")
        return

    print(f"\nConnecting to {workspace_url}...")

    # Fetch all notebooks
    try:
        fetcher = NotebookFetcher(workspace_url, token)
        df_notebooks = fetcher.get_all_notebooks()

        if df_notebooks.empty:
            print("\n⚠ No notebooks found in workspace!")
            return

        # Save to CSV
        output_file = 'notebooks_cache.csv'
        df_notebooks.to_csv(output_file, index=False)

        print("\n" + "="*100)
        print("💾 CACHE SAVED")
        print("="*100)
        print(f"File: {output_file}")
        print(f"Notebooks: {len(df_notebooks)}")
        print(f"\nYou can now use this cache with enrich_with_notebook_paths.py")
        print("The cache will be automatically used if it exists.")

        # Show sample
        print("\n" + "="*100)
        print("📋 SAMPLE OF NOTEBOOKS (first 10)")
        print("="*100)
        print(df_notebooks.head(10).to_string(index=False))

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
