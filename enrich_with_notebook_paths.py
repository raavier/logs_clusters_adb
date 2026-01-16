"""
Script para enriquecer o DataFrame de uso de notebooks com informações da API do Databricks.
Busca o path dos notebooks usando a API REST do Databricks.
"""

import os
import pandas as pd
import requests
from typing import Dict, Optional

# Try to import python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")


class DatabricksNotebookEnricher:
    """Cliente para enriquecer dados de notebooks usando a API do Databricks."""

    def __init__(self, workspace_url: str, token: str):
        """
        Inicializa o cliente da API do Databricks.

        Args:
            workspace_url: URL do workspace Databricks (ex: https://adb-123456789.azuredatabricks.net)
            token: Personal Access Token do Databricks
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        self.cache = {}  # Cache para evitar chamadas duplicadas

    def get_notebook_info(self, notebook_id: str) -> Optional[Dict]:
        """
        Busca informações de um notebook pelo ID.

        Args:
            notebook_id: ID do notebook

        Returns:
            Dicionário com informações do notebook ou None se não encontrado
        """
        # Check cache first
        if notebook_id in self.cache:
            return self.cache[notebook_id]

        # Try workspace API - get by object_id
        # Note: A API do Databricks não tem endpoint direto para buscar por notebook ID interno
        # Vamos tentar diferentes abordagens

        result = {
            'notebook_id': notebook_id,
            'notebook_path': None,
            'notebook_name': None,
            'language': None,
            'object_type': None
        }

        # Approach 1: Try to list notebooks and match by ID
        # This is not efficient but might work for small workspaces
        # In production, you would need a different approach

        print(f"  Attempting to find notebook {notebook_id}...")

        # For now, we'll return the structure and let the user know
        # that we need more information about how to map notebook IDs

        self.cache[notebook_id] = result
        return result

    def search_workspace(self, path: str = '/', level: int = 0) -> list:
        """
        Lista objetos no workspace recursivamente.

        Args:
            path: Caminho para listar
            level: Nível de recursão (para indentação)

        Returns:
            Lista de objetos encontrados
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
            all_objects = []
            notebook_count = 0
            dir_count = 0

            for obj in objects:
                all_objects.append(obj)
                # If it's a directory, recurse
                if obj.get('object_type') == 'DIRECTORY':
                    dir_count += 1
                    all_objects.extend(self.search_workspace(obj['path'], level + 1))
                elif obj.get('object_type') == 'NOTEBOOK':
                    notebook_count += 1

            if notebook_count > 0 or dir_count > 0:
                print(f"{indent}   ✓ Found: {notebook_count} notebooks, {dir_count} directories")

            return all_objects

        except requests.exceptions.RequestException as e:
            print(f"{indent}   ✗ Error listing workspace at {path}: {e}")
            return []

    def get_notebook_by_path(self, path: str) -> Optional[Dict]:
        """
        Busca informações de um notebook pelo path.

        Args:
            path: Caminho do notebook

        Returns:
            Dicionário com informações do notebook
        """
        url = f'{self.workspace_url}/api/2.0/workspace/get-status'
        params = {'path': path}

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting notebook at {path}: {e}")
            return None


def enrich_notebook_usage_df(csv_path: str, workspace_url: str, token: str) -> pd.DataFrame:
    """
    Enriquece o DataFrame de uso de notebooks com informações da API.

    Args:
        csv_path: Caminho para o CSV gerado pelo parse_notebook_usage.py
        workspace_url: URL do workspace Databricks
        token: Personal Access Token

    Returns:
        DataFrame enriquecido
    """
    # Read the CSV
    df = pd.read_csv(csv_path)

    # Initialize enricher
    enricher = DatabricksNotebookEnricher(workspace_url, token)

    print("\n" + "="*100)
    print("📡 FETCHING WORKSPACE OBJECTS")
    print("="*100)
    print("This may take several minutes depending on workspace size...\n")

    # Get all workspace objects
    all_objects = enricher.search_workspace('/')

    print("\n" + "="*100)
    print("📊 PROCESSING NOTEBOOKS")
    print("="*100)

    # Create a mapping of object_id to path (if available in API response)
    object_id_to_path = {}
    for obj in all_objects:
        if obj.get('object_type') == 'NOTEBOOK':
            # Note: The Databricks API might not return the internal notebook ID
            # We'll need to use object_id if available
            if 'object_id' in obj:
                object_id_to_path[str(obj['object_id'])] = {
                    'path': obj['path'],
                    'language': obj.get('language', 'Unknown')
                }

    print(f"\n✓ Total notebooks found in workspace: {len(object_id_to_path)}")
    print(f"✓ Total objects scanned: {len(all_objects)}")

    # Add new columns
    df['notebook_path'] = None
    df['notebook_language'] = None

    print("\n" + "="*100)
    print("🔗 MATCHING NOTEBOOK IDs TO PATHS")
    print("="*100)

    # Try to match notebook IDs
    total_notebooks_to_match = 0
    matched_notebooks = 0

    for idx, row in df.iterrows():
        notebook_ids_str = row['notebook_ids']
        if pd.notna(notebook_ids_str):
            notebook_ids = [nid.strip() for nid in str(notebook_ids_str).split(',')]
            total_notebooks_to_match += len(notebook_ids)

            # Try to find paths for these notebooks
            paths = []
            languages = []

            for notebook_id in notebook_ids:
                if notebook_id in object_id_to_path:
                    info = object_id_to_path[notebook_id]
                    paths.append(info['path'])
                    languages.append(info['language'])
                    matched_notebooks += 1
                    print(f"  ✓ Matched: {notebook_id} → {info['path']}")
                else:
                    paths.append(f"Unknown (ID: {notebook_id})")
                    languages.append('Unknown')
                    print(f"  ✗ Not found: {notebook_id}")

            df.at[idx, 'notebook_path'] = ', '.join(paths)
            df.at[idx, 'notebook_language'] = ', '.join(set(languages))

    print(f"\n✓ Successfully matched: {matched_notebooks}/{total_notebooks_to_match} notebooks")
    if matched_notebooks < total_notebooks_to_match:
        print(f"⚠ Note: {total_notebooks_to_match - matched_notebooks} notebooks could not be matched")
        print("   This is normal - the internal notebook ID may differ from object_id")

    return df


def main():
    """Função principal."""
    print("="*100)
    print("DATABRICKS NOTEBOOK PATH ENRICHMENT")
    print("="*100)

    # Get credentials from environment or user input
    workspace_url = os.environ.get('DATABRICKS_WORKSPACE_URL')
    token = os.environ.get('DATABRICKS_TOKEN')

    if not workspace_url:
        workspace_url = input("\nEnter Databricks workspace URL (e.g., https://adb-123456789.azuredatabricks.net): ").strip()

    if not token:
        print("\nEnter Databricks Personal Access Token:")
        print("(Get it from: User Settings > Developer > Access Tokens)")
        token = input("Token: ").strip()

    if not workspace_url or not token:
        print("\nError: Workspace URL and Token are required!")
        return

    # Check if notebook_usage.csv exists
    csv_path = 'notebook_usage.csv'
    if not os.path.exists(csv_path):
        print(f"\nError: {csv_path} not found!")
        print("Please run parse_notebook_usage.py first.")
        return

    print(f"\nReading {csv_path}...")
    print(f"Connecting to {workspace_url}...")

    # Enrich the dataframe
    try:
        enriched_df = enrich_notebook_usage_df(csv_path, workspace_url, token)

        # Save enriched data
        output_file = 'notebook_usage_enriched.csv'
        enriched_df.to_csv(output_file, index=False)

        print("\n" + "="*100)
        print("ENRICHED DATA:")
        print("="*100)
        print(enriched_df.to_string(index=False))

        print(f"\n\nEnriched data saved to: {output_file}")

        # Show summary
        print("\n" + "="*100)
        print("SUMMARY:")
        print("="*100)
        matched = enriched_df['notebook_path'].notna().sum()
        total = len(enriched_df)
        print(f"Total sessions: {total}")
        print(f"Sessions with matched paths: {matched}")
        print(f"Match rate: {matched/total*100:.1f}%")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
