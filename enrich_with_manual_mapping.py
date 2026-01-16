"""
Script para enriquecer o DataFrame de uso de notebooks usando um arquivo de mapeamento manual.
Mais rápido e simples que a integração com API.
"""

import pandas as pd
import os


def enrich_with_manual_mapping(usage_csv: str = 'notebook_usage.csv',
                                mapping_csv: str = 'notebook_mapping.csv',
                                output_csv: str = 'notebook_usage_enriched.csv'):
    """
    Enriquece o DataFrame de uso de notebooks com mapeamento manual.

    Args:
        usage_csv: Caminho para o CSV de uso (gerado por parse_notebook_usage.py)
        mapping_csv: Caminho para o CSV de mapeamento manual
        output_csv: Caminho para o CSV de saída
    """

    # Check if files exist
    if not os.path.exists(usage_csv):
        print(f"Error: {usage_csv} not found!")
        print("Please run parse_notebook_usage.py first.")
        return

    if not os.path.exists(mapping_csv):
        print(f"Error: {mapping_csv} not found!")
        print(f"\nPlease create {mapping_csv} based on notebook_mapping_template.csv")
        print("Fill in the notebook paths and names for each notebook ID.")
        return

    # Load data
    print(f"Loading {usage_csv}...")
    df_usage = pd.read_csv(usage_csv)

    print(f"Loading {mapping_csv}...")
    df_mapping = pd.read_csv(mapping_csv)

    # Create a mapping dictionary
    notebook_map = {}
    for _, row in df_mapping.iterrows():
        notebook_id = str(row['notebook_id']).strip()
        notebook_map[notebook_id] = {
            'path': row.get('notebook_path', ''),
            'name': row.get('notebook_name', ''),
            'language': row.get('notebook_language', '')
        }

    print(f"\nLoaded mapping for {len(notebook_map)} notebooks")

    # Add new columns
    df_usage['notebook_paths'] = None
    df_usage['notebook_names'] = None
    df_usage['notebook_languages'] = None

    # Enrich each row
    for idx, row in df_usage.iterrows():
        notebook_ids_str = row['notebook_ids']
        if pd.notna(notebook_ids_str):
            notebook_ids = [nid.strip() for nid in str(notebook_ids_str).split(',')]

            paths = []
            names = []
            languages = []

            for notebook_id in notebook_ids:
                if notebook_id in notebook_map:
                    info = notebook_map[notebook_id]
                    path = info['path'] if pd.notna(info['path']) and info['path'] else f"Unknown (ID: {notebook_id})"
                    name = info['name'] if pd.notna(info['name']) and info['name'] else "Unknown"
                    lang = info['language'] if pd.notna(info['language']) and info['language'] else "Unknown"

                    paths.append(path)
                    names.append(name)
                    languages.append(lang)
                else:
                    paths.append(f"Unknown (ID: {notebook_id})")
                    names.append("Unknown")
                    languages.append("Unknown")

            df_usage.at[idx, 'notebook_paths'] = ' | '.join(paths)
            df_usage.at[idx, 'notebook_names'] = ' | '.join(names)
            df_usage.at[idx, 'notebook_languages'] = ' | '.join(set(languages))

    # Reorder columns
    columns_order = ['cluster_id', 'cluster_name', 'session_id',
                     'notebook_ids', 'notebook_names', 'notebook_paths', 'notebook_languages',
                     'num_notebooks', 'start_time', 'end_time', 'duration_seconds']

    # Only use columns that exist
    columns_order = [col for col in columns_order if col in df_usage.columns]
    other_columns = [col for col in df_usage.columns if col not in columns_order]
    df_enriched = df_usage[columns_order + other_columns]

    # Save enriched data
    df_enriched.to_csv(output_csv, index=False)

    print("\n" + "="*100)
    print("ENRICHED DATA:")
    print("="*100)
    print(df_enriched.to_string(index=False))

    print(f"\n\nEnriched data saved to: {output_csv}")

    # Show summary
    print("\n" + "="*100)
    print("SUMMARY:")
    print("="*100)
    total_notebooks = df_enriched['notebook_ids'].str.split(',').apply(lambda x: len(x) if isinstance(x, list) else 0).sum()
    matched = 0
    for ids_str in df_enriched['notebook_ids']:
        if pd.notna(ids_str):
            ids = [nid.strip() for nid in str(ids_str).split(',')]
            for nid in ids:
                if nid in notebook_map:
                    info = notebook_map[nid]
                    if pd.notna(info['path']) and info['path']:
                        matched += 1

    print(f"Total notebook usages: {total_notebooks}")
    print(f"Notebooks with mapped paths: {matched}")
    print(f"Match rate: {matched/total_notebooks*100:.1f}%" if total_notebooks > 0 else "N/A")

    return df_enriched


def main():
    """Função principal."""
    print("="*100)
    print("DATABRICKS NOTEBOOK MANUAL MAPPING ENRICHMENT")
    print("="*100)

    enrich_with_manual_mapping()


if __name__ == '__main__':
    main()
