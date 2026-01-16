"""
Script para extrair informações de uso de notebooks dos logs do Databricks.
"""

import os
import re
import json
from datetime import datetime
from pathlib import Path
import pandas as pd


def parse_timestamp(log_line):
    """Extrai timestamp de uma linha de log."""
    match = re.match(r'(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', log_line)
    if match:
        try:
            return datetime.strptime(match.group(1), '%y/%m/%d %H:%M:%S')
        except:
            return None
    return None


def extract_notebook_id(log_line):
    """Extrai notebook ID de uma linha de log."""
    match = re.search(r'databricksnbid:(\d+)', log_line)
    if match:
        return match.group(1)
    return None


def extract_session_id(log_line):
    """Extrai session ID de uma linha de log."""
    match = re.search(r'\[session: (\d+)\]', log_line)
    if match:
        return match.group(1)
    return None


def extract_execution_context_id(log_line):
    """Extrai execution context ID de uma linha de log."""
    match = re.search(r'ExecutionContextIdV2\((\d+)\)', log_line)
    if match:
        return match.group(1)
    return None


def parse_cluster_info(log_file_path):
    """Extrai informações básicas do cluster."""
    cluster_info = {}

    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Cluster ID
            if 'clusterUsageTags.clusterId=' in line:
                match = re.search(r'clusterId=(.+)', line)
                if match:
                    cluster_info['cluster_id'] = match.group(1).strip()

            # Cluster Name
            elif 'clusterUsageTags.clusterName=' in line:
                match = re.search(r'clusterName=(.+)', line)
                if match:
                    cluster_info['cluster_name'] = match.group(1).strip()

            # All Tags (includes Creator)
            elif 'clusterUsageTags.clusterAllTags=' in line:
                try:
                    json_str = line.split('clusterAllTags=')[1].strip()
                    tags = json.loads(json_str)
                    for tag in tags:
                        if tag['key'] == 'Creator':
                            cluster_info['creator'] = tag['value']
                        elif tag['key'] == 'Created By':
                            cluster_info['created_by'] = tag['value']
                except:
                    pass

            # orgId
            elif 'clusterUsageTags.orgId=' in line:
                match = re.search(r'orgId=(.+)', line)
                if match:
                    cluster_info['org_id'] = match.group(1).strip()

    return cluster_info


def parse_notebook_usage(log_file_path):
    """Extrai informações de uso de notebooks."""
    notebook_sessions = []
    session_info = {}

    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            timestamp = parse_timestamp(line)

            # Session start
            if 'Start MessageSendTask' in line:
                session_id = extract_session_id(line)
                if session_id and timestamp:
                    session_info[session_id] = {
                        'session_id': session_id,
                        'start_time': timestamp,
                        'end_time': None,
                        'notebook_ids': set(),
                        'execution_contexts': set(),
                        'activities': []
                    }

            # Session stop
            elif 'Stop MessageSendTask' in line:
                session_id = extract_session_id(line)
                if session_id and session_id in session_info and timestamp:
                    session_info[session_id]['end_time'] = timestamp

            # Notebook ID detection
            notebook_id = extract_notebook_id(line)
            if notebook_id and timestamp:
                # Try to find associated session
                for session_id, info in session_info.items():
                    if info['start_time'] and info['start_time'] <= timestamp and (info['end_time'] is None or timestamp <= info['end_time']):
                        info['notebook_ids'].add(notebook_id)
                        break
                else:
                    # Create new entry if no session found
                    temp_session = f"unknown_{notebook_id}"
                    if temp_session not in session_info:
                        session_info[temp_session] = {
                            'session_id': temp_session,
                            'start_time': timestamp,
                            'end_time': None,
                            'notebook_ids': {notebook_id},
                            'execution_contexts': set(),
                            'activities': []
                        }

            # Execution context
            exec_context = extract_execution_context_id(line)
            if exec_context and timestamp:
                for session_id, info in session_info.items():
                    if info['start_time'] and info['start_time'] <= timestamp and (info['end_time'] is None or timestamp <= info['end_time']):
                        info['execution_contexts'].add(exec_context)

            # Track activities
            if (notebook_id or exec_context) and timestamp:
                for session_id, info in session_info.items():
                    if info['start_time'] and info['start_time'] <= timestamp and (info['end_time'] is None or timestamp <= info['end_time']):
                        info['activities'].append({
                            'timestamp': timestamp,
                            'log_line': line.strip()[:200]  # First 200 chars
                        })

    # Convert to list
    for session_id, info in session_info.items():
        duration = None
        if info['end_time'] and info['start_time']:
            duration = (info['end_time'] - info['start_time']).total_seconds()
        elif info['activities']:
            # Use last activity as approximate end time
            last_activity = max(info['activities'], key=lambda x: x['timestamp'])
            duration = (last_activity['timestamp'] - info['start_time']).total_seconds()

        notebook_sessions.append({
            'session_id': info['session_id'],
            'start_time': info['start_time'],
            'end_time': info['end_time'],
            'duration_seconds': duration,
            'notebook_ids': ', '.join(info['notebook_ids']) if info['notebook_ids'] else 'Unknown',
            'num_notebooks': len(info['notebook_ids']),
            'execution_contexts': ', '.join(info['execution_contexts']) if info['execution_contexts'] else 'None',
            'num_activities': len(info['activities'])
        })

    return notebook_sessions


def create_notebook_usage_dataframe(cluster_path):
    """Cria DataFrame com informações de uso de notebooks."""
    log_file = os.path.join(cluster_path, 'driver', 'log4j-active.log')

    if not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        return None

    # Get cluster info
    cluster_info = parse_cluster_info(log_file)

    # Get notebook usage
    notebook_usage = parse_notebook_usage(log_file)

    # Create DataFrame
    df = pd.DataFrame(notebook_usage)

    # Add cluster info to each row
    for key, value in cluster_info.items():
        df[key] = value

    # Reorder columns
    columns_order = ['cluster_id', 'cluster_name', 'creator', 'session_id',
                     'notebook_ids', 'num_notebooks', 'start_time', 'end_time',
                     'duration_seconds', 'num_activities', 'execution_contexts']

    # Only use columns that exist
    columns_order = [col for col in columns_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in columns_order]
    df = df[columns_order + other_columns]

    return df


def main():
    """Função principal."""
    # Find all cluster directories
    base_path = Path('.')
    cluster_dirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]

    all_dfs = []

    for cluster_dir in cluster_dirs:
        print(f"Processing cluster: {cluster_dir.name}")
        df = create_notebook_usage_dataframe(str(cluster_dir))
        if df is not None and not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        print("No data found!")
        return

    # Combine all dataframes
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Sort by start time
    final_df = final_df.sort_values('start_time')

    # Display results
    print("\n" + "="*100)
    print("NOTEBOOK USAGE SUMMARY")
    print("="*100)
    print(f"\nTotal sessions: {len(final_df)}")
    print(f"Total notebooks: {final_df['notebook_ids'].nunique()}")

    print("\n" + "-"*100)
    print("DETAILED VIEW:")
    print("-"*100)
    print(final_df.to_string(index=False))

    # Save to CSV
    output_file = 'notebook_usage.csv'
    final_df.to_csv(output_file, index=False)
    print(f"\n\nData saved to: {output_file}")

    return final_df


if __name__ == '__main__':
    df = main()
