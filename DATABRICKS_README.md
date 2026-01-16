# Databricks Notebook Usage Analysis - Volume Version

Scripts adaptados para rodar **dentro do Databricks**, consumindo logs diretamente do Volume.

## 📋 Visão Geral

Este projeto analisa logs de clusters Databricks armazenados em Volumes para extrair informações sobre uso de notebooks, incluindo:
- Quais notebooks foram executados
- Quando e por quanto tempo
- Em quais clusters
- Por quais usuários

## 🗂️ Arquivos Disponíveis

### Scripts para Databricks

1. **`databricks_complete_pipeline.py`** ⭐ **RECOMENDADO**
   - Pipeline completo all-in-one
   - Parse logs + Cache notebooks + Enrichment
   - Mais fácil de usar

2. **`databricks_parse_notebook_usage.py`**
   - Apenas parsing de logs do Volume
   - Para uso modular

3. **`databricks_fetch_notebooks_cache.py`**
   - Apenas criação de cache de notebooks
   - Para uso modular

4. **`databricks_enrich_notebook_usage.py`**
   - Apenas enrichment com cache
   - Para uso modular

### Scripts Locais (originais)

- `parse_notebook_usage.py` - Parse local
- `fetch_all_notebooks.py` - Cache local
- `enrich_with_notebook_paths.py` - Enrichment local

## 🚀 Como Usar no Databricks

### Opção A: Pipeline Completo (Recomendado)

Copie o conteúdo de `databricks_complete_pipeline.py` para um notebook Databricks e execute:

```python
# 1. Execute o pipeline completo
df_result = run_complete_pipeline(
    volume_path="/Volumes/hs_franquia/logs/hs-community-logs",
    cache_table="hs_franquia.analytics.notebooks_cache",
    output_table="hs_franquia.analytics.notebook_usage_enriched"
)

# 2. Visualize os resultados
display(df_result)

# 3. Ou consulte a tabela depois
df = spark.table("hs_franquia.analytics.notebook_usage_enriched")
display(df)
```

**Execuções subsequentes (mais rápido):**
```python
# Use o cache existente para ser mais rápido
df_result = run_complete_pipeline(skip_cache=True)
display(df_result)
```

### Opção B: Executar Etapas Separadamente

Se preferir controle granular:

```python
# Etapa 1: Parse logs do Volume
df_usage = parse_logs_from_volume("/Volumes/hs_franquia/logs/hs-community-logs")
display(df_usage)

# Etapa 2: Criar cache de notebooks (execute uma vez)
df_cache = create_notebooks_cache(save_table="hs_franquia.analytics.notebooks_cache")
display(df_cache)

# Etapa 3: Enrich com informações dos notebooks
df_enriched = enrich_with_cache(df_usage, df_cache)
display(df_enriched)

# Etapa 4: Salvar resultado
df_enriched.write.format("delta").mode("overwrite") \
    .saveAsTable("hs_franquia.analytics.notebook_usage_enriched")
```

### Opção C: Usar Scripts Modulares

**Notebook 1: Criar Cache**
```python
# Cole databricks_fetch_notebooks_cache.py
df_cache = main(output_table="hs_franquia.analytics.notebooks_cache")
display(df_cache)
```

**Notebook 2: Parse e Enrich**
```python
# Cole databricks_parse_notebook_usage.py
df_usage = main(volume_path="/Volumes/hs_franquia/logs/hs-community-logs")

# Cole databricks_enrich_notebook_usage.py
df_enriched = enrich_with_cache(
    usage_df=df_usage,
    cache_table="hs_franquia.analytics.notebooks_cache"
)

display(df_enriched)
```

## 📊 Estrutura dos Dados

### Tabela de Uso (Output)

Colunas no resultado final:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `cluster_id` | string | ID do cluster |
| `cluster_name` | string | Nome do cluster |
| `session_id` | string | ID da sessão |
| `notebook_ids` | string | IDs dos notebooks (separados por vírgula) |
| `notebook_names` | string | Nomes dos notebooks (separados por \|) |
| `notebook_paths` | string | Caminhos completos (separados por \|) |
| `notebook_languages` | string | Linguagens (Python, SQL, etc.) |
| `notebook_owners` | string | Proprietários dos notebooks |
| `num_notebooks` | int | Quantidade de notebooks na sessão |
| `start_time` | timestamp | Início da sessão |
| `end_time` | timestamp | Fim da sessão |
| `duration_seconds` | double | Duração em segundos |

### Tabela de Cache

Colunas no cache de notebooks:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `object_id` | string | ID interno do notebook |
| `name` | string | Nome do notebook |
| `path` | string | Caminho completo no workspace |
| `language` | string | Linguagem (PYTHON, SQL, etc.) |
| `owner` | string | Email do proprietário ou "Shared" |
| `created_at` | timestamp | Data de criação |
| `modified_at` | timestamp | Última modificação |
| `fetched_at` | timestamp | Quando o cache foi criado |

## 🔧 Configuração

### Requisitos

- Databricks Runtime 13.0+
- Acesso ao Volume com logs
- Permissões para criar tabelas no catálogo/schema

### Estrutura do Volume

O Volume deve conter diretórios de logs de clusters no formato:

```
/Volumes/catalogo/schema/volume/
├── 0308-151607-btdoodbb/
│   ├── driver/
│   │   └── log4j-active.log
│   └── executor/
├── 0409-162508-xyznnncc/
│   ├── driver/
│   │   └── log4j-active.log
│   └── executor/
...
```

### Customização

Ajuste as constantes no início do script:

```python
# Caminho do Volume
VOLUME_PATH = "/Volumes/seu_catalogo/seu_schema/seus_logs"

# Tabela para cache de notebooks
CACHE_TABLE = "seu_catalogo.seu_schema.notebooks_cache"

# Tabela para resultados
OUTPUT_TABLE = "seu_catalogo.seu_schema.notebook_usage_enriched"
```

## 📈 Análises Sugeridas

Depois de gerar os dados, você pode fazer análises como:

```sql
-- Notebooks mais usados
SELECT
    notebook_names,
    COUNT(*) as usage_count,
    SUM(duration_seconds) as total_duration_seconds
FROM hs_franquia.analytics.notebook_usage_enriched
WHERE notebook_names NOT LIKE '%Unknown%'
GROUP BY notebook_names
ORDER BY usage_count DESC;

-- Uso por cluster
SELECT
    cluster_name,
    COUNT(DISTINCT session_id) as sessions,
    COUNT(DISTINCT notebook_ids) as unique_notebooks,
    SUM(duration_seconds) / 3600 as total_hours
FROM hs_franquia.analytics.notebook_usage_enriched
GROUP BY cluster_name
ORDER BY sessions DESC;

-- Uso por proprietário
SELECT
    notebook_owners,
    COUNT(*) as usage_count,
    COUNT(DISTINCT notebook_names) as unique_notebooks
FROM hs_franquia.analytics.notebook_usage_enriched
WHERE notebook_owners NOT LIKE '%Unknown%'
GROUP BY notebook_owners
ORDER BY usage_count DESC;

-- Timeline de uso
SELECT
    DATE(start_time) as date,
    COUNT(*) as sessions,
    SUM(duration_seconds) / 3600 as hours
FROM hs_franquia.analytics.notebook_usage_enriched
GROUP BY DATE(start_time)
ORDER BY date DESC;
```

## ⚠️ Limitações

### Informações Disponíveis

✅ **Disponível nos logs:**
- Notebook IDs internos
- Cluster metadata
- Timestamps de execução
- Duração de sessões

✅ **Disponível via Workspace API:**
- Notebook paths e nomes
- Linguagens
- Proprietários (inferido do path /Users/)

❌ **NÃO disponível:**
- Usuário específico que executou (apenas owner do notebook)
- Conteúdo dos comandos executados
- Resultados das execuções

### Matching de Notebooks

- Alguns notebooks podem não ter match (ID interno ≠ object_id da API)
- Isso é normal e esperado
- Notebooks não encontrados aparecem como "Unknown (ID: xxxxx)"

## 🔄 Atualizando o Cache

O cache de notebooks deve ser atualizado periodicamente:

```python
# Recriar cache (execute periodicamente)
df_cache = create_notebooks_cache(save_table="hs_franquia.analytics.notebooks_cache")
```

Recomenda-se atualizar:
- Semanalmente, se há muitos notebooks novos
- Mensalmente, para ambientes estáveis

## 🆘 Troubleshooting

### Erro: "Volume not found"
- Verifique se o caminho do Volume está correto
- Verifique se você tem permissões de leitura no Volume

### Erro: "Table not found" no enrichment
- Execute primeiro a criação do cache
- Ou passe `cache_df` ao invés de `cache_table`

### Nenhum cluster encontrado
- Verifique a estrutura de diretórios no Volume
- Deve haver subdiretórios com `/driver/log4j-active.log`

### Muitos notebooks "Unknown"
- Normal quando IDs internos diferem de object_ids
- Considere usar mapeamento manual se necessário

## 📚 Referências

- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Databricks SDK for Python](https://docs.databricks.com/dev-tools/sdk-python.html)
- [Delta Lake Tables](https://docs.databricks.com/delta/index.html)

## 🤝 Contribuindo

Para melhorias ou correções, abra uma issue ou pull request no repositório.
