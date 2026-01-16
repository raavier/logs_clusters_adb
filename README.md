# Databricks Cluster Logs Analysis

Projeto para análise de logs de clusters Databricks, com foco em extrair informações de uso de notebooks.

## 📁 Estrutura do Projeto

```
.
├── parse_notebook_usage.py          # Script principal para extrair uso de notebooks dos logs
├── enrich_with_notebook_paths.py   # Script para enriquecer com paths via API Databricks
├── notebook_usage.csv               # Output gerado (não versionado)
├── notebook_usage_enriched.csv      # Output enriquecido (não versionado)
└── [cluster-id]/                    # Diretórios com logs dos clusters
    ├── driver/
    │   ├── log4j-active.log
    │   ├── stderr
    │   └── stdout
    └── executor/
        └── ...
```

## 🚀 Como Usar

### 0. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 1. Extrair Uso de Notebooks dos Logs

```bash
python parse_notebook_usage.py
```

**Output:** `notebook_usage.csv` com as seguintes colunas:
- `cluster_id` - ID do cluster
- `cluster_name` - Nome do cluster
- `session_id` - ID da sessão
- `notebook_ids` - IDs dos notebooks usados (separados por vírgula)
- `num_notebooks` - Quantidade de notebooks
- `start_time` - Início da sessão
- `end_time` - Fim da sessão
- `duration_seconds` - Duração em segundos

### 2. Enriquecer com Paths dos Notebooks (Opcional)

Para adicionar os caminhos dos notebooks usando a API do Databricks:

#### a) Criar Personal Access Token

1. Acesse seu workspace Databricks
2. Vá em **User Settings → Developer → Access Tokens**
3. Clique em **Generate New Token**
4. Copie o token gerado

#### b) Configurar credenciais

Copie o arquivo `.env.example` para `.env` e preencha com suas credenciais:

```bash
cp .env.example .env
# Edite o arquivo .env com suas credenciais
```

Conteúdo do `.env`:
```bash
DATABRICKS_WORKSPACE_URL=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi1234567890abcdef
```

#### c) Executar o script de enriquecimento

```bash
python enrich_with_notebook_paths.py
```

> O script lerá automaticamente as credenciais do arquivo `.env`

**Output:** `notebook_usage_enriched.csv` com colunas adicionais:
- `notebook_path` - Caminho do notebook no workspace
- `notebook_language` - Linguagem do notebook (Python, SQL, etc.)

## 📊 Informações Extraídas

### Dos Logs do Cluster

- ✅ Notebook IDs
- ✅ Cluster metadata (ID, nome, creator)
- ✅ Timestamps de início/fim de sessões
- ✅ Duração de uso
- ✅ Bibliotecas instaladas
- ✅ Configurações do cluster

### Via API do Databricks

- ✅ Notebook paths
- ✅ Notebook names
- ✅ Linguagens dos notebooks

## ⚠️ Limitações

### Informações NÃO disponíveis nos logs do cluster:

- ❌ **Usuário específico** executando notebooks (apenas creator do cluster)
- ❌ **Nome do notebook** (apenas ID interno)
- ❌ **Conteúdo dos comandos** executados

Para obter essas informações, é necessário:
- **Audit Logs do Databricks** (para usuários)
- **API REST do Databricks** (para paths/nomes)
- **Event Logs** (se habilitado no workspace)

## 🔐 Segurança

- **Nunca commite** seu Personal Access Token
- Use variáveis de ambiente para credenciais
- O `.gitignore` já está configurado para excluir arquivos sensíveis

## 📝 Exemplo de Output

### notebook_usage.csv
```csv
cluster_id,cluster_name,session_id,notebook_ids,num_notebooks,start_time,end_time,duration_seconds
0308-151607-btdoodbb,hs-community-dev,1965712499,"2348817320275328, 1115877656785222",2,2026-01-16 13:05:19,2026-01-16 13:07:09,110.0
```

### notebook_usage_enriched.csv
```csv
cluster_id,cluster_name,session_id,notebook_ids,num_notebooks,start_time,end_time,duration_seconds,notebook_path,notebook_language
0308-151607-btdoodbb,hs-community-dev,1965712499,"2348817320275328, 1115877656785222",2,2026-01-16 13:05:19,2026-01-16 13:07:09,110.0,"/Users/user@vale.com/analysis.py",Python
```

## 🛠️ Requisitos

```bash
pip install pandas requests
```

## 📚 Referências

- [Databricks REST API](https://docs.databricks.com/api/workspace/introduction)
- [Workspace API](https://docs.databricks.com/api/workspace/workspace)
- [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth.html#personal-access-tokens)
