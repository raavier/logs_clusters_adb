# Databricks Cluster Logs Analysis

Projeto para análise de logs de clusters Databricks, com foco em extrair informações de uso de notebooks.

## 🎯 Duas Versões Disponíveis

Este projeto oferece scripts para **duas formas de uso**:

1. **Versão Local** (este README) - Para análise de logs baixados localmente
2. **Versão Databricks** (ver [DATABRICKS_README.md](DATABRICKS_README.md)) - Para análise direta no Databricks consumindo Volumes ⭐ **RECOMENDADO**

> **💡 Recomendação:** Se você quer **rodar dentro do Databricks** e consumir logs diretamente do Volume, veja [DATABRICKS_README.md](DATABRICKS_README.md).

---

## 📁 Estrutura do Projeto (Versão Local)

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

Existem três formas de adicionar os caminhos dos notebooks:

#### Opção A: Cache de Notebooks (Recomendado - Rápido e Reutilizável)

Esta opção cria um cache de TODOS os notebooks do workspace que pode ser reutilizado:

1. Execute o script para criar o cache (executa uma vez, reutiliza várias vezes):
```bash
python fetch_all_notebooks.py
```

Isso criará o arquivo `notebooks_cache.csv` com todos os notebooks do workspace.

2. Use o cache para enriquecer (rápido - não faz chamadas à API):
```bash
python enrich_with_notebook_paths.py
```

O script detecta automaticamente se o cache existe e o utiliza, sem precisar das credenciais!

**Vantagens:**
- Execute a varredura do workspace apenas uma vez
- Enriqueça múltiplos relatórios sem precisar escanear novamente
- Mais rápido que a integração direta com API
- Para atualizar o cache, basta executar `fetch_all_notebooks.py` novamente

#### Opção B: Mapeamento Manual (Alternativa - Rápido)

1. Copie o template de mapeamento:
```bash
cp notebook_mapping_template.csv notebook_mapping.csv
```

2. Edite `notebook_mapping.csv` e preencha as informações dos notebooks:
```csv
notebook_id,notebook_path,notebook_name,notebook_language
2348817320275328,/Users/usuario/analise,Análise de Dados,Python
1115877656785222,/Workspace/Reports/dashboard,Dashboard Principal,SQL
```

3. Execute o enriquecimento:
```bash
python enrich_with_manual_mapping.py
```

#### Opção C: Via API do Databricks Direto (Automático - Lento)

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

> O script lerá automaticamente as credenciais do arquivo `.env` e fará a varredura completa do workspace toda vez que rodar. Para evitar isso, use a Opção A (Cache).

**Output:** `notebook_usage_enriched.csv` com colunas adicionais:
- `notebook_names` - Nomes dos notebooks
- `notebook_paths` - Caminhos dos notebooks no workspace
- `notebook_languages` - Linguagens dos notebooks (Python, SQL, etc.)
- `notebook_owners` - Proprietários dos notebooks

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
