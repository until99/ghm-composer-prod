# GHM Composer Production - Data Pipelines

Pipeline de dados orquestrado pelo Google Cloud Composer (Apache Airflow) seguindo arquitetura Medallion (Bronze → Silver → Gold) com **geração automática de DAGs via YAML**.

---

## 🏗️ Arquitetura

```
Oracle TASY → [Bronze Layer] → [Silver Layer] → [Gold Layer] → BigQuery Analytics
              (schedule)      (triggered)      (triggered)
```

**Fluxo**:

1. **Bronze** roda agendado (`@daily`, `@monthly`)
2. Ao completar, atualiza **Dataset** do Airflow
3. **Silver** é automaticamente triggered
4. Silver limpa/transforma dados
5. **Gold** é triggered quando Silver completa
6. Gold calcula métricas agregadas

> **Dataset Triggers**: DAGs downstream só executam quando upstream completa com sucesso.

---

## 📂 Estrutura do Projeto

```
ghm-composer-prod/
├── config/
│   └── pipelines.yaml           # ← Configuração de TODAS as pipelines
│
├── dags/
│   ├── dag_generator.py         # ← Gera DAGs automaticamente do YAML
│   └── teste_conexao.py         # DAG de teste
│
├── sql/
│   ├── bronze_*.sql             # Queries de extração Oracle
│   ├── silver_*.sql             # Queries de transformação
│   └── gold_*.sql               # Queries de agregação
│
└── utils/
    ├── yaml_loader.py           # Carregador YAML
    ├── dag_factory.py           # Factory de DAGs
    ├── task_templates.py        # Templates reutilizáveis
    └── sql_helpers.py           # Helpers SQL
```

---

## 🚀 Quick Start - Adicionar Nova Pipeline

### 1️⃣ Edite o YAML

```yaml
# config/pipelines.yaml
bronze:
  nova_tabela:
    dataset: bronze_domain
    table: nova_tabela
    schedule: "@daily"
    sql_file: bronze_nova_tabela.sql
    gcs_path: stage/nova_tabela/{{ ds }}/dados.parquet
    write_disposition: WRITE_APPEND
    tags: [bronze, oracle, tasy]
```

### 2️⃣ Crie o SQL

```sql
-- sql/bronze_nova_tabela.sql
SELECT
    CD_CAMPO,
    NM_CAMPO,
    DT_ATUALIZACAO
FROM DBAMV.NOVA_TABELA
WHERE DT_ATUALIZACAO >= TRUNC(SYSDATE - 1)
```

### 3️⃣ Deploy

```bash
git add config/pipelines.yaml sql/bronze_nova_tabela.sql
git commit -m "feat: adiciona pipeline bronze_nova_tabela"
git push
```

O CI/CD automaticamente faz sync de:

- `dags/` → `gs://BUCKET/dags/`
- `utils/` → `gs://BUCKET/dags/utils/`
- `config/` → `gs://BUCKET/config/`
- `sql/` → `gs://BUCKET/sql/`

**Pronto!** A DAG `bronze_nova_tabela` será criada automaticamente. ✨

---

## 📝 Configuração YAML

### Estrutura Básica

```yaml
global:
  project_id: ghm-data-prod
  oracle_conn_id: tasy_prod_oracle_conn
  bucket_name: ghm-data-prod-composer-bucket-001
  default_retries: 0
  default_retry_delay: 300

bronze:
  tb_raw:
    dataset: bronze_tb_raw
    table: tb_raw
    schedule: "@daily"
    sql_file: bronze_tb_raw.sql
    gcs_path: stage/tb_raw/{{ ds }}/dados.parquet
    write_disposition: WRITE_APPEND | WRITE_TRUNCATE | WRITE_EMPTY
    tags: [bronze, oracle, tasy, raw]

silver:
  td_raw:
    dataset: silver_td_raw
    table: td_raw
    sql_file: silver_td_raw.sql
    write_disposition: WRITE_TRUNCATE
    tags: [silver, dimension, raw]
    dependencies:
      - bronze_td_raw # Aguarda Bronze completar

gold:
  tf_raw:
    dataset: gold_raw
    table: tf_raw
    sql_file: gold_tf_raw.sql
    write_disposition: WRITE_TRUNCATE
    tags: [gold, analytics, metrics]
    dependencies:
      - silver_td_raw # Aguarda Silver completar
```

### Campos Disponíveis

#### Global (obrigatório)

- `project_id`: ID do projeto GCP
- `oracle_conn_id`: Connection ID do Airflow
- `bucket_name`: Bucket GCS para staging
- `default_owner`: Owner das DAGs
- `default_retries`: Tentativas padrão
- `default_retry_delay`: Delay entre retries (segundos)

#### Bronze

- `dataset`: Dataset BigQuery
- `table`: Nome da tabela
- `schedule`: `@daily`, `@hourly`, `@monthly`, ou cron
- `sql_file`: Nome do arquivo SQL (em `sql/`)
- `gcs_path`: Caminho GCS (usa `{{ ds }}` para data)
- `write_disposition`: `WRITE_APPEND` ou `WRITE_TRUNCATE`
- `tags`: Lista de tags
- `use_temp_table` (opcional): `true` para usar tabela temporária
- `temp_table_suffix` (opcional): Sufixo da temp table (padrão: `_temp`)

#### Silver / Gold

- `dataset`: Dataset BigQuery
- `table`: Nome da tabela
- `sql_file`: Nome do arquivo SQL
- `write_disposition`: `WRITE_TRUNCATE` (geralmente)
- `tags`: Lista de tags
- `dependencies`: Lista de DAGs que devem completar antes

---

## 📊 Pipelines Configuradas

Atualmente em `config/pipelines.yaml`:

### Bronze (Ingestão Oracle)

- `bronze_td_paciente` - Dimensão pacientes (@daily)
- `bronze_tb_atendimento_paciente` - Atendimentos (@monthly, usa temp table)

### Silver (Transformação)

- `silver_paciente_consolidated` - Consolidação de dados
  - Triggered por: `bronze_td_paciente`, `bronze_tb_atendimento_paciente`

### Gold (Métricas)

- `gold_paciente_metrics` - KPIs agregados
  - Triggered por: `silver_paciente_consolidated`

---

## 🛠️ Desenvolvimento Local

### Setup

```bash
# Instalar dependências
uv sync

# Ativar ambiente
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

### Testar

```bash
# Validar DAG
python -c "from utils import get_all_pipelines; print(get_all_pipelines())"

# Testar geração
python dags/dag_generator.py

# Validar SQL no BigQuery Console
# (copie conteúdo de sql/*.sql)
```

---

## 🔍 Monitoramento

### Airflow UI

- **DAGs**: `/home` - Lista todas as DAGs geradas
- **Datasets**: `/datasets` - Visualiza dependências entre camadas
- **Graph**: Clique na DAG → Graph - Fluxo de tasks

### BigQuery

- Monitore custos e performance
- Valide particionamento/clustering
- Verifique qualidade dos dados

---

## 📋 Padrões de DAG Gerados

### Bronze (Padrão)

```
extract_oracle_to_gcs → load_gcs_to_bq
```

### Bronze (Com Temp Table)

```
extract_oracle_to_gcs → load_to_temp_table → move_to_final_table
```

### Silver / Gold

```
transform_to_[layer]
```

Todas as configurações do YAML (retries, timeouts, dependencies, tags) são aplicadas automaticamente.

---

## 🎯 Convenções

### Nomenclatura

- **DAG ID**: `{layer}_{nome_tabela}`
- **SQL files**: `{layer}_{nome_tabela}.sql`
- **Tasks**: Verbos (`extract_`, `load_`, `transform_`)

### Tags por Layer

- **Bronze**: `[bronze, oracle, tasy, tb, dominio]`
- **Silver**: `[silver, td, dominio]`
- **Gold**: `[gold, tf, metrics]`

### Write Disposition

- **Bronze**: `WRITE_APPEND` (mantém histórico)
- **Silver/Gold**: `WRITE_TRUNCATE` (full refresh)

---

## 🚨 Troubleshooting

### DAG não aparece no Airflow

```python
# Verifique se o YAML é válido
from utils import get_pipeline_config
config = get_pipeline_config("bronze_sua_tabela")
print(config)
```

### Erro "Pipeline not found"

- Verifique nome no YAML (ex: `td_paciente`, não `bronze_td_paciente`)
- DAG ID é gerado como `{layer}_{nome}` automaticamente

### Recarregar configuração

```python
from utils import reload_config
reload_config()
```

### Erro de dependência

- Certifique-se que DAGs upstream existem no YAML
- Use o DAG ID correto em `dependencies`

---

## 🤝 Contribuindo

1. Edite `config/pipelines.yaml`
2. Crie arquivo SQL correspondente
3. Teste localmente (`uv sync`, validação)
4. Commit com mensagem descritiva

---

## 📞 Contato

- **Owner**: Gabriel Kasten
- **Projeto GCP**: ghm-data-prod
- **Repositório**: until99/ghm-composer-prod

---

**Última atualização**: Dezembro 2025
