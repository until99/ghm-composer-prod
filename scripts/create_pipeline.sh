#!/bin/bash
# Script para criar uma nova pipeline completa (Bronze → Silver → Gold)
# Uso: ./create_pipeline.sh nome_tabela bronze|silver|gold schedule

set -e

TABLE_NAME=$1
LAYER=$2
SCHEDULE=$3

if [ -z "$TABLE_NAME" ] || [ -z "$LAYER" ]; then
    echo "Uso: ./create_pipeline.sh nome_tabela bronze|silver|gold [schedule]"
    echo "Exemplo: ./create_pipeline.sh td_medico bronze @daily"
    exit 1
fi

# Define schedule padrão se não fornecido
if [ -z "$SCHEDULE" ]; then
    if [ "$LAYER" == "bronze" ]; then
        SCHEDULE="@daily"
    else
        SCHEDULE="null"
    fi
fi

echo "🚀 Criando pipeline para: $TABLE_NAME (layer: $LAYER)"

# Converte para uppercase para constantes
TABLE_NAME_UPPER=$(echo "$TABLE_NAME" | tr '[:lower:]' '[:upper:]')

# ===== 1. Adiciona configuração em utils/config.py =====
echo "📝 Atualizando utils/config.py..."

# Aqui você adicionaria a lógica para inserir no config.py
# Por simplicidade, vamos apenas mostrar o que adicionar

echo ""
echo "✅ Adicione ao utils/config.py:"
echo ""
cat << EOF
# Em utils/config.py, adicione:

# ===== ${LAYER^^} LAYER =====
${LAYER^^}_${TABLE_NAME_UPPER} = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="${LAYER}_${TABLE_NAME}",
    table="${TABLE_NAME}",
    layer="${LAYER}"
)

# No dict PIPELINES:
"${LAYER}_${TABLE_NAME}": PipelineConfig(
    dag_id="${LAYER}_${TABLE_NAME}",
    dataset=${LAYER^^}_${TABLE_NAME_UPPER},
    schedule_interval="${SCHEDULE}"
),
EOF

# ===== 2. Cria arquivo SQL =====
echo ""
echo "📄 Criando sql/${LAYER}_${TABLE_NAME}.sql..."

if [ "$LAYER" == "bronze" ]; then
    cat > "sql/${LAYER}_${TABLE_NAME}.sql" << EOF
-- Bronze: Extração de ${TABLE_NAME}
-- Fonte: Oracle TASY

SELECT 
    CD_${TABLE_NAME_UPPER},
    -- Adicione os campos necessários
    TO_CHAR(DT_ATUALIZACAO, 'YYYY-MM-DD HH24:MI:SS') as DT_ATUALIZACAO
FROM DBAMV.${TABLE_NAME_UPPER}
WHERE DT_ATUALIZACAO >= TRUNC(SYSDATE - 1)
EOF
elif [ "$LAYER" == "silver" ]; then
    cat > "sql/${LAYER}_${TABLE_NAME}.sql" << EOF
-- Silver: Transformação de ${TABLE_NAME}
-- Limpeza, padronização e enriquecimento

SELECT 
    -- Adicione as transformações necessárias
    CURRENT_TIMESTAMP() as DT_PROCESSAMENTO
FROM \`ghm-data-prod.bronze_${TABLE_NAME}.${TABLE_NAME}\`
EOF
elif [ "$LAYER" == "gold" ]; then
    cat > "sql/${LAYER}_${TABLE_NAME}.sql" << EOF
-- Gold: Métricas agregadas de ${TABLE_NAME}

SELECT 
    -- Adicione as métricas necessárias
    CURRENT_TIMESTAMP() as DT_CALCULO
FROM \`ghm-data-prod.silver_${TABLE_NAME}.${TABLE_NAME}\`
EOF
fi

# ===== 3. Cria DAG =====
echo "🔧 Criando dags/${LAYER}_${TABLE_NAME}.py..."

if [ "$LAYER" == "bronze" ]; then
    cat > "dags/${LAYER}_${TABLE_NAME}.py" << EOF
from datetime import datetime
from utils import (
    read_sql_file,
    create_dag,
    get_dag_outlets,
    create_oracle_to_gcs_task,
    create_gcs_to_bigquery_task,
    ORACLE_CONN_ID,
    BUCKET_NAME,
    ${LAYER^^}_${TABLE_NAME_UPPER},
)

# Carrega a query SQL do arquivo
SQL_QUERY = read_sql_file("${LAYER}_${TABLE_NAME}.sql")

# Cria a DAG
dag = create_dag(
    dag_id="${LAYER}_${TABLE_NAME}",
    start_date=datetime(2023, 1, 1),
    tags=["${LAYER}", "oracle", "tasy", "${TABLE_NAME}"],
)

with dag:
    # Extrai do Oracle para GCS
    extract_oracle_to_gcs = create_oracle_to_gcs_task(
        task_id="extract_oracle_to_gcs",
        oracle_conn_id=ORACLE_CONN_ID,
        sql_query=SQL_QUERY,
        bucket=BUCKET_NAME,
        gcs_path="stage/${TABLE_NAME}/{{ ds }}/dados.parquet",
    )

    # Carrega do GCS para BigQuery
    load_gcs_to_bq = create_gcs_to_bigquery_task(
        task_id="load_gcs_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["stage/${TABLE_NAME}/{{ ds }}/dados.parquet"],
        destination_table=${LAYER^^}_${TABLE_NAME_UPPER}.full_table_id,
        write_disposition="WRITE_APPEND",
        outlets=get_dag_outlets(dag),
    )

    extract_oracle_to_gcs >> load_gcs_to_bq
EOF
else
    cat > "dags/${LAYER}_${TABLE_NAME}.py" << EOF
from datetime import datetime
from utils import (
    read_sql_file,
    create_dag,
    get_dag_outlets,
    create_bigquery_transform_task,
    ${LAYER^^}_${TABLE_NAME_UPPER},
)

# Carrega a query SQL
SQL_QUERY = read_sql_file("${LAYER}_${TABLE_NAME}.sql")

# Cria a DAG
dag = create_dag(
    dag_id="${LAYER}_${TABLE_NAME}",
    start_date=datetime(2023, 1, 1),
    tags=["${LAYER}", "analytics", "${TABLE_NAME}"],
)

with dag:
    transform = create_bigquery_transform_task(
        task_id="transform_${LAYER}",
        sql_query=SQL_QUERY,
        destination_table=${LAYER^^}_${TABLE_NAME_UPPER}.full_table_id,
        write_disposition="WRITE_TRUNCATE",
        outlets=get_dag_outlets(dag),
    )

    transform
EOF
fi

echo ""
echo "✅ Pipeline criada com sucesso!"
echo ""
echo "📋 Próximos passos:"
echo "1. Atualize utils/config.py com a configuração mostrada acima"
echo "2. Revise e complete sql/${LAYER}_${TABLE_NAME}.sql"
echo "3. Teste a query no BigQuery Console"
echo "4. Commit as mudanças: git add . && git commit -m 'Add ${LAYER}_${TABLE_NAME} pipeline'"
echo "5. Deploy: gsutil -m rsync -r . gs://seu-bucket-composer/"
echo ""
