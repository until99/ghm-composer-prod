"""
DAG Generator - Gera DAGs automaticamente a partir do YAML

Este arquivo lê config/pipelines.yaml e cria dinamicamente todas as DAGs
definidas na configuração. Não é necessário criar arquivos .py para cada DAG.
"""

from datetime import datetime
from utils import (
    read_sql_file,
    create_dag,
    get_dag_outlets,
    create_oracle_to_gcs_task,
    create_gcs_to_bigquery_task,
    create_bigquery_transform_task,
    get_global_config,
    get_all_pipelines,
)

# Carrega configuração global
global_config = get_global_config()

# Carrega todas as pipelines do YAML
all_pipelines = get_all_pipelines()

# Gera DAGs dinamicamente
for dag_id, pipeline_config in all_pipelines.items():
    try:
        # Lê o SQL da pipeline
        sql_query = read_sql_file(pipeline_config.sql_file)

        # Cria a DAG usando a factory
        dag = create_dag(
            dag_id=dag_id,
            start_date=datetime(2023, 1, 1),
        )

        with dag:
            if pipeline_config.layer == "bronze":
                # Bronze: Oracle → GCS → BigQuery
                extract = create_oracle_to_gcs_task(
                    task_id="extract_oracle_to_gcs",
                    oracle_conn_id=global_config.oracle_conn_id,
                    sql_query=sql_query,
                    bucket=global_config.bucket_name,
                    gcs_path=pipeline_config.gcs_path,
                )

                # Se usa tabela temporária, cria workflow específico
                if pipeline_config.use_temp_table:
                    temp_table = f"{pipeline_config.full_table_id}{pipeline_config.temp_table_suffix}"

                    load_temp = create_gcs_to_bigquery_task(
                        task_id="load_to_temp_table",
                        bucket=global_config.bucket_name,
                        source_objects=[pipeline_config.gcs_path],
                        destination_table=temp_table,
                        write_disposition="WRITE_TRUNCATE",
                    )

                    move_to_final = create_bigquery_transform_task(
                        task_id="move_to_final_table",
                        sql_query=f"SELECT * FROM `{temp_table}`",
                        destination_table=pipeline_config.full_table_id,
                        write_disposition=pipeline_config.write_disposition,
                        outlets=get_dag_outlets(dag),
                    )

                    extract >> load_temp >> move_to_final
                else:
                    # Workflow padrão
                    load = create_gcs_to_bigquery_task(
                        task_id="load_gcs_to_bq",
                        bucket=global_config.bucket_name,
                        source_objects=[pipeline_config.gcs_path],
                        destination_table=pipeline_config.full_table_id,
                        write_disposition=pipeline_config.write_disposition,
                        outlets=get_dag_outlets(dag),
                    )

                    extract >> load

            else:
                # Silver/Gold: BigQuery transform
                transform = create_bigquery_transform_task(
                    task_id=f"transform_to_{pipeline_config.layer}",
                    sql_query=sql_query,
                    destination_table=pipeline_config.full_table_id,
                    write_disposition=pipeline_config.write_disposition,
                    outlets=get_dag_outlets(dag),
                )

                transform

        # Registra a DAG no namespace global do Airflow
        globals()[dag_id] = dag

    except Exception as e:
        print(f"Erro ao gerar DAG {dag_id}: {str(e)}")
        raise
