"""
DAG Generator - Gera DAGs automaticamente a partir do YAML

Este arquivo lê config/pipelines.yaml e cria dinamicamente todas as DAGs
definidas na configuração. Não é necessário criar arquivos .py para cada DAG.
"""

import sys
import os
from datetime import datetime

# Adiciona os diretórios necessários ao Python path
dag_folder = os.path.dirname(os.path.abspath(__file__))
root_folder = os.path.dirname(dag_folder)

# Adiciona root folder (para importar utils quando estiver fora de dags/)
if root_folder not in sys.path:
    sys.path.insert(0, root_folder)

# Adiciona dag folder (para importar utils quando estiver em dags/utils/)
if dag_folder not in sys.path:
    sys.path.insert(0, dag_folder)

# Import condicional baseado na localização
try:
    # Tenta import quando utils está em dags/utils/
    from utils.sql_helpers import read_sql_file
    from utils.dag_factory import create_dag, get_dag_outlets
    from utils.task_templates import (
        create_oracle_to_gcs_task,
        create_gcs_to_bigquery_task,
        create_bigquery_transform_task,
    )
    from utils.yaml_loader import get_global_config, get_all_pipelines
except ModuleNotFoundError:
    # Fallback: importa diretamente dos arquivos
    import importlib.util
    
    # Tenta encontrar utils em diferentes locais
    utils_path = os.path.join(dag_folder, 'utils')
    if not os.path.exists(utils_path):
        # utils está na raiz, não em dags/utils/
        utils_path = os.path.join(root_folder, 'utils')
    
    # sql_helpers
    spec = importlib.util.spec_from_file_location("sql_helpers", os.path.join(utils_path, "sql_helpers.py"))
    sql_helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sql_helpers)
    read_sql_file = sql_helpers.read_sql_file
    
    # dag_factory
    spec = importlib.util.spec_from_file_location("dag_factory", os.path.join(utils_path, "dag_factory.py"))
    dag_factory = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_factory)
    create_dag = dag_factory.create_dag
    get_dag_outlets = dag_factory.get_dag_outlets
    
    # task_templates
    spec = importlib.util.spec_from_file_location("task_templates", os.path.join(utils_path, "task_templates.py"))
    task_templates = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_templates)
    create_oracle_to_gcs_task = task_templates.create_oracle_to_gcs_task
    create_gcs_to_bigquery_task = task_templates.create_gcs_to_bigquery_task
    create_bigquery_transform_task = task_templates.create_bigquery_transform_task
    
    # yaml_loader
    spec = importlib.util.spec_from_file_location("yaml_loader", os.path.join(utils_path, "yaml_loader.py"))
    yaml_loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(yaml_loader)
    get_global_config = yaml_loader.get_global_config
    get_all_pipelines = yaml_loader.get_all_pipelines

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
