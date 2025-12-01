"""
Utilitários compartilhados para as DAGs do Composer.
"""

# Não importa automaticamente para evitar dependências circulares
# e problemas quando Airflow não está disponível.
# Use imports diretos nos arquivos que precisarem:
#   from utils.sql_helpers import read_sql_file
#   from utils.dag_factory import create_dag
#   from utils.yaml_loader import get_global_config

__all__ = [
    "read_sql_file",
    "create_dag",
    "get_dag_outlets",
    "create_oracle_to_gcs_task",
    "create_gcs_to_bigquery_task",
    "create_bigquery_transform_task",
    "get_global_config",
    "get_pipeline_config",
    "get_all_pipelines",
    "reload_config",
]

__all__ = [
    # SQL helpers
    "read_sql_file",
    # DAG factory
    "create_dag",
    "get_dag_outlets",
    # Task templates
    "create_oracle_to_gcs_task",
    "create_gcs_to_bigquery_task",
    "create_bigquery_transform_task",
    # YAML config
    "get_global_config",
    "get_pipeline_config",
    "get_all_pipelines",
    "reload_config",
]
