"""
Utilitários compartilhados para as DAGs do Composer.
"""


# Lazy imports para evitar dependências circulares
def __getattr__(name):
    if name == "read_sql_file":
        from utils.sql_helpers import read_sql_file

        return read_sql_file
    elif name == "create_dag":
        from utils.dag_factory import create_dag

        return create_dag
    elif name == "get_dag_outlets":
        from utils.dag_factory import get_dag_outlets

        return get_dag_outlets
    elif name == "create_oracle_to_gcs_task":
        from utils.task_templates import create_oracle_to_gcs_task

        return create_oracle_to_gcs_task
    elif name == "create_gcs_to_bigquery_task":
        from utils.task_templates import create_gcs_to_bigquery_task

        return create_gcs_to_bigquery_task
    elif name == "create_bigquery_transform_task":
        from utils.task_templates import create_bigquery_transform_task

        return create_bigquery_transform_task
    elif name == "get_global_config":
        from utils.yaml_loader import get_global_config

        return get_global_config
    elif name == "get_pipeline_config":
        from utils.yaml_loader import get_pipeline_config

        return get_pipeline_config
    elif name == "get_all_pipelines":
        from utils.yaml_loader import get_all_pipelines

        return get_all_pipelines
    elif name == "reload_config":
        from utils.yaml_loader import reload_config

        return reload_config
    raise AttributeError(f"module 'utils' has no attribute '{name}'")
