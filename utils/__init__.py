"""
Utilitários compartilhados para as DAGs do Composer.
"""

try:
    # Tenta import relativo (quando utils é um pacote)
    from .sql_helpers import read_sql_file
    from .dag_factory import create_dag, get_dag_outlets
    from .task_templates import (
        create_oracle_to_gcs_task,
        create_gcs_to_bigquery_task,
        create_bigquery_transform_task,
    )
    from .yaml_loader import (
        get_global_config,
        get_pipeline_config,
        get_all_pipelines,
        reload_config,
    )
except ImportError:
    # Fallback para import absoluto
    from sql_helpers import read_sql_file
    from dag_factory import create_dag, get_dag_outlets
    from task_templates import (
        create_oracle_to_gcs_task,
        create_gcs_to_bigquery_task,
        create_bigquery_transform_task,
    )
    from yaml_loader import (
        get_global_config,
        get_pipeline_config,
        get_all_pipelines,
        reload_config,
    )

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
