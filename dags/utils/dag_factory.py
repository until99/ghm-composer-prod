"""
Factory para criar DAGs com padrões consistentes e dependências de datasets.
"""

from airflow import DAG, Dataset
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from utils.yaml_loader import get_pipeline_config, get_global_config, get_all_pipelines


def create_dag(
    dag_id: str,
    start_date: datetime = datetime(2023, 1, 1),
    catchup: bool = False,
    default_args: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
    use_yaml_config: bool = True,
    **kwargs,
) -> DAG:
    """
    Factory para criar DAGs com configuração padronizada a partir do YAML.

    Args:
        dag_id: ID único da DAG
        start_date: Data de início da DAG
        catchup: Se deve executar datas passadas
        default_args: Argumentos padrão para as tasks
        tags: Tags para organização
        use_yaml_config: Se deve usar configuração do YAML
        **kwargs: Argumentos adicionais para a DAG

    Returns:
        DAG configurada
    """
    global_config = get_global_config()

    dag_default_args = {
        "owner": global_config.default_owner,
        "retries": global_config.default_retries,
        "retry_delay": timedelta(seconds=global_config.default_retry_delay),
    }

    if default_args:
        dag_default_args.update(default_args)

    schedule = kwargs.pop("schedule_interval", None)
    outlets = None

    if use_yaml_config:
        try:
            pipeline_config = get_pipeline_config(dag_id)

            if pipeline_config.retries is not None:
                dag_default_args["retries"] = pipeline_config.retries

            if pipeline_config.retry_delay is not None:
                dag_default_args["retry_delay"] = timedelta(
                    seconds=pipeline_config.retry_delay
                )

            if schedule is None:
                schedule = pipeline_config.schedule

            if pipeline_config.dependencies:
                all_pipelines = get_all_pipelines()
                schedule = []
                for dep_dag_id in pipeline_config.dependencies:
                    if dep_dag_id in all_pipelines:
                        dep_config = all_pipelines[dep_dag_id]
                        schedule.append(Dataset(dep_config.dataset_uri))

            outlets = [Dataset(pipeline_config.dataset_uri)]

            if not tags and pipeline_config.tags:
                tags = pipeline_config.tags

        except (ValueError, KeyError):
            pass

    dag = DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        default_args=dag_default_args,
        tags=tags or [],
        **kwargs,
    )

    if outlets:
        dag.outlets = outlets

    return dag


def get_dag_outlets(dag: DAG) -> List[Dataset]:
    """
    Retorna os outlets (datasets) configurados para uma DAG.

    Args:
        dag: Instância da DAG

    Returns:
        Lista de datasets que essa DAG atualiza
    """
    return getattr(dag, "outlets", [])
