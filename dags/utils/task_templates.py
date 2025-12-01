"""
Templates e helpers para criar tasks comuns nas DAGs.
"""

from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow import Dataset
from typing import Optional, List, Dict, Any


def create_oracle_to_gcs_task(
    task_id: str,
    oracle_conn_id: str,
    sql_query: str,
    bucket: str,
    gcs_path: str,
    export_format: str = "parquet",
    **kwargs,
) -> OracleToGCSOperator:
    """
    Cria task para extrair dados do Oracle para GCS.

    Args:
        task_id: ID da task
        oracle_conn_id: ID da conexão Oracle
        sql_query: Query SQL para extrair dados
        bucket: Nome do bucket GCS
        gcs_path: Caminho no bucket (pode usar templates Jinja)
        export_format: Formato de export (parquet, csv, json)
        **kwargs: Argumentos adicionais para o operator

    Returns:
        OracleToGCSOperator configurado
    """
    return OracleToGCSOperator(
        task_id=task_id,
        oracle_conn_id=oracle_conn_id,
        sql=sql_query,
        bucket=bucket,
        filename=gcs_path,
        export_format=export_format,
        ensure_ascii=False,
        **kwargs,
    )


def create_gcs_to_bigquery_task(
    task_id: str,
    bucket: str,
    source_objects: List[str],
    destination_table: str,
    source_format: str = "PARQUET",
    write_disposition: str = "WRITE_TRUNCATE",
    create_disposition: str = "CREATE_IF_NEEDED",
    autodetect: bool = True,
    outlets: Optional[List[Dataset]] = None,
    **kwargs,
) -> GCSToBigQueryOperator:
    """
    Cria task para carregar dados do GCS para BigQuery.

    Args:
        task_id: ID da task
        bucket: Nome do bucket GCS
        source_objects: Lista de objetos no GCS
        destination_table: Tabela destino (formato: project.dataset.table)
        source_format: Formato dos dados (PARQUET, CSV, JSON)
        write_disposition: Como escrever (WRITE_TRUNCATE, WRITE_APPEND)
        create_disposition: Quando criar tabela (CREATE_IF_NEEDED, CREATE_NEVER)
        autodetect: Se deve detectar schema automaticamente
        outlets: Datasets que essa task atualiza (para triggering)
        **kwargs: Argumentos adicionais

    Returns:
        GCSToBigQueryOperator configurado
    """
    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=bucket,
        source_objects=source_objects,
        destination_project_dataset_table=destination_table,
        source_format=source_format,
        write_disposition=write_disposition,
        create_disposition=create_disposition,
        autodetect=autodetect,
        outlets=outlets,
        **kwargs,
    )


def create_bigquery_transform_task(
    task_id: str,
    sql_query: str,
    destination_table: Optional[str] = None,
    write_disposition: str = "WRITE_TRUNCATE",
    create_disposition: str = "CREATE_IF_NEEDED",
    use_legacy_sql: bool = False,
    outlets: Optional[List[Dataset]] = None,
    location: str = "US",
    **kwargs,
) -> BigQueryInsertJobOperator:
    """
    Cria task para transformação SQL no BigQuery.

    Args:
        task_id: ID da task
        sql_query: Query SQL para transformação
        destination_table: Tabela destino (formato: project.dataset.table)
        write_disposition: Como escrever (WRITE_TRUNCATE, WRITE_APPEND)
        create_disposition: Quando criar tabela
        use_legacy_sql: Se usa SQL legado
        outlets: Datasets que essa task atualiza
        location: Localização do dataset
        **kwargs: Argumentos adicionais

    Returns:
        BigQueryInsertJobOperator configurado
    """
    configuration = {
        "query": {
            "query": sql_query,
            "useLegacySql": use_legacy_sql,
        }
    }

    if destination_table:
        configuration["query"]["destinationTable"] = {
            "projectId": destination_table.split(".")[0],
            "datasetId": destination_table.split(".")[1],
            "tableId": destination_table.split(".")[2],
        }
        configuration["query"]["writeDisposition"] = write_disposition
        configuration["query"]["createDisposition"] = create_disposition

    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration=configuration,
        location=location,
        outlets=outlets,
        **kwargs,
    )
