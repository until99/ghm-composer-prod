"""
Configurações centralizadas para datasets e dependências das DAGs.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class DatasetConfig:
    """Configuração de um dataset BigQuery."""

    project_id: str
    dataset: str
    table: str
    layer: str  # bronze, silver, gold

    @property
    def full_table_id(self) -> str:
        """Retorna o ID completo da tabela no formato project.dataset.table"""
        return f"{self.project_id}.{self.dataset}.{self.table}"

    @property
    def dataset_uri(self) -> str:
        """Retorna o URI do dataset para uso com Airflow Dataset"""
        return f"bigquery://{self.full_table_id}"


@dataclass
class PipelineConfig:
    """Configuração completa de um pipeline de dados."""

    dag_id: str
    dataset: DatasetConfig
    dependencies: Optional[List[DatasetConfig]] = None
    schedule_interval: Optional[str] = None

    @property
    def upstream_dataset_uris(self) -> List[str]:
        """Retorna lista de URIs dos datasets upstream"""
        if not self.dependencies:
            return []
        return [dep.dataset_uri for dep in self.dependencies]


# Configurações globais
PROJECT_ID = "ghm-data-prod"
ORACLE_CONN_ID = "tasy_prod_oracle_conn"
BUCKET_NAME = "ghm-data-prod-composer-bucket-001"


# ===== BRONZE LAYER =====
BRONZE_TD_PACIENTE = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_td_paciente",
    table="td_paciente",
    layer="bronze",
)

BRONZE_TB_ATENDIMENTO_PACIENTE = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_atendimento_paciente",
    table="tb_atendimento_paciente",
    layer="bronze",
)


# ===== SILVER LAYER =====
SILVER_TD_MEDICO = DatasetConfig(
    project_id=PROJECT_ID, dataset="silver_analytics", table="td_medico", layer="silver"
)

SILVER_PACIENTE_CONSOLIDATED = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_analytics",
    table="paciente_consolidated",
    layer="silver",
)


# ===== GOLD LAYER =====
GOLD_PACIENTE_METRICS = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="gold_analytics",
    table="paciente_metrics",
    layer="gold",
)


# ===== PIPELINE DEFINITIONS =====
PIPELINES: Dict[str, PipelineConfig] = {
    # Bronze pipelines (sem dependências, rodam por schedule)
    "bronze_td_paciente": PipelineConfig(
        dag_id="bronze_td_paciente",
        dataset=BRONZE_TD_PACIENTE,
        schedule_interval="@daily",
    ),
    "bronze_tb_atendimento_paciente": PipelineConfig(
        dag_id="bronze_tb_atendimento_paciente",
        dataset=BRONZE_TB_ATENDIMENTO_PACIENTE,
        schedule_interval="@monthly",
    ),
    # Silver pipelines (dependem de bronze, rodam via dataset trigger)
    "silver_paciente_consolidated": PipelineConfig(
        dag_id="silver_paciente_consolidated",
        dataset=SILVER_PACIENTE_CONSOLIDATED,
        dependencies=[BRONZE_TD_PACIENTE, BRONZE_TB_ATENDIMENTO_PACIENTE],
        schedule_interval=None,  # Triggered by datasets
    ),
    # Gold pipelines (dependem de silver, rodam via dataset trigger)
    "gold_paciente_metrics": PipelineConfig(
        dag_id="gold_paciente_metrics",
        dataset=GOLD_PACIENTE_METRICS,
        dependencies=[SILVER_PACIENTE_CONSOLIDATED, SILVER_TD_MEDICO],
        schedule_interval=None,  # Triggered by datasets
    ),
}


def get_pipeline_config(dag_id: str) -> PipelineConfig:
    """Retorna a configuração do pipeline para uma DAG específica."""
    if dag_id not in PIPELINES:
        raise ValueError(f"Pipeline config not found for dag_id: {dag_id}")
    return PIPELINES[dag_id]
