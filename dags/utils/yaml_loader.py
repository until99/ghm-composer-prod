"""
Carregador e validador de configurações YAML.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class GlobalConfig:
    """Configurações globais do projeto."""

    project_id: str
    oracle_conn_id: str
    bucket_name: str
    default_owner: str
    default_retries: int
    default_retry_delay: int


@dataclass
class PipelineYamlConfig:
    """Configuração de uma pipeline individual do YAML."""

    dag_id: str
    layer: str
    dataset: str
    table: str
    sql_file: str
    write_disposition: str
    tags: List[str]
    schedule: Optional[str] = None
    dependencies: Optional[List[str]] = None
    gcs_path: Optional[str] = None
    use_temp_table: bool = False
    temp_table_suffix: str = "_temp"
    timeout: Optional[int] = None
    retries: Optional[int] = None
    retry_delay: Optional[int] = None
    partitioning: Optional[Dict[str, Any]] = None
    clustering: Optional[Dict[str, Any]] = None

    @property
    def full_table_id(self) -> str:
        """Retorna ID completo da tabela."""
        from .yaml_loader import get_global_config

        config = get_global_config()
        return f"{config.project_id}.{self.dataset}.{self.table}"

    @property
    def dataset_uri(self) -> str:
        """Retorna URI do dataset para Airflow."""
        return f"bigquery://{self.full_table_id}"


class YamlConfigLoader:
    """Carrega e valida configurações do arquivo YAML."""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Inicializa o loader.

        Args:
            config_path: Caminho para o arquivo YAML. Se None, usa o padrão.
        """
        if config_path is None:
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "pipelines.yaml"

            if not config_path.exists():
                project_root = Path(__file__).parent.parent.parent
                config_path = project_root / "config" / "pipelines.yaml"

        self.config_path = config_path
        self._raw_config: Optional[Dict] = None
        self._global_config: Optional[GlobalConfig] = None
        self._pipelines: Optional[Dict[str, PipelineYamlConfig]] = None

    def load(self) -> None:
        """Carrega o arquivo YAML."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            self._raw_config = yaml.safe_load(f)

    def _parse_global_config(self) -> GlobalConfig:
        """Parse configurações globais."""
        global_cfg = self._raw_config.get("global", {})
        return GlobalConfig(
            project_id=global_cfg.get("project_id", "ghm-data-prod"),
            oracle_conn_id=global_cfg.get("oracle_conn_id", "tasy_prod_oracle_conn"),
            bucket_name=global_cfg.get(
                "bucket_name", "ghm-data-prod-composer-bucket-001"
            ),
            default_owner=global_cfg.get("default_owner", "Gabriel Kasten"),
            default_retries=global_cfg.get("default_retries", 1),
            default_retry_delay=global_cfg.get("default_retry_delay", 300),
        )

    def _parse_pipelines(self) -> Dict[str, PipelineYamlConfig]:
        """Parse todas as pipelines."""
        pipelines = {}

        # Parse Bronze
        for name, cfg in self._raw_config.get("bronze", {}).items():
            dag_id = f"bronze_{name}"
            pipelines[dag_id] = self._create_pipeline_config(
                dag_id=dag_id, layer="bronze", name=name, config=cfg
            )

        # Parse Silver
        for name, cfg in self._raw_config.get("silver", {}).items():
            dag_id = f"silver_{name}"
            pipelines[dag_id] = self._create_pipeline_config(
                dag_id=dag_id, layer="silver", name=name, config=cfg
            )

        # Parse Gold
        for name, cfg in self._raw_config.get("gold", {}).items():
            dag_id = f"gold_{name}"
            pipelines[dag_id] = self._create_pipeline_config(
                dag_id=dag_id, layer="gold", name=name, config=cfg
            )

        return pipelines

    def _create_pipeline_config(
        self, dag_id: str, layer: str, name: str, config: Dict
    ) -> PipelineYamlConfig:
        """Cria configuração de pipeline a partir do YAML."""
        advanced = self._raw_config.get("advanced", {})

        # Timeout customizado
        timeout = advanced.get("timeouts", {}).get(dag_id)

        # Retry customizado
        retry_cfg = advanced.get("retry_config", {}).get(dag_id, {})
        retries = retry_cfg.get("retries")
        retry_delay = retry_cfg.get("retry_delay")

        # Particionamento
        partitioning = advanced.get("partitioning", {}).get(dag_id)

        # Clustering
        clustering = advanced.get("clustering", {}).get(dag_id)

        return PipelineYamlConfig(
            dag_id=dag_id,
            layer=layer,
            dataset=config["dataset"],
            table=config["table"],
            sql_file=config["sql_file"],
            write_disposition=config.get("write_disposition", "WRITE_TRUNCATE"),
            tags=config.get("tags", []),
            schedule=config.get("schedule"),
            dependencies=config.get("dependencies"),
            gcs_path=config.get("gcs_path"),
            use_temp_table=config.get("use_temp_table", False),
            temp_table_suffix=config.get("temp_table_suffix", "_temp"),
            timeout=timeout,
            retries=retries,
            retry_delay=retry_delay,
            partitioning=partitioning,
            clustering=clustering,
        )

    def get_global_config(self) -> GlobalConfig:
        """Retorna configurações globais."""
        if self._raw_config is None:
            self.load()

        if self._global_config is None:
            self._global_config = self._parse_global_config()

        return self._global_config

    def get_pipelines(self) -> Dict[str, PipelineYamlConfig]:
        """Retorna todas as pipelines."""
        if self._raw_config is None:
            self.load()

        if self._pipelines is None:
            self._pipelines = self._parse_pipelines()

        return self._pipelines

    def get_pipeline(self, dag_id: str) -> PipelineYamlConfig:
        """Retorna configuração de uma pipeline específica."""
        pipelines = self.get_pipelines()
        if dag_id not in pipelines:
            raise ValueError(f"Pipeline '{dag_id}' not found in config")
        return pipelines[dag_id]


# Singleton loader
_loader = YamlConfigLoader()


def get_global_config() -> GlobalConfig:
    """Retorna configurações globais."""
    return _loader.get_global_config()


def get_all_pipelines() -> Dict[str, PipelineYamlConfig]:
    """Retorna todas as pipelines."""
    return _loader.get_pipelines()


def get_pipeline_config(dag_id: str) -> PipelineYamlConfig:
    """Retorna configuração de uma pipeline."""
    return _loader.get_pipeline(dag_id)


def reload_config() -> None:
    """Recarrega configuração do YAML."""
    global _loader
    _loader = YamlConfigLoader()
    _loader.load()
