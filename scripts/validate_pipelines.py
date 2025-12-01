#!/usr/bin/env python3
"""
Script de validação para pipelines de dados.

Valida:
- Configurações de datasets
- Dependências entre pipelines
- Arquivos SQL existem
- Sintaxe das DAGs
"""

import sys
from pathlib import Path
from typing import List, Tuple

# Adiciona o diretório raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils import PIPELINES, get_pipeline_config


class ValidationError:
    def __init__(self, dag_id: str, error_type: str, message: str):
        self.dag_id = dag_id
        self.error_type = error_type
        self.message = message

    def __str__(self):
        return f"❌ [{self.dag_id}] {self.error_type}: {self.message}"


class ValidationWarning:
    def __init__(self, dag_id: str, warning_type: str, message: str):
        self.dag_id = dag_id
        self.warning_type = warning_type
        self.message = message

    def __str__(self):
        return f"⚠️  [{self.dag_id}] {self.warning_type}: {self.message}"


def validate_dataset_config(dag_id: str, config) -> List[ValidationError]:
    """Valida configuração do dataset."""
    errors = []

    # Valida dataset principal
    if not config.dataset:
        errors.append(ValidationError(dag_id, "CONFIG", "Dataset não configurado"))
        return errors

    # Valida campos obrigatórios
    if not config.dataset.project_id:
        errors.append(ValidationError(dag_id, "DATASET", "project_id não definido"))

    if not config.dataset.dataset:
        errors.append(ValidationError(dag_id, "DATASET", "dataset não definido"))

    if not config.dataset.table:
        errors.append(ValidationError(dag_id, "DATASET", "table não definida"))

    if not config.dataset.layer or config.dataset.layer not in [
        "bronze",
        "silver",
        "gold",
    ]:
        errors.append(
            ValidationError(
                dag_id, "DATASET", f"layer inválida: {config.dataset.layer}"
            )
        )

    return errors


def validate_dependencies(
    dag_id: str, config
) -> Tuple[List[ValidationError], List[ValidationWarning]]:
    """Valida dependências entre pipelines."""
    errors = []
    warnings = []

    # Bronze não deve ter dependências
    if config.dataset.layer == "bronze" and config.dependencies:
        warnings.append(
            ValidationWarning(
                dag_id,
                "DEPENDENCY",
                "Bronze layer com dependências (esperado: sem dependências)",
            )
        )

    # Silver/Gold devem ter dependências
    if config.dataset.layer in ["silver", "gold"] and not config.dependencies:
        warnings.append(
            ValidationWarning(
                dag_id,
                "DEPENDENCY",
                f"{config.dataset.layer.capitalize()} layer sem dependências (esperado: com dependências)",
            )
        )

    # Valida cada dependência
    if config.dependencies:
        for dep in config.dependencies:
            if not dep.full_table_id:
                errors.append(
                    ValidationError(
                        dag_id, "DEPENDENCY", f"Dependência inválida: {dep}"
                    )
                )

    return errors, warnings


def validate_sql_file(dag_id: str, layer: str) -> List[ValidationError]:
    """Valida se arquivo SQL existe."""
    errors = []

    sql_path = project_root / "sql" / f"{dag_id}.sql"

    if not sql_path.exists():
        errors.append(
            ValidationError(dag_id, "SQL", f"Arquivo não encontrado: {sql_path}")
        )

    return errors


def validate_dag_file(dag_id: str) -> List[ValidationError]:
    """Valida se arquivo de DAG existe e tem sintaxe válida."""
    errors = []

    dag_path = project_root / "dags" / f"{dag_id}.py"

    if not dag_path.exists():
        errors.append(
            ValidationError(dag_id, "DAG", f"Arquivo não encontrado: {dag_path}")
        )
        return errors

    # Tenta compilar o arquivo Python
    try:
        with open(dag_path, "r", encoding="utf-8") as f:
            compile(f.read(), str(dag_path), "exec")
    except SyntaxError as e:
        errors.append(ValidationError(dag_id, "DAG", f"Erro de sintaxe: {e}"))

    return errors


def validate_schedule(dag_id: str, config) -> List[ValidationWarning]:
    """Valida configuração de schedule."""
    warnings = []

    # Bronze deve ter schedule
    if config.dataset.layer == "bronze" and not config.schedule_interval:
        warnings.append(
            ValidationWarning(
                dag_id,
                "SCHEDULE",
                "Bronze sem schedule_interval (deveria ter @daily, @hourly, etc)",
            )
        )

    # Silver/Gold não devem ter schedule (usam dataset triggers)
    if config.dataset.layer in ["silver", "gold"] and config.schedule_interval:
        warnings.append(
            ValidationWarning(
                dag_id,
                "SCHEDULE",
                f"{config.dataset.layer.capitalize()} com schedule_interval (deveria ser None para dataset trigger)",
            )
        )

    return warnings


def run_validation():
    """Executa todas as validações."""
    print("\n" + "=" * 60)
    print("🔍 VALIDANDO CONFIGURAÇÃO DE PIPELINES")
    print("=" * 60 + "\n")

    all_errors = []
    all_warnings = []

    for dag_id, config in PIPELINES.items():
        print(f"Validando: {dag_id} ({config.dataset.layer})...")

        # Validações
        all_errors.extend(validate_dataset_config(dag_id, config))

        dep_errors, dep_warnings = validate_dependencies(dag_id, config)
        all_errors.extend(dep_errors)
        all_warnings.extend(dep_warnings)

        all_errors.extend(validate_sql_file(dag_id, config.dataset.layer))
        all_errors.extend(validate_dag_file(dag_id))
        all_warnings.extend(validate_schedule(dag_id, config))

    # Resultados
    print("\n" + "=" * 60)
    print("📊 RESULTADOS DA VALIDAÇÃO")
    print("=" * 60 + "\n")

    if all_errors:
        print(f"❌ {len(all_errors)} ERRO(S) ENCONTRADO(S):\n")
        for error in all_errors:
            print(f"  {error}")
        print()

    if all_warnings:
        print(f"⚠️  {len(all_warnings)} AVISO(S):\n")
        for warning in all_warnings:
            print(f"  {warning}")
        print()

    if not all_errors and not all_warnings:
        print("✅ TODAS AS VALIDAÇÕES PASSARAM!")
        print("🚀 Pipeline está pronto para deploy!")
    elif not all_errors:
        print("✅ Sem erros críticos")
        print("⚠️  Revise os avisos antes do deploy")
    else:
        print("❌ CORRIJA OS ERROS ANTES DO DEPLOY")
        return 1

    print()
    return 0


def print_pipeline_summary():
    """Imprime resumo das pipelines configuradas."""
    print("\n" + "=" * 60)
    print("📋 RESUMO DAS PIPELINES")
    print("=" * 60 + "\n")

    layers = {"bronze": [], "silver": [], "gold": []}

    for dag_id, config in PIPELINES.items():
        layers[config.dataset.layer].append(dag_id)

    for layer, dags in layers.items():
        print(f"{layer.upper()} Layer ({len(dags)} DAGs):")
        for dag in sorted(dags):
            config = PIPELINES[dag]
            schedule = config.schedule_interval or "dataset-triggered"
            deps = len(config.dependencies) if config.dependencies else 0
            print(f"  ├─ {dag}")
            print(f"  │  ├─ Schedule: {schedule}")
            print(f"  │  └─ Dependencies: {deps}")
        print()


if __name__ == "__main__":
    print_pipeline_summary()
    exit_code = run_validation()
    sys.exit(exit_code)
