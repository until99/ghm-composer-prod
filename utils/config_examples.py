"""
Exemplos de configurações avançadas para casos de uso específicos.

Este arquivo demonstra padrões comuns e casos especiais.
"""

from utils.config import DatasetConfig, PipelineConfig, PROJECT_ID

# ==========================================
# CASO 1: Pipeline com múltiplas dependências
# ==========================================

# Silver que depende de 3 tabelas bronze
SILVER_PATIENT_360 = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_analytics",
    table="patient_360_view",
    layer="silver",
)

PIPELINE_PATIENT_360 = PipelineConfig(
    dag_id="silver_patient_360",
    dataset=SILVER_PATIENT_360,
    dependencies=[
        # Aguarda TODAS essas tabelas Bronze serem atualizadas
        DatasetConfig(PROJECT_ID, "bronze_patient", "patient_master", "bronze"),
        DatasetConfig(PROJECT_ID, "bronze_visits", "patient_visits", "bronze"),
        DatasetConfig(PROJECT_ID, "bronze_billing", "patient_billing", "bronze"),
    ],
    schedule_interval=None,  # Triggered by datasets
)


# ==========================================
# CASO 2: Gold com dependências mistas
# ==========================================

# Gold que depende de múltiplas tabelas Silver
GOLD_EXECUTIVE_DASHBOARD = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="gold_reporting",
    table="executive_dashboard",
    layer="gold",
)

PIPELINE_EXECUTIVE_DASHBOARD = PipelineConfig(
    dag_id="gold_executive_dashboard",
    dataset=GOLD_EXECUTIVE_DASHBOARD,
    dependencies=[
        # Múltiplas fontes Silver
        DatasetConfig(PROJECT_ID, "silver_analytics", "patient_consolidated", "silver"),
        DatasetConfig(PROJECT_ID, "silver_analytics", "revenue_consolidated", "silver"),
        DatasetConfig(PROJECT_ID, "silver_analytics", "operational_metrics", "silver"),
    ],
    schedule_interval=None,
)


# ==========================================
# CASO 3: Pipeline com schedule customizado
# ==========================================

# Bronze que roda em horário específico
BRONZE_MONTHLY_REPORT = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_reports",
    table="monthly_financial",
    layer="bronze",
)

PIPELINE_MONTHLY_REPORT = PipelineConfig(
    dag_id="bronze_monthly_report",
    dataset=BRONZE_MONTHLY_REPORT,
    dependencies=None,  # Sem dependências
    schedule_interval="0 2 1 * *",  # Todo dia 1 do mês às 02:00
)


# ==========================================
# CASO 4: Pipeline incremental
# ==========================================

# Bronze que processa apenas dados novos
BRONZE_TRANSACTION_LOG = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_transactions",
    table="transaction_log",
    layer="bronze",
)

PIPELINE_TRANSACTION_LOG = PipelineConfig(
    dag_id="bronze_transaction_log_incremental",
    dataset=BRONZE_TRANSACTION_LOG,
    schedule_interval="*/15 * * * *",  # A cada 15 minutos
)


# ==========================================
# CASO 5: Chain complexo Bronze → Silver → Gold
# ==========================================

# Bronze: Dados brutos de vendas
BRONZE_SALES = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_erp",
    table="sales_transactions",
    layer="bronze",
)

# Silver: Vendas limpas e enriquecidas
SILVER_SALES_ENRICHED = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_sales",
    table="sales_enriched",
    layer="silver",
)

# Gold: KPIs de vendas
GOLD_SALES_KPI = DatasetConfig(
    project_id=PROJECT_ID, dataset="gold_sales", table="sales_kpi", layer="gold"
)

# Configurações do pipeline
PIPELINE_BRONZE_SALES = PipelineConfig(
    dag_id="bronze_sales", dataset=BRONZE_SALES, schedule_interval="@hourly"
)

PIPELINE_SILVER_SALES = PipelineConfig(
    dag_id="silver_sales_enriched",
    dataset=SILVER_SALES_ENRICHED,
    dependencies=[BRONZE_SALES],
    schedule_interval=None,
)

PIPELINE_GOLD_SALES = PipelineConfig(
    dag_id="gold_sales_kpi",
    dataset=GOLD_SALES_KPI,
    dependencies=[SILVER_SALES_ENRICHED],
    schedule_interval=None,
)


# ==========================================
# CASO 6: Fan-out pattern
# ==========================================
# Um Bronze alimenta múltiplos Silvers para diferentes propósitos

BRONZE_PATIENT_EVENTS = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="bronze_events",
    table="patient_events",
    layer="bronze",
)

# Silver 1: Análise clínica
SILVER_CLINICAL_ANALYSIS = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_clinical",
    table="patient_clinical_analysis",
    layer="silver",
)

# Silver 2: Análise financeira
SILVER_FINANCIAL_ANALYSIS = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_financial",
    table="patient_financial_analysis",
    layer="silver",
)

# Silver 3: Análise operacional
SILVER_OPERATIONAL_ANALYSIS = DatasetConfig(
    project_id=PROJECT_ID,
    dataset="silver_operations",
    table="patient_operational_analysis",
    layer="silver",
)

# Todos dependem do mesmo Bronze, mas processam para propósitos diferentes
PIPELINE_CLINICAL = PipelineConfig(
    dag_id="silver_clinical_analysis",
    dataset=SILVER_CLINICAL_ANALYSIS,
    dependencies=[BRONZE_PATIENT_EVENTS],
    schedule_interval=None,
)

PIPELINE_FINANCIAL = PipelineConfig(
    dag_id="silver_financial_analysis",
    dataset=SILVER_FINANCIAL_ANALYSIS,
    dependencies=[BRONZE_PATIENT_EVENTS],
    schedule_interval=None,
)

PIPELINE_OPERATIONAL = PipelineConfig(
    dag_id="silver_operational_analysis",
    dataset=SILVER_OPERATIONAL_ANALYSIS,
    dependencies=[BRONZE_PATIENT_EVENTS],
    schedule_interval=None,
)


# ==========================================
# DICIONÁRIO CONSOLIDADO
# ==========================================

ADVANCED_PIPELINES = {
    "silver_patient_360": PIPELINE_PATIENT_360,
    "gold_executive_dashboard": PIPELINE_EXECUTIVE_DASHBOARD,
    "bronze_monthly_report": PIPELINE_MONTHLY_REPORT,
    "bronze_transaction_log_incremental": PIPELINE_TRANSACTION_LOG,
    # Chain completo
    "bronze_sales": PIPELINE_BRONZE_SALES,
    "silver_sales_enriched": PIPELINE_SILVER_SALES,
    "gold_sales_kpi": PIPELINE_GOLD_SALES,
    # Fan-out pattern
    "silver_clinical_analysis": PIPELINE_CLINICAL,
    "silver_financial_analysis": PIPELINE_FINANCIAL,
    "silver_operational_analysis": PIPELINE_OPERATIONAL,
}


# ==========================================
# HELPER: Visualizar dependências
# ==========================================


def print_pipeline_dependencies(pipeline_name: str):
    """
    Imprime as dependências de uma pipeline de forma legível.

    Uso:
        print_pipeline_dependencies("silver_patient_360")
    """
    if pipeline_name not in ADVANCED_PIPELINES:
        print(f"❌ Pipeline '{pipeline_name}' não encontrada")
        return

    pipeline = ADVANCED_PIPELINES[pipeline_name]
    print(f"\n📊 Pipeline: {pipeline.dag_id}")
    print(f"📁 Dataset: {pipeline.dataset.full_table_id}")
    print(f"⏰ Schedule: {pipeline.schedule_interval or 'Dataset-triggered'}")

    if pipeline.dependencies:
        print(f"🔗 Dependências ({len(pipeline.dependencies)}):")
        for dep in pipeline.dependencies:
            print(f"   ├─ {dep.full_table_id}")
    else:
        print("🔗 Dependências: Nenhuma (scheduled)")
    print()


def visualize_all_pipelines():
    """Visualiza todas as pipelines e suas dependências."""
    print("\n" + "=" * 60)
    print("🏗️  VISUALIZAÇÃO DE TODAS AS PIPELINES")
    print("=" * 60)

    for name in ADVANCED_PIPELINES:
        print_pipeline_dependencies(name)


if __name__ == "__main__":
    # Teste local
    visualize_all_pipelines()
