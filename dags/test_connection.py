"""
DAG simples para testar uma connection Oracle no Airflow.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

ORACLE_CONN_ID = "tasy_prod_oracle_conn"


def test_oracle_connection(**context):
    """
    Testa a connection Oracle executando uma query simples.
    """
    try:
        print(f"Testando connection Oracle: {ORACLE_CONN_ID}")

        # Cria o hook Oracle
        hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)

        # Executa uma query simples para testar
        result = hook.get_first("SELECT 1 FROM DUAL")

        print("Connection Oracle OK!")
        print(f"Resultado da query: {result}")

        # Informações da connection
        conn = hook.get_connection(ORACLE_CONN_ID)
        print("\nDetalhes da connection:")
        print(f"  Host: {conn.host}")
        print(f"  Port: {conn.port}")
        print(f"  Schema/SID: {conn.schema}")
        print(f"  Login: {conn.login}")

        return True

    except Exception as e:
        print(f"Erro ao testar connection Oracle: {str(e)}")
        raise


# Configurações padrão da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Definição da DAG
with DAG(
    "test_oracle_connection",
    default_args=default_args,
    description="DAG simples para testar connection Oracle",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "oracle", "connection"],
) as dag:
    test_oracle = PythonOperator(
        task_id="test_oracle_connection",
        python_callable=test_oracle_connection,
    )
