from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definição básica da DAG
with DAG(
    dag_id="teste_conexao_github_composer",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["teste", "ci-cd"],
) as dag:
    task_hello = BashOperator(
        task_id="diga_ola",
        bash_command='echo "Conexão GitHub -> Composer funcionando com sucesso!"',
    )

    task_hello
