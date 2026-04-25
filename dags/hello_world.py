from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 4, 25),
    schedule=None,
    catchup=False,
) as dag:
    task = BashOperator(
        task_id="print_hello_in_terminal",
        bash_command="echo 'Hello, World from Airflow!'",
    )
