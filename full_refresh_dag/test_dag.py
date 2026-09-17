from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello World!")
    return "DAG running"

with DAG(
    dag_id="test_dag",
    start_date=datetime(2026, 9, 13),
    schedule_interval="0 8 * * *",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello
    )