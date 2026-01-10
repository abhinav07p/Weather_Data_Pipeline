from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("Hello from Airflow! Abhinav is building a Weather Intelligence System 🚀")

with DAG(
    dag_id="hello_test_dag_01",
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="greet_task",
        python_callable=greet
    )
