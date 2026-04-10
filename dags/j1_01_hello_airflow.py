from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="j1_01_hello_airflow",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def hello_airflow():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end

hello_airflow()
