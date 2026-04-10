from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime

def _extract():
    print("Extraction de 500 lignes depuis l'API")
    return 500

def _transform(ti):
    row_count = ti.xcom_pull(task_ids="extract")
    print(f"Transformation de {row_count} lignes")

@dag(
    dag_id="j1_02_python_operator",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def python_operator_dag():
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    extract >> transform

python_operator_dag()
