from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime

raw_orders = Asset("s3://data-lake/raw/orders/")

@dag(
    dag_id="j1_04_producer",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def producer():

    @task(outlets=[raw_orders])
    def extract_and_upload() -> str:
        print("Extraction et upload vers S3")
        return "s3://data-lake/raw/orders/2026-04-07.json"

    extract_and_upload()

producer()
