from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime

raw_orders = Asset("s3://data-lake/raw/orders/")

@dag(
    dag_id="j1_04_consumer",
    schedule=[raw_orders],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def consumer():

    @task()
    def transform() -> None:
        print("Transformation des commandes brutes")

    transform()

consumer()
