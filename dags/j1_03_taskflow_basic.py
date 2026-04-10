from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="j1_03_taskflow_basic",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def taskflow_basic():

    @task()
    def extract() -> dict:
        data = {"source": "api", "rows": 500}
        print(f"Extracted {data['rows']} rows from {data['source']}")
        return data

    @task()
    def transform(data: dict) -> dict:
        data["status"] = "transformed"
        print(f"Transformed: {data}")
        return data

    @task()
    def load(data: dict) -> None:
        print(f"Loading {data['rows']} rows - status: {data['status']}")

    raw = extract()
    transformed = transform(raw)
    load(transformed)

taskflow_basic()
