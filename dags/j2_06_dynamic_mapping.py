from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="j2_06_dynamic_mapping",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j2"],
)
def dag_dynamic():

    @task()
    def get_sources() -> list[dict]:
        return [
            {"source": "crm", "table": "customers"},
            {"source": "erp", "table": "orders"},
            {"source": "web", "table": "events"},
        ]

    @task()
    def ingest(config: dict) -> str:
        return f"Ingested {config['table']} from {config['source']}"

    @task()
    def aggregate(results: list[str]) -> None:
        print(f"Done: {len(results)} sources ingested")

    sources = get_sources()
    results = ingest.expand(config=sources)
    aggregate(results)

dag_dynamic()
