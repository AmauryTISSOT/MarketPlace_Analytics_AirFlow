from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(
    dag_id="j1_05_task_groups",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j1"],
)
def task_groups_demo():

    with TaskGroup("extraction", tooltip="Extraction des sources") as tg_extract:
        @task()
        def extract_crm() -> dict:
            return {"source": "crm", "rows": 200}

        @task()
        def extract_erp() -> dict:
            return {"source": "erp", "rows": 350}

        crm = extract_crm()
        erp = extract_erp()

    with TaskGroup("transformation", tooltip="Transformation") as tg_transform:
        @task()
        def transform_all(crm_data: dict, erp_data: dict) -> dict:
            total = crm_data["rows"] + erp_data["rows"]
            return {"total_rows": total}

        result = transform_all(crm, erp)

    @task()
    def load(data: dict) -> None:
        print(f"Chargement de {data['total_rows']} lignes")

    tg_extract >> tg_transform
    load(result)

task_groups_demo()
