from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@dag(
    dag_id="j2_05_branching",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["formation", "j2"],
)
def dag_branching():

    @task()
    def compute_score() -> float:
        return 0.95

    @task.branch()
    def check_quality(score: float) -> str:
        return "process_valid" if score > 0.9 else "handle_anomaly"

    @task()
    def process_valid():
        print("Data OK")

    @task()
    def handle_anomaly():
        print("Alert envoyee")

    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    score = compute_score()
    branch = check_quality(score)
    branch >> [process_valid(), handle_anomaly()] >> join

dag_branching()
