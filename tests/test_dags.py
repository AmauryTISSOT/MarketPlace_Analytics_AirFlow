"""
Tests pytest pour les DAGs — Formation Airflow IPSSI
Utilise DagBag pour valider les DAGs.
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    """Tous les DAGs doivent se parser sans erreur d'import."""
    assert not dagbag.import_errors, (
        f"Import errors:\n"
        + "\n".join(f"  {k}: {v}" for k, v in dagbag.import_errors.items())
    )


def test_all_dags_catchup_false(dagbag):
    """Aucun DAG ne doit avoir catchup=True."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} has catchup=True"


def test_marketplace_ingest_has_idempotent_transform(dagbag):
    """Le DAG ingest doit contenir une task de transform idempotente."""
    dag = dagbag.dags.get("marketplace_orders_ingest_daily")
    assert dag is not None, "DAG marketplace_orders_ingest_daily not found"
    task_ids = [t.task_id for t in dag.tasks]
    assert "transform_staging_to_dwh" in task_ids, (
        f"Missing transform_staging_to_dwh task. Tasks: {task_ids}"
    )
