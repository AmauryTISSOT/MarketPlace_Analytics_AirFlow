import ast
import re
from pathlib import Path

INGEST_DAG = (
    Path(__file__).resolve().parent.parent
    / "dags"
    / "marketplace_orders_ingest_daily.py"
)


def _source() -> str:
    assert INGEST_DAG.exists(), f"{INGEST_DAG.name} not found"
    return INGEST_DAG.read_text(encoding="utf-8")


def _function_names() -> list[str]:
    tree = ast.parse(_source(), filename=INGEST_DAG.name)
    return [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def test_staging_load_present():
    """Le DAG ingest doit avoir une fonction load_staging_orders."""
    assert (
        "load_staging_orders" in _function_names()
    ), "Missing load_staging_orders task"


def test_transform_staging_to_dwh_present():
    """Le DAG ingest doit avoir une fonction transform_staging_to_dwh."""
    assert (
        "transform_staging_to_dwh" in _function_names()
    ), "Missing transform_staging_to_dwh task"


def test_staging_delete_before_insert():
    """Le load staging doit etre idempotent (DELETE avant INSERT)."""
    source = _source()
    assert re.search(
        r"DELETE\s+FROM\s+staging\.orders\s+WHERE\s+dt", source
    ), "Missing DELETE FROM staging.orders WHERE dt pattern"


def test_dwh_delete_before_insert():
    """La transformation staging -> dwh doit utiliser DELETE + INSERT."""
    source = _source()
    assert re.search(
        r"DELETE\s+FROM\s+dwh\.fact_orders\s+WHERE\s+dt", source
    ), "Missing DELETE FROM dwh.fact_orders WHERE dt pattern"
    assert re.search(
        r"INSERT\s+INTO\s+dwh\.fact_orders", source
    ), "Missing INSERT INTO dwh.fact_orders pattern"


def test_dwh_insert_selects_from_staging():
    """L'INSERT dans dwh.fact_orders doit lire depuis staging.orders."""
    source = _source()
    assert re.search(
        r"INSERT\s+INTO\s+dwh\.fact_orders.*FROM\s+staging\.orders",
        source,
        re.DOTALL,
    ), "INSERT INTO dwh.fact_orders should SELECT FROM staging.orders"
