import sys
import types
from pathlib import Path

import pytest

# Ajouter dags/ au path pour importer map_orders_to_staging_rows
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))


SAMPLE_ORDERS = [
    {
        "id": "ORD-20250101-0001",
        "date": "2025-01-01",
        "seller_id": "SELL-0001",
        "customer_id": "CUST-0042",
        "product_id": "PROD-0003",
        "product": "Gadget Pro",
        "quantity": 5,
        "unit_price": 29.99,
        "total": 149.95,
        "status": "completed",
    },
    {
        "id": "ORD-20250101-0002",
        "date": "2025-01-01",
        "seller_id": "SELL-0002",
        "customer_id": "CUST-0100",
        "product_id": "PROD-0010",
        "product": "Relay Module",
        "quantity": 1,
        "unit_price": 9.99,
        "total": 9.99,
        "status": "pending",
    },
]


@pytest.fixture(scope="module")
def map_orders():
    """Importe map_orders_to_staging_rows en mockant les modules Airflow."""
    airflow_mock = types.ModuleType("airflow")
    decorators_mock = types.ModuleType("airflow.decorators")

    def mock_dag(**kwargs):
        def decorator(f):
            def noop(*args, **kw):
                pass

            return noop

        return decorator

    decorators_mock.dag = mock_dag
    decorators_mock.task = lambda f: f
    airflow_mock.decorators = decorators_mock

    # Injecter les mocks
    saved = {
        k: sys.modules.pop(k, None)
        for k in ["airflow", "airflow.decorators", "marketplace_orders_ingest_daily"]
    }
    sys.modules["airflow"] = airflow_mock
    sys.modules["airflow.decorators"] = decorators_mock

    from marketplace_orders_ingest_daily import map_orders_to_staging_rows

    yield map_orders_to_staging_rows

    # Cleanup : restaurer l'etat original de sys.modules
    for key in ["airflow", "airflow.decorators", "marketplace_orders_ingest_daily"]:
        sys.modules.pop(key, None)
        if saved.get(key) is not None:
            sys.modules[key] = saved[key]


def test_map_orders_returns_correct_count(map_orders):
    """map_orders_to_staging_rows doit retourner autant de tuples que d'orders."""
    rows = map_orders(SAMPLE_ORDERS)
    assert len(rows) == 2


def test_map_orders_extracts_order_id(map_orders):
    """Le premier element du tuple doit etre l'order id."""
    rows = map_orders(SAMPLE_ORDERS)
    assert rows[0][0] == "ORD-20250101-0001"
    assert rows[1][0] == "ORD-20250101-0002"


def test_map_orders_extracts_seller_id(map_orders):
    """Le seller_id doit etre correctement extrait."""
    rows = map_orders(SAMPLE_ORDERS)
    assert rows[0][1] == "SELL-0001"
    assert rows[1][1] == "SELL-0002"


def test_map_orders_tuple_has_8_fields(map_orders):
    """Chaque tuple doit avoir 8 champs (colonnes staging.orders sans loaded_at)."""
    rows = map_orders(SAMPLE_ORDERS)
    for row in rows:
        assert len(row) == 8, f"Expected 8 fields, got {len(row)}: {row}"


def test_map_orders_field_order(map_orders):
    """Les champs doivent etre dans l'ordre : id, seller_id, customer_id, product_id, date, quantity, total, status."""
    row = map_orders(SAMPLE_ORDERS)[0]
    assert row == (
        "ORD-20250101-0001",
        "SELL-0001",
        "CUST-0042",
        "PROD-0003",
        "2025-01-01",
        5,
        149.95,
        "completed",
    )


def test_map_orders_empty_list(map_orders):
    """Une liste vide doit retourner une liste vide."""
    assert map_orders([]) == []
