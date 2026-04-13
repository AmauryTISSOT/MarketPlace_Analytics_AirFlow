import sys
import os
from unittest.mock import MagicMock, patch
import pytest

# on ajoute le dossier plugins au path pour pouvoir importer l'operateur
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))

from operators.data_quality_operator import DataQualityOperator


@pytest.fixture
def mock_pg_hook():
    with patch("operators.data_quality_operator.PostgresHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        yield mock_hook


def make_operator(rules):
    op = DataQualityOperator(
        task_id="test_dq",
        rules=rules,
        postgres_conn_id="postgres_dwh",
    )
    return op


def test_toutes_regles_ok(mock_pg_hook):
    # le mock retourne 0 pour chaque regle
    mock_pg_hook.get_first.return_value = (0,)

    rules = [
        {
            "name": "not_null",
            "sql": "SELECT COUNT(*) FROM staging.orders WHERE order_id IS NULL",
        },
        {
            "name": "not_empty",
            "sql": "SELECT COUNT(*) FROM staging.orders WHERE status IS NULL",
        },
    ]

    op = make_operator(rules)
    result = op.execute(context={})

    assert result == "pass"
    assert mock_pg_hook.get_first.call_count == 2


def test_une_regle_echoue(mock_pg_hook):
    # premiere regle OK, deuxieme regle KO (3 lignes en erreur)
    mock_pg_hook.get_first.side_effect = [(0,), (3,)]

    rules = [
        {
            "name": "not_null",
            "sql": "SELECT COUNT(*) FROM staging.orders WHERE order_id IS NULL",
        },
        {
            "name": "quantity_ok",
            "sql": "SELECT COUNT(*) FROM staging.orders WHERE quantity <= 0",
        },
    ]

    op = make_operator(rules)
    result = op.execute(context={})

    assert result == "fail"


def test_toutes_regles_echouent(mock_pg_hook):
    mock_pg_hook.get_first.return_value = (5,)

    rules = [
        {"name": "r1", "sql": "SELECT 1"},
        {"name": "r2", "sql": "SELECT 1"},
        {"name": "r3", "sql": "SELECT 1"},
    ]

    op = make_operator(rules)
    result = op.execute(context={})

    assert result == "fail"
    assert mock_pg_hook.get_first.call_count == 3


def test_resultat_none(mock_pg_hook):
    mock_pg_hook.get_first.return_value = None

    rules = [
        {"name": "check_vide", "sql": "SELECT COUNT(*) FROM staging.orders WHERE 1=0"},
    ]

    op = make_operator(rules)
    result = op.execute(context={})

    assert result == "pass"


def test_bonne_connexion_utilisee(mock_pg_hook):
    mock_pg_hook.get_first.return_value = (0,)

    with patch("operators.data_quality_operator.PostgresHook") as mock_class:
        mock_class.return_value = mock_pg_hook

        op = DataQualityOperator(
            task_id="test_conn",
            rules=[{"name": "r1", "sql": "SELECT 1"}],
            postgres_conn_id="ma_connexion_custom",
        )
        op.execute(context={})

        mock_class.assert_called_once_with(postgres_conn_id="ma_connexion_custom")
