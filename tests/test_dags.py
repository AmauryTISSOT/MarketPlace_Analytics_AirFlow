import ast
import re
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def _dag_files():
    return list(DAGS_DIR.glob("*.py"))


def _read_source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_no_syntax_errors():
    """Tous les fichiers DAG doivent se parser sans erreur de syntaxe."""
    errors = {}
    for dag_file in _dag_files():
        try:
            ast.parse(_read_source(dag_file), filename=dag_file.name)
        except SyntaxError as exc:
            errors[dag_file.name] = str(exc)
    assert not errors, f"Syntax errors found:\n" + "\n".join(
        f"  {k}: {v}" for k, v in errors.items()
    )


def test_dags_found():
    """Au moins un fichier DAG doit etre present dans dags/."""
    assert len(_dag_files()) > 0, "No DAG files found in dags/ folder"


def test_no_catchup_true():
    """Aucun DAG ne doit avoir catchup=True (anti-pattern)."""
    violations = []
    for dag_file in _dag_files():
        source = _read_source(dag_file)
        if re.search(r"catchup\s*=\s*True", source):
            violations.append(dag_file.name)
    assert not violations, f"DAGs with catchup=True: {violations}"


def test_all_dags_have_tags():
    """Tous les DAGs decores avec @dag doivent avoir un parametre tags=."""
    violations = []
    for dag_file in _dag_files():
        source = _read_source(dag_file)
        if "@dag(" in source and "tags=" not in source:
            violations.append(dag_file.name)
    assert not violations, f"DAGs without tags: {violations}"
