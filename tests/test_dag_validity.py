"""Validate every DAG file in ``dags/`` parses without errors.

Run with::

    pytest tests/

This is the cheapest, highest-value test suite for an Airflow project.
If you ever accidentally introduce a circular dependency, an undefined
variable, or break the DAG graph, this catches it before deployment.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from airflow.models.dagbag import DagBag

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    """The single most useful Airflow test: every DAG file must import."""
    if dagbag.import_errors:
        formatted = "\n".join(f"{path}: {err}" for path, err in dagbag.import_errors.items())
        pytest.fail(f"DAG import errors found:\n{formatted}")


def test_dag_ids_are_unique(dagbag: DagBag) -> None:
    seen: dict[str, str] = {}
    for dag_id, dag in dagbag.dags.items():
        if dag_id in seen:
            pytest.fail(f"Duplicate dag_id {dag_id!r} in {dag.fileloc} and {seen[dag_id]}.")
        seen[dag_id] = dag.fileloc


def test_every_dag_has_tags_and_owner(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} has no tags."
        for task in dag.tasks:
            assert task.owner, f"Task {dag_id}.{task.task_id} has no owner."


@pytest.mark.parametrize(
    "module_relpath",
    sorted(str(p.relative_to(DAGS_FOLDER)) for p in DAGS_FOLDER.rglob("*.py") if p.name != "__init__.py"),
)
def test_dag_module_importable(module_relpath: str) -> None:
    """Each DAG file should also be loadable as a plain Python module."""
    full_path = DAGS_FOLDER / module_relpath
    spec = importlib.util.spec_from_file_location(full_path.stem, full_path)
    assert spec and spec.loader, f"Could not build import spec for {full_path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
