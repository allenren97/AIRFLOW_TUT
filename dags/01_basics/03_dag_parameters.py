"""Module 01.03 — A guided tour of the most important DAG parameters.

GOAL
----
Every parameter passed to ``DAG(...)`` here is annotated with WHY you set
it. Open this file and the Airflow docs for ``DAG`` side-by-side.

Key knobs you will use constantly:

* ``dag_id``         — must be unique across the Airflow deployment.
* ``schedule``       — None / cron / preset / timedelta / dataset / Timetable.
* ``start_date``     — the first logical date Airflow can schedule.
* ``catchup``        — if True, backfill from start_date to now.
* ``max_active_runs``— concurrency cap across runs of THIS dag.
* ``max_active_tasks``— concurrency cap across tasks within a run.
* ``default_args``   — applied to every task that does not override.
* ``params``         — typed parameters editable from the "Trigger DAG w/ config" UI.
* ``tags``           — UI filtering only.
* ``doc_md``         — markdown shown on the DAG details page.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DOC_MD = """
### mod_01_03_dag_parameters

A reference DAG showing every commonly-used `DAG(...)` parameter.

* Triggered manually only (``schedule=None``).
* Each task retries once with a 30 second delay.
* Supports a typed ``message`` parameter you can edit in the UI before triggering.
"""

with DAG(
    dag_id="mod_01_03_dag_parameters",
    description="Annotated tour of common DAG parameters.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    dagrun_timeout=timedelta(minutes=15),
    default_args={
        "owner": "learner",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "depends_on_past": False,
        "email_on_failure": False,
    },
    params={
        # Editable in the UI when you click "Trigger DAG w/ config".
        "message": Param(
            "hello",
            type="string",
            title="Message to echo",
            description="Will be echoed by the print_message task.",
        ),
        "iterations": Param(
            3,
            type="integer",
            minimum=1,
            maximum=10,
            title="Iteration count",
        ),
    },
    tags=["module-01", "reference"],
    doc_md=DOC_MD,
) as dag:
    BashOperator(
        task_id="print_message",
        bash_command='for i in $(seq 1 {{ params.iterations }}); do echo "{{ params.message }} #$i"; done',
    )
