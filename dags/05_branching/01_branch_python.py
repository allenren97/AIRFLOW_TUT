"""Module 05.01 — Branching with ``@task.branch``.

GOAL
----
A "branching" task picks one (or several) downstream task IDs to follow;
the others are skipped. Two equivalent options:

  * ``@task.branch`` decorator (preferred TaskFlow style).
  * Classic ``BranchPythonOperator``.

KEY DETAIL
----------
After a branch, downstream tasks that converge from BOTH branches must
set ``trigger_rule="none_failed_min_one_success"`` (or similar) — see
``03_trigger_rules.py``. Otherwise they are skipped because at least one
parent was skipped.
"""

from __future__ import annotations

import random

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="mod_05_01_branch_python",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-05", "branching"],
)
def branch_pipeline():
    @task.branch
    def pick_path() -> str:
        # Could read from an XCom, a Variable, or external system.
        return "process_high_priority" if random.random() > 0.5 else "process_low_priority"

    @task
    def process_high_priority() -> None:
        print("Working on the high-priority lane.")

    @task
    def process_low_priority() -> None:
        print("Working on the low-priority lane.")

    join = EmptyOperator(
        task_id="join",
        # Without this, ``join`` would be skipped because one branch is skipped.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    branch = pick_path()
    branch >> [process_high_priority(), process_low_priority()] >> join


branch_pipeline()
