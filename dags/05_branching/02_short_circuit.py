"""Module 05.02 — ``ShortCircuitOperator`` and ``@task.short_circuit``.

GOAL
----
A short-circuit task returns a truthy / falsy value:
  * Truthy  → all downstream tasks proceed normally.
  * Falsy   → all downstream tasks are SKIPPED.

Use it when you have a precondition that determines whether the rest of
the DAG should run for this interval (e.g. "is today a business day?",
"did the upstream API actually return data?").

The ``ignore_downstream_trigger_rules`` flag controls whether the skip
propagates through tasks that have non-default trigger rules.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="mod_05_02_short_circuit",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-05", "branching"],
)
def short_circuit_pipeline():
    @task
    def detect_records() -> int:
        # Pretend we polled an upstream system.
        return 0  # change to >0 to see the downstream tasks run.

    @task.short_circuit
    def has_records(count: int) -> bool:
        return count > 0

    @task
    def process() -> None:
        print("Processing records!")

    end = EmptyOperator(task_id="end")

    has_records(detect_records()) >> process() >> end


short_circuit_pipeline()
