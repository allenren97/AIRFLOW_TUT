"""Module 06.02 — ``ExternalTaskSensor``: wait on a task in another DAG.

GOAL
----
``ExternalTaskSensor`` waits for a task (or whole DAG) in ANOTHER DAG to
reach a target state for the same logical date. Two big knobs:

  * ``external_dag_id`` / ``external_task_id`` — what to wait on.
  * ``execution_delta`` or ``execution_date_fn`` — align logical dates if
    the two DAGs run on different schedules.
  * ``allowed_states`` / ``failed_states``      — what counts as "done".

Modern alternative: use **Datasets** (module 08) — they are usually
cleaner because the producer DAG explicitly publishes the dependency.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState


@dag(
    dag_id="mod_06_02_external_task_sensor",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-06", "sensor"],
)
def downstream_pipeline():
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream_dag",
        external_dag_id="mod_01_05_default_args_and_catchup",
        external_task_id="echo_interval",
        allowed_states=[TaskInstanceState.SUCCESS],
        failed_states=[TaskInstanceState.FAILED, TaskInstanceState.SKIPPED],
        # Both DAGs run @daily so logical dates already align.
        execution_delta=timedelta(0),
        poke_interval=30,
        timeout=timedelta(hours=1).total_seconds(),
        mode="reschedule",
    )

    @task
    def downstream_work() -> None:
        print("Upstream is done — proceeding.")

    wait_for_upstream >> downstream_work()


downstream_pipeline()
