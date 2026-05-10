"""Module 01.05 — ``default_args``, ``catchup``, and backfilling.

GOAL
----
* See how ``default_args`` cascades to every task in the DAG.
* See how ``catchup=True`` causes Airflow to schedule a run for every
  missed interval between ``start_date`` and "now" the moment the DAG is
  unpaused.

CAREFUL
-------
``catchup=True`` + a ``start_date`` far in the past can spawn HUNDREDS of
runs the instant you toggle the DAG on. Always pair it with
``max_active_runs`` and an idempotent task design.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "learner",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}


def _print_interval(**context) -> None:
    print(
        f"This task is processing window "
        f"[{context['data_interval_start']} .. {context['data_interval_end']})"
    )


with DAG(
    dag_id="mod_01_05_default_args_and_catchup",
    description="Demonstrates default_args and (intentionally) limited catchup.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 6, 5, tz="UTC"),  # bounded for safety
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["module-01", "scheduling"],
) as dag:
    BashOperator(
        task_id="echo_interval",
        bash_command='echo "Interval start: {{ data_interval_start }}"',
    ) >> PythonOperator(
        task_id="print_interval_python",
        python_callable=_print_interval,
        # If we wanted to *override* a default for just this task, we would
        # do it here, e.g. retries=0.
    )
