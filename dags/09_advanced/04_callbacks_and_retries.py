"""Module 09.04 — Callbacks, retries, and (the now-soft-deprecated) SLA.

GOAL
----
DAG and task callbacks are how you wire alerting / cleanup logic:

  * ``on_success_callback`` — task or DAG succeeded.
  * ``on_failure_callback`` — task or DAG failed (after final retry).
  * ``on_retry_callback``   — between retries.
  * ``on_skipped_callback`` — task got skipped.

You can pass a single callable OR a list of callables. The callback
receives the standard Airflow context dict — handy for sending Slack /
PagerDuty / Datadog events.

Retries:
  * ``retries`` (int) and ``retry_delay`` (timedelta).
  * ``retry_exponential_backoff=True`` plus ``max_retry_delay`` for
    capped exponential backoff.

> Note: Airflow 2.10 deprecates classic SLAs in favor of the upcoming
> "Deadline" feature (Airflow 3). For 2.10 you can still use ``sla=`` on
> tasks and ``sla_miss_callback=`` on the DAG, but the newer pattern is
> to detect breaches yourself in callbacks.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

LOG = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    ti = context["task_instance"]
    LOG.error(
        "ALERT: %s.%s failed on try %d (run %s).",
        context["dag"].dag_id,
        ti.task_id,
        ti.try_number,
        context["run_id"],
    )


def cleanup_on_success(context: dict) -> None:
    LOG.info("DAG %s succeeded — running cleanup.", context["dag"].dag_id)


@dag(
    dag_id="mod_09_04_callbacks_and_retries",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "callbacks", "retries"],
    on_success_callback=cleanup_on_success,
    on_failure_callback=alert_on_failure,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "on_retry_callback": lambda ctx: LOG.warning(
            "Retrying %s (try %d)", ctx["task_instance"].task_id, ctx["task_instance"].try_number
        ),
    },
)
def callbacks_pipeline():
    @task
    def flaky() -> None:
        # Run a few times — see retry_exponential_backoff in action.
        raise AirflowException("simulated transient failure")

    @task(on_failure_callback=alert_on_failure)
    def task_specific_callbacks() -> None:
        print("Per-task callbacks override DAG defaults if both set.")

    flaky() >> task_specific_callbacks()


callbacks_pipeline()
