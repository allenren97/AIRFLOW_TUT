"""Module 05.03 — Trigger rules.

GOAL
----
The ``trigger_rule`` attribute on a task determines when it runs based on
the state of its DIRECT upstream tasks.

Airflow 2.10 supports these rules (see ``airflow.utils.trigger_rule.TriggerRule``):

  - ``all_success``                (default)
  - ``all_failed``
  - ``all_done``
  - ``all_skipped``
  - ``one_success``
  - ``one_failed``
  - ``one_done``
  - ``none_failed``
  - ``none_failed_min_one_success`` (replaces deprecated ``none_failed_or_skipped``)
  - ``none_skipped``
  - ``always``

This DAG sets up a few common combinations side-by-side so you can run
it once and inspect which downstream tasks ran/were skipped.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="mod_05_03_trigger_rules",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-05", "trigger-rules"],
)
def trigger_rules_pipeline():
    @task
    def succeeds() -> str:
        return "ok"

    @task
    def also_succeeds() -> str:
        return "ok"

    @task
    def fails() -> None:
        raise AirflowFailException("intentional failure to demonstrate trigger rules")

    s1 = succeeds()
    s2 = also_succeeds()
    f1 = fails()

    # Runs only if EVERY upstream succeeded — default rule.
    only_if_all_success = EmptyOperator(
        task_id="only_if_all_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Runs only if EVERY upstream failed.
    only_if_all_failed = EmptyOperator(
        task_id="only_if_all_failed",
        trigger_rule=TriggerRule.ALL_FAILED,
    )

    # Runs once every upstream is "done" — succeeded, failed, or skipped.
    cleanup = EmptyOperator(
        task_id="cleanup_always_runs",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Useful after a branch where some siblings get skipped.
    after_branch = EmptyOperator(
        task_id="after_branch_one_success",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    [s1, s2, f1] >> only_if_all_success
    [s1, s2, f1] >> only_if_all_failed
    [s1, s2, f1] >> cleanup
    [s1, s2, f1] >> after_branch


trigger_rules_pipeline()
