"""Module 07.04 — Zipping XComs and mapping a TaskGroup.

GOAL
----
* You can map over an entire ``@task_group`` — every mapped instance
  produces a copy of the whole group with each task expanded for that
  iteration.  This is the cleanest way to model "for each X, run a
  multi-step pipeline".
* ``.expand_kwargs(list_of_dicts)`` is the simplest way to pair multiple
  arguments element-wise, even when the dicts come from different
  upstream tasks.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="mod_07_04_mapped_task_group",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-07", "mapping", "task-group"],
)
def mapped_task_group_pipeline():
    @task
    def list_jobs() -> list[dict]:
        return [
            {"region": "us", "table": "orders"},
            {"region": "eu", "table": "customers"},
            {"region": "ap", "table": "events"},
        ]

    @task_group(group_id="process_one_job")
    def process_one_job(region: str, table: str):
        """An entire mini-pipeline applied per job."""

        @task
        def stage(region: str, table: str) -> str:
            return f"staged-{region}-{table}"

        @task
        def commit(staged: str) -> None:
            print(f"Committed {staged}")

        commit(stage(region, table))

    # ``.expand_kwargs`` on a TaskGroup unpacks each dict as kwargs
    # for the group, so we get one fully-expanded copy per dict.
    process_one_job.expand_kwargs(list_jobs())


mapped_task_group_pipeline()
