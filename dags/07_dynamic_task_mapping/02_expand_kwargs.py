"""Module 07.02 — ``.expand_kwargs()``: vary multiple kwargs together.

GOAL
----
``.expand(a=[...], b=[...])`` creates the **cross product** of the two
iterables — len(a) * len(b) task instances.

When you instead want to pair them — i.e. iteration #1 sees a[0]+b[0],
iteration #2 sees a[1]+b[1] — use ``.expand_kwargs(list_of_dicts)``.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_07_02_expand_kwargs",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-07", "mapping"],
)
def expand_kwargs_pipeline():
    @task
    def make_jobs() -> list[dict]:
        return [
            {"region": "us-east-1", "table": "orders"},
            {"region": "us-east-1", "table": "customers"},
            {"region": "eu-west-1", "table": "orders"},
        ]

    @task
    def run_job(region: str, table: str) -> str:
        return f"{region}:{table}"

    results = run_job.expand_kwargs(make_jobs())

    @task
    def report(rs: list[str]) -> None:
        print(f"Ran {len(rs)} jobs: {rs}")

    report(results)


expand_kwargs_pipeline()
