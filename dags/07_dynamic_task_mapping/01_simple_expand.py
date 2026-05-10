"""Module 07.01 — ``.expand()`` (Dynamic Task Mapping).

GOAL
----
Dynamic Task Mapping (Airflow 2.3+) creates many task instances at
RUN time, one per element of an iterable. The number of mapped tasks is
not known until the upstream task that produces the iterable runs.

Use ``.expand(arg=iterable)`` for kwargs and ``.expand(arg)`` for the
single positional case. Each mapped task instance gets ONE element of
the iterable as its argument.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_07_01_simple_expand",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-07", "mapping"],
)
def simple_expand_pipeline():
    @task
    def list_files() -> list[str]:
        return ["a.csv", "b.csv", "c.csv", "d.csv"]

    @task
    def process(filename: str) -> int:
        # Each mapped task instance only sees ONE filename.
        print(f"Processing {filename}")
        return len(filename)

    @task
    def total(sizes: list[int]) -> None:
        # Mapped task results are gathered into a list automatically.
        print(f"Total sum of sizes = {sum(sizes)}")

    sizes = process.expand(filename=list_files())
    total(sizes)


simple_expand_pipeline()
