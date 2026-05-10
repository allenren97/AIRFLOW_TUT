"""Module 02.02 — Multiple outputs from a single TaskFlow task.

GOAL
----
By default, a ``@task`` function pushes ONE return value to XCom under the
key ``return_value``.  When you set ``multiple_outputs=True`` and return a
dict, Airflow pushes each dict key as a separate XCom entry — so you can
fan-out to several downstream tasks.

NOTE
----
``multiple_outputs=True`` is inferred automatically when the function's
return type annotation is a ``TypedDict`` or ``-> dict[str, X]``, so you
can often skip the flag in modern Airflow.
"""

from __future__ import annotations

from typing import TypedDict

import pendulum

from airflow.decorators import dag, task


class StatsPayload(TypedDict):
    total: int
    average: float
    rows: int


@dag(
    dag_id="mod_02_02_multiple_outputs",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-02", "taskflow", "xcom"],
)
def multi_output_pipeline():
    @task(multiple_outputs=True)
    def compute_stats() -> StatsPayload:
        values = [10, 20, 30, 40]
        return {
            "total": sum(values),
            "average": sum(values) / len(values),
            "rows": len(values),
        }

    @task
    def report_total(total: int) -> None:
        print(f"Total = {total}")

    @task
    def report_average(average: float) -> None:
        print(f"Average = {average:.2f}")

    stats = compute_stats()
    # Each TypedDict key is its own XCom entry; reach into it directly.
    report_total(stats["total"])
    report_average(stats["average"])


multi_output_pipeline()
