"""Module 02.01 — TaskFlow API basics.

GOAL
----
The TaskFlow API (introduced in Airflow 2.0) lets you write DAGs as if
they were ordinary Python functions:

  * ``@dag`` decorator builds the DAG.
  * ``@task`` decorator turns a function into a task. Calling the task
    function inside the DAG body returns an XCom reference, NOT the value.
  * Passing a task's return value to another task automatically:
      - registers the upstream/downstream dependency, and
      - pushes the return value through XCom for you.

You will use this style for >90% of new DAGs in Airflow 2.10. The
classic operator style is still useful for SQL/HTTP/etc. operators.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_02_01_taskflow_basics",
    description="The smallest TaskFlow DAG: extract -> transform -> load.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-02", "taskflow"],
)
def taskflow_pipeline():
    @task
    def extract() -> dict[str, int]:
        """Pretend to load some data from a source."""
        return {"orders": 42, "refunds": 3}

    @task
    def transform(payload: dict[str, int]) -> int:
        """Compute net orders."""
        return payload["orders"] - payload["refunds"]

    @task
    def load(net: int) -> None:
        """Pretend to write somewhere."""
        print(f"Loaded {net} net orders.")

    raw = extract()
    net = transform(raw)
    load(net)


taskflow_pipeline()
