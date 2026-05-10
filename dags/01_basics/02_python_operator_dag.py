"""Module 01.02 — PythonOperator and the task context.

GOAL
----
Learn the *classic* way to run Python code in a task:

  * ``PythonOperator(python_callable=...)`` calls a regular function.
  * ``op_kwargs`` / ``op_args`` pass arguments at parse time (templated!).
  * The function can accept ``**context`` to read the runtime context dict
    (logical date, run id, dag, task instance, etc.).

The TaskFlow API (covered in module 02) is now the recommended style for
new DAGs, but you must still understand the classic style because lots of
real-world Airflow code uses it.
"""

from __future__ import annotations

import logging

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

LOG = logging.getLogger(__name__)


def greet(name: str, **context) -> None:
    """Plain Python function — Airflow calls this during execution."""
    ti = context["task_instance"]
    LOG.info(
        "Hello %s! Run %s of task %s on logical date %s (try #%d).",
        name,
        context["run_id"],
        ti.task_id,
        context["logical_date"],
        ti.try_number,
    )


def summarize(**context) -> str:
    """Return a value — it gets pushed to XCom under the key ``return_value``."""
    return f"Summary for {context['run_id']}"


with DAG(
    dag_id="mod_01_02_python_operator",
    description="PythonOperator basics: op_kwargs, context, and return-value XCom.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-01", "beginner"],
) as dag:
    greet_task = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"name": "Airflow Learner"},
    )

    summary_task = PythonOperator(
        task_id="summarize",
        python_callable=summarize,
    )

    greet_task >> summary_task
