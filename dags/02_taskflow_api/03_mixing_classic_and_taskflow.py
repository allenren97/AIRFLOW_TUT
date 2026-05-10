"""Module 02.03 — Mixing TaskFlow tasks with classic operators.

GOAL
----
You will frequently need to combine TaskFlow ``@task`` functions with
classic operators (e.g. ``BashOperator``, provider operators, sensors).
Two patterns matter:

  1. ``BashOperator`` instance >> taskflow_call() — works directly.
  2. To send a TaskFlow return value into a classic operator's templated
     field, just reference the XCom in a Jinja expression.

You can also reach the underlying ``BaseOperator`` of a TaskFlow task via
``my_task.operator`` — useful when you must set non-decorator attributes.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="mod_02_03_mixing_taskflow_and_classic",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-02", "taskflow"],
)
def mixed_pipeline():
    start = EmptyOperator(task_id="start")

    @task
    def pick_target() -> str:
        return "world"

    target = pick_target()

    # Use the XCom value inside a classic operator's Jinja template.
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello, {{ ti.xcom_pull(task_ids=\'pick_target\') }}!"',
    )

    end = EmptyOperator(task_id="end")

    # Mixing dependency styles is fine.
    start >> target >> say_hello >> end


mixed_pipeline()
