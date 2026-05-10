"""Module 01.01 — Hello World DAG (classic operator style).

GOAL
----
Understand the absolute minimum to define and run a DAG in Airflow 2.10:

  * The ``DAG`` object describes WHEN something runs and HOW its tasks relate.
  * Each ``BashOperator`` / ``PythonOperator`` is a TASK — a unit of work.
  * Dependencies are expressed with the ``>>`` (downstream) operator.

THINGS TO TRY IN THE UI
-----------------------
1. Toggle the DAG ON in the UI (top-left switch).
2. Click "Trigger DAG" and watch the Graph view light up.
3. Click into ``say_hello`` → "Logs" to see the printed output.
4. Change the ``schedule`` below to ``"@hourly"`` and observe the next run.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="mod_01_01_hello_world",
    description="The smallest useful DAG: two BashOperators in sequence.",
    schedule=None,  # only runs when manually triggered
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,  # don't backfill old runs the first time we toggle it on
    tags=["module-01", "beginner"],
) as dag:
    start = EmptyOperator(task_id="start")

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow on $(date)!"',
    )

    end = EmptyOperator(task_id="end")

    start >> say_hello >> end
