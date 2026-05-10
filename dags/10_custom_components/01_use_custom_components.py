"""Module 10.01 — Use the custom Operator / Hook / Sensor from ``plugins/``.

GOAL
----
Tie module 10's plugin classes together in a real DAG. To make this run
end-to-end, create a Connection in the UI:

  Admin → Connections → +
    Conn Id   = greeting_default
    Conn Type = Greeting
    Host      = my-greeting-service
    Extra     = {"prefix": "Howdy"}

Or, to seed it via env var, set:
    AIRFLOW_CONN_GREETING_DEFAULT='greeting://my-greeting-service?prefix=Howdy'
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task

from custom_operator import GreetOperator  # type: ignore[import-not-found]
from custom_sensor import CounterSensor  # type: ignore[import-not-found]


@dag(
    dag_id="mod_10_01_custom_components",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-10", "plugin"],
)
def custom_components_pipeline():
    wait = CounterSensor(
        task_id="wait_for_counter",
        threshold=3,
        poke_interval=2,
        timeout=timedelta(minutes=1).total_seconds(),
        mode="poke",
    )

    greet = GreetOperator(
        task_id="greet",
        name="Airflow Learner",
        greeting_conn_id="greeting_default",
    )

    @task
    def announce(message: str) -> None:
        print(f"Greet operator returned: {message!r}")

    wait >> greet
    announce(greet.output)


custom_components_pipeline()
