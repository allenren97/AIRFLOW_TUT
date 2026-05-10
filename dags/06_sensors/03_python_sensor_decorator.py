"""Module 06.03 — ``@task.sensor`` (PythonSensor in TaskFlow style).

GOAL
----
``@task.sensor`` lets you write a custom sensor as a regular Python
function. The function must return either:

  * a bool — ``True`` to continue, ``False`` to keep poking; or
  * a ``PokeReturnValue`` — bool + optional XCom payload.

Returning the ``xcom_value`` lets the sensor pass downstream the data
it found, so you don't need a separate fetch task.
"""

from __future__ import annotations

import random
from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue


@dag(
    dag_id="mod_06_03_python_sensor",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-06", "sensor", "taskflow"],
)
def python_sensor_pipeline():
    @task.sensor(
        poke_interval=5,
        timeout=timedelta(minutes=2).total_seconds(),
        mode="reschedule",
    )
    def wait_for_random_event() -> PokeReturnValue:
        ready = random.random() > 0.7
        return PokeReturnValue(
            is_done=ready,
            xcom_value={"ready_at": pendulum.now("UTC").to_iso8601_string()} if ready else None,
        )

    @task
    def announce(payload: dict) -> None:
        print(f"Sensor finished, found: {payload}")

    announce(wait_for_random_event())


python_sensor_pipeline()
