"""Module 06.04 — Deferrable operators / sensors.

GOAL
----
A *deferrable* operator hands its work off to the **triggerer** process
and releases its worker slot. The triggerer wakes the task back up via
an asyncio event, which means you can have thousands of waiting tasks
without consuming worker slots.

Most provider sensors expose ``deferrable=True`` for this. Example here
uses ``TimeDeltaSensorAsync`` (Airflow built-in) which simply sleeps,
but the same pattern applies to S3KeySensor, HttpSensor, etc.

Requirement
-----------
The Airflow triggerer must be running. With the bundled docker-compose
file in this repo it is.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensorAsync


@dag(
    dag_id="mod_06_04_deferrable_sensor",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-06", "sensor", "deferrable"],
)
def deferrable_pipeline():
    wait = TimeDeltaSensorAsync(
        task_id="wait_30s",
        delta=timedelta(seconds=30),
    )

    @task
    def keep_going() -> None:
        print("Triggerer woke me up.")

    wait >> keep_going()


deferrable_pipeline()
