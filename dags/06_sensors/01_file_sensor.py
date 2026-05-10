"""Module 06.01 — ``FileSensor`` and the ``poke`` vs ``reschedule`` modes.

GOAL
----
A sensor is just a special operator whose job is to wait for a condition.
Three execution modes:

  * ``mode='poke'``        — keeps a worker slot occupied while polling. Cheap,
                             but blocks one slot per waiting sensor. OK for short waits.
  * ``mode='reschedule'``  — releases the slot between pokes. Use for long waits.
  * deferrable             — uses the triggerer process (covered in 04_deferrable).

KEY PARAMETERS (apply to almost every sensor)
---------------------------------------------
  * ``timeout``            — maximum total wait, raises if exceeded.
  * ``poke_interval``      — seconds between pokes.
  * ``soft_fail``          — if True, mark task as SKIPPED on timeout instead of FAILED.
  * ``exponential_backoff``— back off between pokes.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor


@dag(
    dag_id="mod_06_01_file_sensor",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-06", "sensor"],
)
def file_sensor_pipeline():
    # In the default Airflow image this connection points at /opt/airflow.
    # Create the file with `touch /opt/airflow/include/heartbeat.txt` to satisfy the sensor.
    wait_for_file = FileSensor(
        task_id="wait_for_heartbeat",
        filepath="include/heartbeat.txt",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=timedelta(minutes=2).total_seconds(),
        mode="reschedule",  # Free worker slots while waiting.
        soft_fail=False,
    )

    @task
    def react_to_file() -> None:
        print("File appeared! Starting work.")

    wait_for_file >> react_to_file()


file_sensor_pipeline()
