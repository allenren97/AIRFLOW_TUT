"""Module 09.05 — Custom Timetable: business-hours-only schedule.

GOAL
----
A ``Timetable`` is the most powerful scheduling primitive — it lets you
express schedules that cron simply can't, like:

  * "Skip US holidays."
  * "Run at 9:30 only on trading days."
  * "Every 4 business hours, but never on weekends."

Implement two methods:

  * ``next_dagrun_info(last_automated_data_interval, restriction)`` —
    return the next data interval to schedule.
  * ``infer_manual_data_interval(run_after)`` — used when the user hits
    "trigger DAG" manually.

This example: run once every weekday at 09:00 UTC.

Custom timetables must be declared via a plugin so Airflow's scheduler
can serialize them. The ``plugins/custom_timetable_plugin.py`` file in
this repo registers it; the DAG references it by import.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task

# Imported from plugins/ so the scheduler can serialize it.
# The plugin file registers ``WeekdayMorningTimetable`` under the
# ``timetables`` plugin entrypoint.
from custom_timetable_plugin import WeekdayMorningTimetable  # type: ignore[import-not-found]


@dag(
    dag_id="mod_09_05_custom_timetable",
    schedule=WeekdayMorningTimetable(),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "timetable"],
)
def custom_timetable_pipeline():
    @task
    def hello(**context) -> None:
        print(f"Running on {context['logical_date']}")

    hello()


custom_timetable_pipeline()
