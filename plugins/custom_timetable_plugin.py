"""Custom Timetable that fires once per weekday at 09:00 UTC.

Two key methods:

* ``next_dagrun_info`` — given the last automated interval and the
  scheduling restriction (start/end dates and catchup mode), return the
  NEXT ``DagRunInfo`` to schedule, or ``None`` if there isn't one yet.
* ``infer_manual_data_interval`` — when a user clicks "Trigger DAG"
  manually, decide what the data interval is for that ad-hoc run.

Custom timetables MUST be registered through an ``AirflowPlugin`` so the
serialization layer can recover them across scheduler restarts.
"""

from __future__ import annotations

from typing import Any

import pendulum
from pendulum import DateTime

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class WeekdayMorningTimetable(Timetable):
    """Run once at 09:00 UTC every weekday (Mon-Fri)."""

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # For manual runs, treat the interval as "the calendar day before run_after".
        end = run_after
        start = end.subtract(days=1)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        # Pick the candidate "next 09:00 UTC" after the previous run.
        if last_automated_data_interval is None:
            # First-ever run: anchor at restriction.earliest (or now).
            next_start = restriction.earliest or pendulum.now("UTC")
            next_start = next_start.replace(hour=9, minute=0, second=0, microsecond=0)
            if next_start < (restriction.earliest or next_start):
                next_start = next_start.add(days=1)
        else:
            next_start = last_automated_data_interval.end.add(days=1).replace(hour=9, minute=0)

        # Skip weekends (Saturday=5, Sunday=6).
        while next_start.weekday() >= 5:
            next_start = next_start.add(days=1)

        end = next_start.add(days=1)
        if restriction.latest is not None and next_start > restriction.latest:
            return None
        return DagRunInfo.interval(start=next_start, end=end)


class WeekdayMorningTimetablePlugin(AirflowPlugin):
    name = "weekday_morning_timetable_plugin"
    timetables = [WeekdayMorningTimetable]
