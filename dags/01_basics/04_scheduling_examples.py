"""Module 01.04 — Scheduling: presets, cron, timedelta.

GOAL
----
Understand the four ways to set ``schedule=...``:

  1. ``None``                       — manual triggers only.
  2. Preset string                  — ``@hourly``, ``@daily``, ``@weekly``, ``@monthly``, ``@yearly``, ``@once``.
  3. Cron expression                — ``"30 14 * * 1-5"`` (= 14:30 weekdays).
  4. ``timedelta``                  — relative interval, e.g. every 15 minutes.

(Datasets and Timetables are scheduling sources too — covered in modules
08 and 09 respectively.)

KEY CONCEPT — logical_date vs run start
---------------------------------------
A DAG run for the interval ``[data_interval_start, data_interval_end)``
is queued AFTER ``data_interval_end`` has passed. So a ``@daily`` run
labelled ``2025-01-01`` actually runs at ``00:00 UTC on 2025-01-02`` —
because Airflow waits for the interval to be complete before processing
it. This is intentional: ETL usually wants a closed period.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# A small helper so we don't repeat ourselves while creating four DAGs.
def _make_dag(dag_id: str, schedule, *, doc: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        tags=["module-01", "scheduling"],
        doc_md=doc,
    ) as dag:
        BashOperator(
            task_id="print_logical_date",
            bash_command='echo "Logical date: {{ ds }} ({{ data_interval_start }} -> {{ data_interval_end }})"',
        )
    return dag


manual_dag = _make_dag(
    "mod_01_04_schedule_manual",
    None,
    doc="`schedule=None` — only ever runs from a manual trigger.",
)

daily_dag = _make_dag(
    "mod_01_04_schedule_daily",
    "@daily",
    doc="`schedule='@daily'` — preset shortcut for `0 0 * * *` (UTC midnight).",
)

cron_weekday_dag = _make_dag(
    "mod_01_04_schedule_cron",
    "30 14 * * 1-5",
    doc="`schedule='30 14 * * 1-5'` — 14:30 on Monday through Friday.",
)

every_15_min_dag = _make_dag(
    "mod_01_04_schedule_timedelta",
    timedelta(minutes=15),
    doc="`schedule=timedelta(minutes=15)` — relative interval.",
)
