"""Module 08.04 — Attaching ``extra`` metadata to dataset events.

GOAL
----
Each dataset event can carry an ``extra`` dict (e.g. row count, file
size, schema version). The downstream DAG can read this metadata via
``triggering_dataset_events`` to make decisions without re-querying
the source.

This is the "data-aware" part of data-aware scheduling: don't just
"file changed", but "this many new rows of this shape arrived".
"""

from __future__ import annotations

import pendulum

from airflow.datasets import Dataset, DatasetAlias
from airflow.decorators import dag, task

EVENTS_ALIAS = DatasetAlias("hourly-events-feed")


@dag(
    dag_id="mod_08_04_event_producer",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "extras"],
)
def event_producer():
    @task(outlets=[EVENTS_ALIAS])
    def emit(*, outlet_events, **context) -> None:
        ds = context["ds"]
        rows = 1234
        outlet_events[EVENTS_ALIAS].add(
            Dataset(f"file:///opt/airflow/include/events/{ds}.json"),
            extra={"row_count": rows, "schema_version": "v3"},
        )

    emit()


@dag(
    dag_id="mod_08_04_event_consumer",
    schedule=EVENTS_ALIAS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "extras"],
)
def event_consumer():
    @task
    def react_to_size(**context) -> None:
        events = context.get("triggering_dataset_events", {})
        for ds_uri, ev_list in events.items():
            for ev in ev_list:
                rows = ev.extra.get("row_count", 0)
                if rows > 1000:
                    print(f"BIG batch for {ds_uri} ({rows} rows) — running heavy path.")
                else:
                    print(f"Small batch for {ds_uri} ({rows} rows) — running light path.")

    react_to_size()


event_producer()
event_consumer()
