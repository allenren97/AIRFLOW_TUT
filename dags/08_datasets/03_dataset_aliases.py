"""Module 08.03 — ``DatasetAlias`` (NEW in Airflow 2.10).

GOAL
----
A ``DatasetAlias`` is a stable name that *resolves* to one or more real
``Dataset`` URIs determined at runtime. Two big use cases:

  1. The exact URI is only known at task execution time
     (e.g. ``s3://bucket/run-id=2025-05-10T00:00:00/...``).
  2. You want to schedule a downstream DAG against a CONCEPT (``customer-data``)
     while letting the producer publish concrete dataset events under it.

USAGE
-----
* Producer: declare ``outlets=[DatasetAlias("name")]`` and inside the
  task call
  ``outlet_events[DatasetAlias("name")].add(Dataset("s3://..."))``.
* Consumer: schedule against ``DatasetAlias("name")`` (or against a
  concrete ``Dataset`` if you already know the URI).

Open the "Dataset Aliases" tab in the 2.10 UI to see resolved members.
"""

from __future__ import annotations

import pendulum

from airflow.datasets import Dataset, DatasetAlias
from airflow.decorators import dag, task

DAILY_REPORT_ALIAS = DatasetAlias("daily-customer-report")


@dag(
    dag_id="mod_08_03_alias_producer",
    description="Emits dataset events through a DatasetAlias resolved at runtime.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "alias", "producer"],
)
def alias_producer():
    @task(outlets=[DAILY_REPORT_ALIAS])
    def write_report(*, outlet_events, **context) -> None:
        ds = context["ds"]
        # Build the concrete dataset URI for this run.
        concrete = Dataset(f"file:///opt/airflow/include/reports/{ds}/customers.parquet")
        outlet_events[DAILY_REPORT_ALIAS].add(concrete, extra={"ds": ds})
        print(f"Published {concrete.uri} under alias {DAILY_REPORT_ALIAS.name}")

    write_report()


@dag(
    dag_id="mod_08_03_alias_consumer",
    description="Triggered when ANY dataset under the alias receives an event.",
    schedule=DAILY_REPORT_ALIAS,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "alias", "consumer"],
)
def alias_consumer():
    @task
    def consume(**context) -> None:
        events = context.get("triggering_dataset_events", {})
        for ds_uri, ev_list in events.items():
            for ev in ev_list:
                print(f"Got event for {ds_uri} extra={ev.extra}")

    consume()


alias_producer()
alias_consumer()
