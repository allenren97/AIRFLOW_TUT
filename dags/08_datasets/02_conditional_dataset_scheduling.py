"""Module 08.02 — Conditional dataset scheduling (Airflow 2.9+, polished in 2.10).

GOAL
----
Combine multiple datasets with logical operators to express richer
trigger semantics:

  * ``A & B``                 — wait for BOTH datasets to be updated.
  * ``A | B``                 — trigger when EITHER dataset is updated.
  * ``A | (B & C)``           — arbitrary combinations, with parens.

Behind the scenes Airflow stores a dataset expression and evaluates it
each time one of the referenced datasets receives an event.
"""

from __future__ import annotations

import pendulum

from airflow.datasets import Dataset
from airflow.decorators import dag, task

USERS_DATASET = Dataset("file:///opt/airflow/include/users.parquet")
ORDERS_DATASET = Dataset("file:///opt/airflow/include/orders.parquet")
PROMOTIONS_DATASET = Dataset("file:///opt/airflow/include/promotions.parquet")


@dag(
    dag_id="mod_08_02_users_producer",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "producer"],
)
def users_producer():
    @task(outlets=[USERS_DATASET])
    def refresh() -> None:
        print(f"Refreshed {USERS_DATASET.uri}")

    refresh()


@dag(
    dag_id="mod_08_02_orders_producer",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "producer"],
)
def orders_producer():
    @task(outlets=[ORDERS_DATASET])
    def refresh() -> None:
        print(f"Refreshed {ORDERS_DATASET.uri}")

    refresh()


@dag(
    dag_id="mod_08_02_promotions_producer",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "producer"],
)
def promotions_producer():
    @task(outlets=[PROMOTIONS_DATASET])
    def refresh() -> None:
        print(f"Refreshed {PROMOTIONS_DATASET.uri}")

    refresh()


# ---- Consumer: trigger only when (users AND orders) are both fresh, ---------
# ---- OR when promotions is refreshed (regardless of the others).  ---------
@dag(
    dag_id="mod_08_02_conditional_consumer",
    description="Triggers on (users & orders) | promotions.",
    schedule=(USERS_DATASET & ORDERS_DATASET) | PROMOTIONS_DATASET,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "consumer"],
)
def conditional_consumer():
    @task
    def react(**context) -> None:
        # ``context["triggering_dataset_events"]`` lists the events that
        # caused this run — handy for branching by which input arrived.
        events = context.get("triggering_dataset_events", {})
        print(f"Triggered by {sum(len(v) for v in events.values())} events: {dict(events)}")

    react()


users_producer()
orders_producer()
promotions_producer()
conditional_consumer()
