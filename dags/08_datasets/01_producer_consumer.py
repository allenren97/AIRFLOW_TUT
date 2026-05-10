"""Module 08.01 — Datasets (data-aware scheduling): producer & consumer.

GOAL
----
A ``Dataset`` is a logical handle for a piece of data (S3 object, table,
file, etc.). When a TASK with that dataset in its ``outlets`` succeeds,
Airflow records a *dataset event*. Any DAG whose ``schedule=`` references
that dataset is then queued to run.

This unlocks **data-aware scheduling**: instead of guessing when an
upstream pipeline finishes, downstream DAGs trigger the moment the data
is fresh.

In the UI: open the "Datasets" tab to see the graph of producers and
consumers.
"""

from __future__ import annotations

import pendulum

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Datasets are usually defined once at module scope so producer and
# consumer DAGs can both import the same identity.
ORDERS_DATASET = Dataset("file:///opt/airflow/include/orders.parquet")


@dag(
    dag_id="mod_08_01_dataset_producer",
    description="Writes the orders dataset and emits a dataset event on success.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "producer"],
)
def producer_dag():
    @task(outlets=[ORDERS_DATASET])
    def write_orders() -> None:
        # Pretend we just wrote a parquet file at ORDERS_DATASET.uri.
        # The fact that this task SUCCEEDED is what publishes the event.
        print(f"Refreshed dataset {ORDERS_DATASET.uri}")

    write_orders()


@dag(
    dag_id="mod_08_01_dataset_consumer",
    description="Runs whenever the orders dataset is updated.",
    schedule=[ORDERS_DATASET],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-08", "datasets", "consumer"],
)
def consumer_dag():
    @task
    def consume_orders() -> None:
        print(f"Consuming the latest version of {ORDERS_DATASET.uri}")

    consume_orders()


producer_dag()
consumer_dag()
