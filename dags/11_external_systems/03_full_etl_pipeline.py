"""Module 11.03 — Full ETL: HTTP -> transform -> Postgres -> dataset event.

GOAL
----
A small but realistic pipeline tying together a lot of what you've
learned:

  1. Fetch data from an HTTP API.
  2. Transform / validate it in Python.
  3. Load into Postgres.
  4. Publish a Dataset event so a downstream DAG can react.
  5. Use a TaskGroup, retries, and an SLA-style failure callback.

PRECONDITIONS
-------------
* Postgres connection ``postgres_default`` exists.
* HTTP connection ``http_jsonplaceholder`` exists OR Variable
  ``API_BASE_URL`` is set (defaults to JSONPlaceholder).
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests

from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG = logging.getLogger(__name__)

USERS_DATASET = Dataset("postgres://airflow/learning_users")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS learning_users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT,
    company TEXT,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS learning_users_audit (
    run_id TEXT PRIMARY KEY,
    rows_inserted INT,
    finished_at TIMESTAMPTZ DEFAULT now()
);
"""


def alert(context: dict) -> None:
    LOG.error(
        "ETL failed for run %s, task %s",
        context["run_id"],
        context["task_instance"].task_id,
    )


@dag(
    dag_id="mod_11_03_full_etl",
    description="HTTP -> transform -> Postgres -> Dataset event.",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": alert,
    },
    tags=["module-11", "etl", "datasets"],
)
def full_etl():
    setup = SQLExecuteQueryOperator(
        task_id="setup_tables",
        conn_id="postgres_default",
        sql=CREATE_TABLE_SQL,
    )

    @task_group(group_id="extract_transform")
    def extract_transform():
        @task
        def extract() -> list[dict]:
            base = Variable.get("API_BASE_URL", default_var="https://jsonplaceholder.typicode.com")
            resp = requests.get(f"{base}/users", timeout=10)
            resp.raise_for_status()
            return resp.json()

        @task
        def transform(users: list[dict]) -> list[tuple]:
            return [
                (
                    int(u["id"]),
                    str(u["name"]),
                    str(u["email"]).lower(),
                    str(u.get("company", {}).get("name", "")),
                )
                for u in users
                if u.get("email") and u.get("name")
            ]

        return transform(extract())

    @task(outlets=[USERS_DATASET])
    def load(rows: list[tuple], **context) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run("DELETE FROM learning_users;")  # idempotent reload
        hook.insert_rows(
            table="learning_users",
            rows=rows,
            target_fields=["id", "name", "email", "company"],
        )
        hook.insert_rows(
            table="learning_users_audit",
            rows=[(context["run_id"], len(rows))],
            target_fields=["run_id", "rows_inserted"],
            replace=True,
            replace_index=["run_id"],
        )
        return len(rows)

    @task
    def announce(count: int) -> None:
        print(f"Loaded {count} users — downstream DAGs subscribed to {USERS_DATASET.uri} will trigger.")

    rows = extract_transform()
    n = load(rows)
    setup >> rows
    announce(n)


@dag(
    dag_id="mod_11_03_full_etl_consumer",
    description="Triggered when full_etl publishes a fresh users dataset.",
    schedule=USERS_DATASET,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-11", "etl", "datasets", "consumer"],
)
def full_etl_consumer():
    @task
    def run_downstream_analysis() -> None:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        cnt = hook.get_first("SELECT COUNT(*) FROM learning_users;")[0]
        print(f"Downstream analysis sees {cnt} users in learning_users.")

    run_downstream_analysis()


full_etl()
full_etl_consumer()
