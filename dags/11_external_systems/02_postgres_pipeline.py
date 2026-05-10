"""Module 11.02 — Postgres provider: SQL operator + hook.

GOAL
----
* ``SQLExecuteQueryOperator`` (or older ``PostgresOperator``) runs SQL
  statements against a connection.
* ``PostgresHook`` is the imperative counterpart — useful inside a
  ``@task`` when you want pandas/psycopg2 control.

PRECONDITION
------------
Create a Postgres connection in Admin → Connections:
    Conn Id   = postgres_default
    Conn Type = Postgres
    Host      = postgres   (or wherever your DB lives)
    Schema    = airflow
    Login/Password = airflow / airflow
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS learning_orders (
    id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now()
);
"""


@dag(
    dag_id="mod_11_02_postgres_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-11", "postgres", "etl"],
)
def postgres_pipeline():
    create = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql=CREATE_TABLE_SQL,
    )

    @task
    def insert_rows() -> int:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = [("Alice", 12.50), ("Bob", 88.10), ("Carol", 23.99)]
        hook.insert_rows(table="learning_orders", rows=rows, target_fields=["customer", "amount"])
        return len(rows)

    @task
    def report(inserted: int) -> None:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        total = hook.get_first("SELECT COUNT(*) FROM learning_orders;")[0]
        print(f"Inserted {inserted} rows; table now has {total} total rows.")

    inserted = insert_rows()
    create >> inserted >> report(inserted)


postgres_pipeline()
