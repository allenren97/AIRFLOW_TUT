"""Module 11.01 — Calling an HTTP API with the HTTP provider + TaskFlow.

GOAL
----
There are two common ways to talk to HTTP services:

  1. ``HttpOperator`` (provider) — declarative, supports retry/templating.
  2. Plain ``requests`` inside a ``@task`` — most flexible.

This DAG shows both styles against ``jsonplaceholder.typicode.com``,
seeded via the ``API_BASE_URL`` Airflow Variable (see ``.env.example``).
"""

from __future__ import annotations

import json

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.http.operators.http import HttpOperator


@dag(
    dag_id="mod_11_01_http_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-11", "http", "etl"],
)
def http_pipeline():
    # Style 1: provider operator.
    # In a real environment the connection ``http_jsonplaceholder`` would be
    # configured with host=https://jsonplaceholder.typicode.com.
    todos = HttpOperator(
        task_id="fetch_todos_via_provider",
        method="GET",
        endpoint="todos",
        http_conn_id="http_jsonplaceholder",
        log_response=False,
        do_xcom_push=True,
    )

    # Style 2: plain requests in a TaskFlow task.
    @task
    def fetch_users_via_requests() -> list[dict]:
        base_url = Variable.get("API_BASE_URL", default_var="https://jsonplaceholder.typicode.com")
        resp = requests.get(f"{base_url}/users", timeout=10)
        resp.raise_for_status()
        return resp.json()

    @task
    def summarize(todos_payload: str | None, users: list[dict]) -> dict:
        todos_list = json.loads(todos_payload) if isinstance(todos_payload, str) else (todos_payload or [])
        return {
            "todos": len(todos_list),
            "users": len(users),
            "ratio": (len(todos_list) / max(len(users), 1)) if users else 0,
        }

    @task
    def report(summary: dict) -> None:
        print(f"Summary: {summary}")

    users = fetch_users_via_requests()
    summary = summarize(todos.output, users)
    report(summary)


http_pipeline()
