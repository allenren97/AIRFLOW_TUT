"""Module 04.01 — Classic XCom push / pull.

GOAL
----
XCom (= "cross-communication") is Airflow's small-message bus between
tasks. With classic operators, two patterns dominate:

  1. Implicit push — return a value from your ``python_callable``; Airflow
     stores it under the XCom key ``return_value``.
  2. Explicit push — call ``ti.xcom_push(key="...", value=...)``.

You read it back with ``ti.xcom_pull(task_ids="...", key="...")``.

CAVEAT
------
XCom payloads are stored in the metadata DB. Keep them small (KB, not MB).
For larger blobs, write to S3/GCS and pass the URI through XCom.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def push_function(**context) -> str:
    ti = context["task_instance"]
    ti.xcom_push(key="custom_key", value="explicit-value")
    return "implicit-return-value"


def pull_function(**context) -> None:
    ti = context["task_instance"]
    explicit = ti.xcom_pull(task_ids="push", key="custom_key")
    implicit = ti.xcom_pull(task_ids="push")  # default key=return_value
    print(f"explicit={explicit} | implicit={implicit}")


with DAG(
    dag_id="mod_04_01_classic_xcom",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-04", "xcom"],
) as dag:
    push = PythonOperator(task_id="push", python_callable=push_function)
    pull = PythonOperator(task_id="pull", python_callable=pull_function)
    push >> pull
