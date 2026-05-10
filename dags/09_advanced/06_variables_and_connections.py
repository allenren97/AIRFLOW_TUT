"""Module 09.06 — Variables and Connections.

GOAL
----
* ``Variable`` — small key/value pairs (strings or JSON), edited in
  Admin → Variables. Use for environment-specific config you don't want
  to hardcode.
* ``Connection`` — credentials + endpoint metadata for external systems
  (Postgres, S3, HTTP, ...). Edited in Admin → Connections. Hooks pull
  from these by ``conn_id``.

PROVISIONING TIP
----------------
Both can be seeded via env vars:
  * ``AIRFLOW_VAR_<KEY>=value``                — Variable.
  * ``AIRFLOW_CONN_<CONN_ID>=postgres://...``  — Connection (URI form).

The example ``.env.example`` in this repo seeds two Variables.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="mod_09_06_variables_and_connections",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "variables", "connections"],
)
def vars_pipeline():
    @task
    def read_variables() -> None:
        # Read with a default so the task doesn't crash if the variable is missing.
        greeting = Variable.get("GREETING", default_var="hello (default)")
        api = Variable.get("API_BASE_URL", default_var="https://example.com")
        print(f"GREETING={greeting!r} API_BASE_URL={api!r}")

    @task
    def use_in_template(**context) -> None:
        # In templates: {{ var.value.GREETING }} or {{ var.json.SOME_JSON_VAR.field }}.
        # Demonstrating programmatic access here as a fallback.
        print(f"Templated greeting would render via Jinja: {{ var.value.GREETING }}")

    read_variables() >> use_in_template()


vars_pipeline()
