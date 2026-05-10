"""Module 09.02 — DAG ``Params`` with a typed UI form.

GOAL
----
``Param`` defines typed, validated runtime inputs that show up as a form
when the user clicks "Trigger DAG w/ config". This replaces ad-hoc
``conf={"foo": "bar"}`` JSON with a real schema.

Useful types: ``string``, ``integer``, ``number``, ``boolean``, ``array``,
plus enum-like ``enum=[...]`` and bounds (``minimum=``, ``maximum=``).
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="mod_09_02_params_ui_form",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "params"],
    params={
        "environment": Param(
            "dev",
            type="string",
            enum=["dev", "staging", "prod"],
            title="Environment",
            description="Which environment to run against.",
        ),
        "batch_size": Param(
            100,
            type="integer",
            minimum=1,
            maximum=10_000,
            title="Batch size",
        ),
        "dry_run": Param(
            True,
            type="boolean",
            title="Dry run?",
        ),
        "tags": Param(
            ["nightly"],
            type="array",
            items={"type": "string"},
            title="Tags",
        ),
    },
)
def params_ui_pipeline():
    @task
    def show(**context) -> None:
        p = context["params"]
        print(
            f"environment={p['environment']!r} "
            f"batch_size={p['batch_size']} "
            f"dry_run={p['dry_run']} "
            f"tags={p['tags']}"
        )

    show()


params_ui_pipeline()
