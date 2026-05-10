"""Module 09.03 — Pools, priority weights, and concurrency knobs.

GOAL
----
* ``pool`` — A *pool* limits how many tasks can run concurrently across
  the WHOLE Airflow deployment. Useful when many DAGs hit one resource
  (e.g. external API, slow Postgres). Create the pool in the UI:
  Admin → Pools → Add (e.g. ``api_pool`` with 3 slots).
* ``priority_weight`` — When more tasks want to run than slots are
  available, higher-priority tasks run first.
* ``weight_rule`` — How priority is computed across the DAG ("downstream"
  vs "upstream" vs "absolute").
* ``max_active_tasks`` (DAG) and ``max_active_tis_per_dag`` (Task) tune
  concurrency further.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.utils.weight_rule import WeightRule


@dag(
    dag_id="mod_09_03_pools_and_priority",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "concurrency"],
    max_active_tasks=4,
)
def pools_pipeline():
    @task(
        pool="api_pool",
        pool_slots=1,
        priority_weight=10,
        weight_rule=WeightRule.ABSOLUTE,
    )
    def hit_api(endpoint: str) -> str:
        print(f"GET {endpoint}")
        return endpoint

    @task(
        pool="api_pool",
        pool_slots=1,
        priority_weight=1,
        weight_rule=WeightRule.ABSOLUTE,
    )
    def low_priority_cleanup() -> None:
        print("Low priority cleanup")

    targets = ["/users", "/orders", "/products", "/inventory", "/promotions"]
    hit_api.expand(endpoint=targets)
    low_priority_cleanup()


pools_pipeline()
