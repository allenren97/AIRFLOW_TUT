"""Module 03.03 — TaskGroup (and ``@task_group`` decorator).

GOAL
----
``TaskGroup`` is a UI-only grouping that:
  * Collapses a bundle of related tasks into a single node in the Graph view.
  * Prefixes child task IDs with the group name (``etl.extract``, ...).
  * Replaced the now-deprecated ``SubDagOperator`` — always prefer TaskGroup.

Two equivalent styles:
  * Context manager — ``with TaskGroup("etl") as etl:``.
  * Decorator       — ``@task_group``.

Setting dependencies on the GROUP automatically applies them to every
task inside the group.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


@dag(
    dag_id="mod_03_03_task_groups",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-03", "task-group"],
)
def task_groups_pipeline():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def extract_users() -> list[int]:
        return [1, 2, 3]

    @task
    def extract_orders() -> list[int]:
        return [10, 20, 30]

    @task
    def transform_users(users: list[int]) -> int:
        return len(users)

    @task
    def transform_orders(orders: list[int]) -> int:
        return sum(orders)

    @task
    def load_summary(users: int, orders: int) -> None:
        print(f"users={users} orders={orders}")

    # --- Style 1: context manager -------------------------------------------------
    with TaskGroup("extract", tooltip="Pull raw data from source systems") as extract_group:
        users = extract_users()
        orders = extract_orders()

    # --- Style 2: decorator ------------------------------------------------------
    @task_group(group_id="transform")
    def transform_group(users_data: list[int], orders_data: list[int]):
        return transform_users(users_data), transform_orders(orders_data)

    user_count, order_total = transform_group(users, orders)

    start >> extract_group >> [user_count, order_total] >> load_summary(user_count, order_total) >> end


task_groups_pipeline()
