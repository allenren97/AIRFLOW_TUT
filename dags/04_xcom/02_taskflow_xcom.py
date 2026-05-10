"""Module 04.02 — XCom in TaskFlow style.

GOAL
----
TaskFlow makes XCom invisible:

  * Returning from a ``@task`` pushes the value to XCom.
  * Calling ``other_task(my_task_return)`` pulls it back implicitly and
    sets the dependency edge for you.

Sometimes you still want to push/pull explicitly — for example when a
task needs to read XCom from a NON-direct upstream task. Use the
``ti`` (task_instance) object, available by adding ``**context`` or
specific kwargs to the function signature.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_04_02_taskflow_xcom",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-04", "xcom", "taskflow"],
)
def xcom_taskflow_pipeline():
    @task
    def fetch_user_id() -> int:
        return 42

    @task
    def fetch_order_count() -> int:
        return 17

    @task
    def report(user_id: int, orders: int) -> None:
        print(f"User {user_id} has {orders} orders.")

    @task
    def lateral_pull(**context) -> None:
        # You CAN reach across the graph by pulling explicitly — useful when
        # the upstream isn't a direct argument. Prefer the implicit style
        # whenever possible; explicit pulls hide the dependency edge.
        ti = context["task_instance"]
        user_id = ti.xcom_pull(task_ids="fetch_user_id")
        print(f"Lateral pull saw user_id={user_id}")

    user_id = fetch_user_id()
    orders = fetch_order_count()
    report(user_id, orders)

    # Make the lateral_pull explicit dependency so it actually runs after fetch_user_id.
    user_id >> lateral_pull()


xcom_taskflow_pipeline()
