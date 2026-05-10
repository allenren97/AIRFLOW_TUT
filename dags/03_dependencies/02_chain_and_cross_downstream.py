"""Module 03.02 — ``chain`` and ``cross_downstream`` helpers.

GOAL
----
* ``chain(t1, t2, t3, ...)`` is equivalent to ``t1 >> t2 >> t3``, but
  also handles **lists** correctly — pairing them element-wise instead
  of fan-out/fan-in.
* ``cross_downstream(upstream_list, downstream_list)`` connects EVERY
  upstream task to EVERY downstream task (fan-out + fan-in).

Compare the resulting graphs in the UI to internalize the difference.
"""

from __future__ import annotations

import pendulum

from airflow.models.baseoperator import chain, cross_downstream
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="mod_03_02_chain_helper",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-03", "dependencies"],
) as dag_chain:
    a1, a2 = EmptyOperator(task_id="a1"), EmptyOperator(task_id="a2")
    b1, b2 = EmptyOperator(task_id="b1"), EmptyOperator(task_id="b2")
    c1, c2 = EmptyOperator(task_id="c1"), EmptyOperator(task_id="c2")

    # Pairs lists element-wise: a1->b1->c1 and a2->b2->c2 — NOT a fan-out.
    chain([a1, a2], [b1, b2], [c1, c2])


with DAG(
    dag_id="mod_03_02_cross_downstream",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-03", "dependencies"],
) as dag_cross:
    a1, a2 = EmptyOperator(task_id="a1"), EmptyOperator(task_id="a2")
    b1, b2, b3 = (
        EmptyOperator(task_id="b1"),
        EmptyOperator(task_id="b2"),
        EmptyOperator(task_id="b3"),
    )

    # Connects each task in the first list to every task in the second.
    # i.e. 2 * 3 = 6 edges total.
    cross_downstream([a1, a2], [b1, b2, b3])
