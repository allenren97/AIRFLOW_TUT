"""Module 03.01 — Setting dependencies with ``>>`` and ``<<``.

GOAL
----
Four equivalent ways to say "A runs before B":

  * ``a >> b``                  (most common; reads left-to-right)
  * ``b << a``                  (right-to-left, occasionally clearer)
  * ``a.set_downstream(b)``     (verbose, programmatic)
  * ``b.set_upstream(a)``       (verbose, programmatic)

You can also chain lists:
  * ``a >> [b, c] >> d``        (a runs first, then b and c in parallel, then d).
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="mod_03_01_bitshift_deps",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-03", "dependencies"],
) as dag:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    d = EmptyOperator(task_id="d")
    e = EmptyOperator(task_id="e")

    a >> [b, c] >> d
    d.set_downstream(e)
