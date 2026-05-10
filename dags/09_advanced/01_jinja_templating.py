"""Module 09.01 — Jinja templating and macros.

GOAL
----
Many operator fields are *templated* — Airflow renders Jinja against the
runtime context just before execution. The most common variables:

  * ``{{ ds }}``                — logical date as ``YYYY-MM-DD``.
  * ``{{ ds_nodash }}``         — same but ``YYYYMMDD``.
  * ``{{ data_interval_start }}`` / ``{{ data_interval_end }}``.
  * ``{{ run_id }}`` / ``{{ ti.try_number }}``.
  * ``{{ params.foo }}``        — DAG params.
  * ``{{ var.value.MY_VAR }}``  — Airflow Variable.
  * ``{{ var.json.MY_JSON.key }}`` — JSON-typed Variable.
  * ``{{ macros.ds_add(ds, 7) }}`` — built-in macros library.

Operators advertise which fields are templated via the
``template_fields`` class attribute. For ``BashOperator`` it is
``bash_command`` and ``env``; for ``PythonOperator`` it is ``op_args``,
``op_kwargs``, and ``templates_dict``.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def show_templates(*, ds: str, params: dict, **_) -> None:
    """The kwargs ``ds`` and ``params`` are RENDERED before this runs."""
    print(f"Got rendered ds={ds} params={params}")


with DAG(
    dag_id="mod_09_01_jinja_templating",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-09", "templating"],
    params={"region": "us-east-1"},
) as dag:
    BashOperator(
        task_id="bash_template",
        bash_command=(
            'echo "ds={{ ds }}, '
            'next_ds={{ macros.ds_add(ds, 1) }}, '
            'try={{ ti.try_number }}, '
            'region={{ params.region }}"'
        ),
    ) >> PythonOperator(
        task_id="python_template",
        python_callable=show_templates,
        op_kwargs={"ds": "{{ ds }}", "params": "{{ params }}"},
    )
