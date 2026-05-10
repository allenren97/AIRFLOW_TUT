"""Unit-test the *plain Python* logic inside TaskFlow tasks.

The goal is to test the underlying functions WITHOUT spinning up the
Airflow scheduler/worker. Two patterns:

  * If your TaskFlow task is decorated with ``@task``, you can call its
    underlying function via ``my_task.function`` (or
    ``my_task.python_callable`` for classic operators).
  * Even better: keep the heavy logic in plain helpers in ``include/``
    and import those directly into your tests.
"""

from __future__ import annotations


def test_user_transform_filters_invalid_rows() -> None:
    """Smoke-test the transform from module 11.03 by importing the function.

    We can't easily reach into the @task body without parsing the DAG, so
    instead we replicate the small transform logic here. In a real project
    you'd extract that function into ``include/etl.py`` and import it here.
    """
    raw = [
        {"id": 1, "name": "Alice", "email": "ALICE@x.com", "company": {"name": "ACME"}},
        {"id": 2, "name": "", "email": "bob@x.com"},  # filtered: blank name
        {"id": 3, "name": "Carol", "email": None},  # filtered: missing email
        {"id": 4, "name": "Dan", "email": "dan@x.com", "company": {}},
    ]

    cleaned = [
        (
            int(u["id"]),
            str(u["name"]),
            str(u["email"]).lower(),
            str(u.get("company", {}).get("name", "")),
        )
        for u in raw
        if u.get("email") and u.get("name")
    ]

    assert cleaned == [
        (1, "Alice", "alice@x.com", "ACME"),
        (4, "Dan", "dan@x.com", ""),
    ]
