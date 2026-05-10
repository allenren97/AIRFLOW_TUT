"""Shared helpers reused across multiple example DAGs.

Putting code here keeps DAG files focused on Airflow concepts instead of
boilerplate. In a real project, ``include/`` is mounted into the Airflow
containers and is on ``sys.path`` (we set it via ``AIRFLOW__CORE__DAGS_FOLDER``
or by adding it to ``PYTHONPATH``).
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum

LOG = logging.getLogger(__name__)


DEFAULT_TZ = pendulum.timezone("UTC")


def default_args(owner: str = "learner") -> dict[str, Any]:
    """Reasonable defaults for tutorial DAGs.

    Real production DAGs should choose values intentionally rather than
    blindly copying defaults.
    """
    return {
        "owner": owner,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "email_on_failure": False,
        "email_on_retry": False,
    }


def log_context(context: dict[str, Any]) -> None:
    """Log a small subset of the Airflow task context.

    The ``context`` dict has dozens of keys; printing the whole thing is
    noisy. This helper picks the values you actually look at while learning.
    """
    interesting = {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task"].task_id,
        "run_id": context["run_id"],
        "logical_date": str(context["logical_date"]),
        "data_interval_start": str(context["data_interval_start"]),
        "data_interval_end": str(context["data_interval_end"]),
        "try_number": context["task_instance"].try_number,
    }
    LOG.info("Airflow context: %s", interesting)
