"""A trivial Hook implementation used by the custom operator/sensor.

A Hook is a thin wrapper around an external system that pulls credentials
from an Airflow Connection.  Real hooks usually subclass ``BaseHook`` and
expose a ``get_conn()`` method (and one method per logical operation,
e.g. ``run_query``, ``upload_file``).

To keep this self-contained we don't talk to a real system; we just read
the connection's ``host`` and ``extra``.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow.hooks.base import BaseHook

LOG = logging.getLogger(__name__)


class GreetingHook(BaseHook):
    """Pretend "client" for an imaginary greeting service."""

    conn_name_attr = "greeting_conn_id"
    default_conn_name = "greeting_default"
    conn_type = "greeting"
    hook_name = "Greeting"

    def __init__(self, greeting_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.greeting_conn_id = greeting_conn_id

    def get_conn(self) -> dict[str, Any]:
        conn = self.get_connection(self.greeting_conn_id)
        return {
            "host": conn.host or "localhost",
            "extra": conn.extra_dejson,
        }

    def greet(self, name: str) -> str:
        client = self.get_conn()
        prefix = client["extra"].get("prefix", "Hello")
        return f"{prefix}, {name}! (from {client['host']})"
