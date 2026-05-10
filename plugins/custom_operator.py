"""Custom Operator that uses GreetingHook.

Anatomy of a custom Operator:
  * Subclass ``BaseOperator``.
  * List templated fields in ``template_fields``.
  * Optional UI color: ``ui_color`` / ``ui_fgcolor``.
  * Implement ``execute(self, context)``; whatever it returns is XCom-pushed.
"""

from __future__ import annotations

from typing import Any

from airflow.models.baseoperator import BaseOperator

from custom_hook import GreetingHook  # type: ignore[import-not-found]


class GreetOperator(BaseOperator):
    """Greets ``name`` using a GreetingHook connection."""

    template_fields = ("name",)
    ui_color = "#a4d8e0"

    def __init__(self, *, name: str, greeting_conn_id: str = "greeting_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.greeting_conn_id = greeting_conn_id

    def execute(self, context: dict[str, Any]) -> str:
        hook = GreetingHook(greeting_conn_id=self.greeting_conn_id)
        message = hook.greet(self.name)
        self.log.info(message)
        return message
