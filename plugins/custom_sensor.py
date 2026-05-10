"""Custom Sensor that polls until a counter reaches a threshold.

Anatomy of a custom Sensor:
  * Subclass ``BaseSensorOperator``.
  * Implement ``poke(self, context) -> bool``.  Return True when the
    waited-for condition is satisfied; False to keep polling.
  * Inherit all the standard sensor knobs (``poke_interval``, ``timeout``,
    ``mode``, ``soft_fail``, ``exponential_backoff``).
"""

from __future__ import annotations

from typing import Any

from airflow.sensors.base import BaseSensorOperator


class CounterSensor(BaseSensorOperator):
    """Waits until an in-memory counter passes ``threshold`` (demo only)."""

    template_fields = ("threshold",)

    def __init__(self, *, threshold: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.threshold = threshold
        self._counter = 0

    def poke(self, context: dict[str, Any]) -> bool:
        self._counter += 1
        self.log.info("CounterSensor poke #%d (threshold=%d)", self._counter, self.threshold)
        return self._counter >= self.threshold
