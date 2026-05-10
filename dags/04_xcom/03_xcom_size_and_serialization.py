"""Module 04.03 — XCom size, serialization, and large-payload pattern.

GOAL
----
Airflow serializes XCom values with JSON by default. That means:

  * ``datetime``, ``set``, custom classes — must be converted (e.g. ``isoformat()``).
  * ``pandas.DataFrame``, files, big lists — DO NOT push to XCom directly.

PATTERN: stash the heavy thing in object storage (S3/GCS/local FS) and
push the URI / key through XCom.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_04_03_xcom_large_payload",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-04", "xcom", "best-practice"],
)
def big_payload_pipeline():
    @task
    def produce_big_payload() -> str:
        """Pretend this is a 100MB dataframe; we save it to disk and pass the path."""
        rows = [{"i": i, "x": i * i} for i in range(50_000)]
        path = Path(tempfile.gettempdir()) / "mod_04_03_big_payload.json"
        path.write_text(json.dumps(rows))
        return str(path)  # The XCom payload is just the path string.

    @task
    def consume_big_payload(path: str) -> int:
        rows = json.loads(Path(path).read_text())
        return len(rows)

    @task
    def report(count: int) -> None:
        print(f"Processed {count} rows from upstream blob.")

    report(consume_big_payload(produce_big_payload()))


big_payload_pipeline()
