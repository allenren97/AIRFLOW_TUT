"""Module 07.03 — ``.partial()`` (constants) + chaining mapped tasks.

GOAL
----
* ``.partial(...)`` provides values that are CONSTANT across every
  mapped task instance, while ``.expand(...)`` provides the values that
  vary per instance.
* Mapped tasks compose: the output of an ``.expand()`` is itself an
  iterable that you can feed into another ``.expand()`` to get a
  per-element pipeline.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="mod_07_03_partial_and_chained",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["module-07", "mapping"],
)
def chained_mapping_pipeline():
    @task
    def list_files() -> list[str]:
        return ["a.csv", "b.csv", "c.csv"]

    @task
    def download(filename: str, *, bucket: str) -> str:
        # `bucket` is constant for every mapped instance (came from .partial()).
        # `filename` varies per instance (came from .expand()).
        return f"s3://{bucket}/{filename}"

    @task
    def parse(uri: str) -> int:
        return len(uri)

    @task
    def report(lengths: list[int]) -> None:
        print(f"Parsed {len(lengths)} URIs: {lengths}")

    uris = download.partial(bucket="my-bucket").expand(filename=list_files())
    lengths = parse.expand(uri=uris)  # chain a second mapping over the first's output
    report(lengths)


chained_mapping_pipeline()
