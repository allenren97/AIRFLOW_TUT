# Airflow 2.10 — Cheatsheet

## Essential imports

```python
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.datasets import Dataset, DatasetAlias
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
```

## DAG anatomy (TaskFlow)

```python
@dag(
    dag_id="...",
    schedule=None,                 # None | cron | preset | timedelta | Dataset | Timetable
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["..."],
)
def my_pipeline():
    @task
    def step():
        ...
    step()

my_pipeline()
```

## Schedule values

| Value | Meaning |
| --- | --- |
| `None` | manual only |
| `"@once"` | fire exactly once |
| `"@hourly" / "@daily" / "@weekly" / "@monthly" / "@yearly"` | preset |
| `"30 14 * * 1-5"` | cron — 14:30 weekdays |
| `timedelta(minutes=15)` | every 15 min after the previous run |
| `Dataset("s3://...")` | data-aware scheduling |
| `[ds_a, ds_b]` | trigger when ANY listed dataset is updated |
| `ds_a & ds_b` / `ds_a | ds_b` / `ds_a | (ds_b & ds_c)` | conditional dataset expressions |
| `MyTimetable()` | custom timetable from a plugin |

## Trigger rules (`airflow.utils.trigger_rule.TriggerRule`)

| Rule | Run when |
| --- | --- |
| `all_success` (default) | every upstream succeeded |
| `all_failed` | every upstream failed |
| `all_done` | every upstream is in a terminal state |
| `all_skipped` | every upstream was skipped |
| `one_success` | at least one upstream succeeded |
| `one_failed` | at least one upstream failed |
| `one_done` | at least one upstream is done |
| `none_failed` | no upstream failed (skipped is OK) |
| `none_failed_min_one_success` | no failures + ≥1 success — use after a branch |
| `none_skipped` | no upstream was skipped |
| `always` | unconditional |

## XCom

```python
# Implicit (TaskFlow):
@task
def make() -> int: return 7
@task
def use(x: int): print(x)
use(make())

# Explicit (classic):
ti.xcom_push(key="foo", value=42)
ti.xcom_pull(task_ids="upstream", key="foo")

# Multiple outputs:
@task(multiple_outputs=True)
def stats() -> dict[str, int]: return {"a": 1, "b": 2}
```

## Dynamic Task Mapping

```python
process.expand(filename=list_files())                 # one kwarg, varies
process.expand_kwargs([{"a": 1}, {"a": 2}])           # list of full kwarg dicts
download.partial(bucket="b").expand(filename=files)   # constants + variables
group_task.expand_kwargs(list_of_dicts)               # map a whole TaskGroup
```

## Datasets (data-aware scheduling)

```python
DS = Dataset("s3://bucket/key")

@task(outlets=[DS])                # producer task
def write(): ...

@dag(schedule=DS): ...             # consumer DAG (single dataset)
@dag(schedule=[DS_A, DS_B]): ...   # consumer triggers when ANY listed updates
@dag(schedule=DS_A & DS_B): ...    # AND
@dag(schedule=DS_A | DS_B): ...    # OR
@dag(schedule=DS_A | (DS_B & DS_C)): ...  # arbitrary expression
```

## Dataset Aliases (NEW in 2.10)

```python
ALIAS = DatasetAlias("my-alias")

@task(outlets=[ALIAS])
def producer(*, outlet_events, **ctx):
    outlet_events[ALIAS].add(
        Dataset(f"s3://bucket/run-{ctx['ds']}/data.parquet"),
        extra={"row_count": 100},
    )

@dag(schedule=ALIAS)              # subscribe to the alias, not a fixed URI
def consumer(): ...
```

## Sensors

```python
FileSensor(filepath="...", mode="reschedule", poke_interval=10, timeout=60*30)
ExternalTaskSensor(external_dag_id="...", external_task_id="...", execution_delta=timedelta(0))

@task.sensor(poke_interval=5, timeout=120, mode="reschedule")
def waiter() -> PokeReturnValue: ...

# Deferrable (uses triggerer process):
TimeDeltaSensorAsync(delta=timedelta(minutes=10))
S3KeySensor(..., deferrable=True)
```

## Branching

```python
@task.branch
def pick() -> str | list[str]:    # return downstream task_id(s) to follow
    return "high_priority" if ... else "low_priority"

@task.short_circuit               # return falsy -> skip everything downstream
def precondition() -> bool: ...
```

## Templating

```jinja
{{ ds }} {{ ds_nodash }} {{ data_interval_start }} {{ data_interval_end }}
{{ run_id }} {{ ti.try_number }} {{ ti.task_id }}
{{ params.my_param }}
{{ var.value.MY_VAR }} {{ var.json.MY_JSON.key }}
{{ macros.ds_add(ds, 7) }} {{ macros.datetime.utcnow() }}
{{ ti.xcom_pull(task_ids="other") }}
```

## Variables / Connections (env-var seeding)

```bash
AIRFLOW_VAR_GREETING="hello"                      # Variable.get("GREETING")
AIRFLOW_CONN_POSTGRES_DEFAULT="postgres://airflow:airflow@postgres/airflow"
```

## CLI greatest hits

```bash
airflow dags list
airflow dags list-import-errors
airflow dags test mod_01_01_hello_world 2025-01-01
airflow tasks test mod_01_01_hello_world say_hello 2025-01-01
airflow dags trigger mod_01_01_hello_world
airflow connections add greeting_default --conn-uri 'greeting://my-host?prefix=Howdy'
airflow variables set GREETING "hello"
```

## Project structure (this repo)

```
airflow/
├── dags/<module>/<NN>_<topic>.py
├── plugins/                 (custom operator/hook/sensor/timetable)
├── include/                 (shared helpers)
├── tests/                   (pytest suite)
└── notes/cheatsheet.md      (this file)
```
