# Module 01 — Airflow Basics

> **Goal**: understand what a DAG is, what a task is, and how scheduling actually works in Airflow 2.10.

---

## 1. What is Airflow, in one paragraph?

Airflow is a **workflow orchestrator**. You write Python code that describes:

- **what** work to do (tasks),
- **in what order** (dependencies),
- **when** to run it (schedule).

Airflow's *scheduler* watches the clock, decides when each "run" of your workflow should happen, and asks *workers* to actually execute the tasks.

---

## 2. The three things you must internalize

### 2.1 DAG — Directed Acyclic Graph

A `DAG` is one Python object that bundles together all of:

- a unique `dag_id`,
- a `schedule` (when it should run),
- a `start_date` (the first datetime Airflow is allowed to schedule from),
- a graph of tasks (no cycles allowed — that's the "acyclic" part).

```python
with DAG(
    dag_id="mod_01_01_hello_world",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    ...
```

### 2.2 Task — one unit of work

A task is an instance of an `Operator`. The operator class decides *what kind* of work happens; the task is *that operator with specific arguments*.

```python
say_hello = BashOperator(task_id="say_hello", bash_command="echo hi")
```

There are hundreds of operators (`BashOperator`, `PythonOperator`, `PostgresOperator`, `HttpOperator`, `EmailOperator`, …). The TaskFlow `@task` decorator (module 02) is a shortcut that turns a Python function into a `PythonOperator` for you.

### 2.3 Dependency — `>>`

You connect tasks with the bitshift operator:

```python
start >> say_hello >> end
```

Read it as "start runs, then say_hello, then end". You can also do `[a, b] >> c` (fan-in) and `a >> [b, c]` (fan-out).

---

## 3. Scheduling — the part that confuses everyone

### 3.1 The four ways to set `schedule=`

| Form | Example | Meaning |
|---|---|---|
| `None` | `schedule=None` | Manual triggers only |
| Preset | `schedule="@daily"` | `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`, `@once` |
| Cron | `schedule="30 14 * * 1-5"` | Standard 5-field cron |
| `timedelta` | `schedule=timedelta(minutes=15)` | Relative interval |

### 3.2 `start_date` and the "data interval" mental model

Airflow's mental model is **batch processing of closed time intervals**.

A `@daily` DAG with `start_date=2025-01-01` runs:

- **Run #1** covers the interval `[2025-01-01 00:00, 2025-01-02 00:00)` → it actually executes at the **end** of that interval, i.e. midnight on `2025-01-02`.
- **Run #2** covers `[2025-01-02 00:00, 2025-01-03 00:00)` → executes at midnight on `2025-01-03`.
- And so on.

This is why the `logical_date` of a run looks like it's "yesterday". It is — Airflow waits for the period to be complete before processing it. If you're computing yesterday's sales, this is exactly what you want.

### 3.3 `catchup`

When you toggle a DAG on:

- `catchup=True` → Airflow schedules a run for **every interval** between `start_date` and now.
- `catchup=False` → Airflow schedules only **the most recent** missed interval.

> ⚠️ `catchup=True` + a `start_date` from a year ago = hundreds of queued runs the moment you unpause. Pair with `max_active_runs=1` and idempotent tasks.

---

## 4. `default_args` — settings that cascade

Anything you put in `default_args` is applied to **every task** in the DAG (unless the task overrides it).

```python
default_args = {
    "owner": "learner",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}
```

The most common keys: `retries`, `retry_delay`, `execution_timeout`, `email_on_failure`, `depends_on_past`, `on_failure_callback`.

---

## 5. The runtime context

When a Python task runs, Airflow injects a big dict called the **task context**:

```python
def my_func(**context):
    print(context["ds"])               # logical date as YYYY-MM-DD
    print(context["run_id"])           # unique run identifier
    print(context["data_interval_start"], context["data_interval_end"])
    print(context["task_instance"].try_number)
```

The same values are available in Jinja templates inside operator fields: `{{ ds }}`, `{{ run_id }}`, `{{ ti.try_number }}` (more in module 09).

---

## 6. `Param` — typed inputs from the UI

`Param(...)` makes a typed parameter that appears as a real form field in the "Trigger DAG w/ config" dialog. Beats stringly-typed `conf={...}`.

```python
params={
    "iterations": Param(3, type="integer", minimum=1, maximum=10),
}
```

Read it inside a task as `context["params"]["iterations"]`, or in templates as `{{ params.iterations }}`.

---

## 7. Common gotchas

- The DAG file is **parsed every few seconds** by the scheduler. **Do not** put slow code (HTTP calls, DB queries) at module level — only inside tasks.
- A DAG won't appear in the UI if there's an import error. Watch the **DAGs → Import errors** page.
- `dag_id` must be **globally unique** in the deployment.
- If a run is queued forever, check `max_active_runs`, `pool` slots, and that the DAG is actually unpaused.

---

## 8. Practice — work through these files in order

1. **`01_hello_world_dag.py`** — toggle it on, trigger manually, click into `say_hello` → Logs.
2. **`02_python_operator_dag.py`** — see how `op_kwargs` and `**context` work; check XCom value pushed by `summarize`.
3. **`03_dag_parameters.py`** — open "Trigger DAG w/ config", change `iterations`, watch the bash loop.
4. **`04_scheduling_examples.py`** — four DAGs, four schedule styles. Compare the next-run times in the UI.
5. **`05_default_args_and_catchup.py`** — bounded backfill (`end_date=2025-06-05`); when you unpause it you'll see exactly 5 runs queue.

---

## 9. Self-check

You're ready for module 02 when you can answer these without looking:

1. What's the difference between `start_date` and the time a run actually executes?
2. What does `catchup=False` do?
3. Where does `default_args` apply?
4. How do you read a DAG `Param` from inside a task body? From a Jinja template?
5. Why should you never put a network call at the top level of a DAG file?
