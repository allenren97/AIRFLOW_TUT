# Module 09 — Advanced topics

> **Goal**: a tour of the production-grade knobs you'll touch once your DAGs leave the laptop.

The six topics here are loosely related — pick the lesson section relevant to whatever piece of your DAG isn't behaving.

---

## 1. Jinja templating & macros

Many operator fields are **templated**: Airflow renders Jinja against the runtime context just before execution. This is how you avoid hardcoding logical dates or run ids.

### What's templated?

Each operator declares a class attribute `template_fields`. For example:

- `BashOperator.template_fields = ("bash_command", "env", "cwd")`
- `PythonOperator.template_fields = ("op_args", "op_kwargs", "templates_dict")`
- `SQLExecuteQueryOperator.template_fields = ("sql", "parameters", "conn_id", ...)`

Anything else is rendered as-is.

### Variables you'll use constantly

| Template | Value |
|---|---|
| `{{ ds }}` | logical date as `YYYY-MM-DD` |
| `{{ ds_nodash }}` | same, `YYYYMMDD` |
| `{{ data_interval_start }}` / `{{ data_interval_end }}` | ISO datetimes of the run window |
| `{{ run_id }}` | unique identifier for this DAG run |
| `{{ ti.try_number }}` | 1, 2, 3 across retries |
| `{{ params.foo }}` | a DAG `Param` |
| `{{ var.value.MY_VAR }}` | a string Airflow Variable |
| `{{ var.json.MY_JSON.key }}` | a JSON Airflow Variable, sub-keyed |
| `{{ macros.ds_add(ds, 7) }}` | built-in macro library — date math, etc. |
| `{{ ti.xcom_pull(task_ids="other") }}` | XCom from another task |

### How rendering happens

Templates are rendered by the **scheduler** at the moment the task is queued. So a `xcom_pull` template runs at queue time, not parse time — meaning the upstream task's value is already there.

### Custom Jinja filters / globals

Pass them via `user_defined_macros={...}` and `user_defined_filters={...}` on the DAG. Useful for project-specific helpers.

---

## 2. `Param` and the UI form

`Param(...)` declares typed runtime inputs. They show up as a **real form** when the user clicks "Trigger DAG w/ config".

```python
params={
    "environment": Param("dev", type="string", enum=["dev", "staging", "prod"]),
    "batch_size":  Param(100, type="integer", minimum=1, maximum=10000),
    "dry_run":     Param(True, type="boolean"),
    "tags":        Param(["nightly"], type="array", items={"type": "string"}),
}
```

Read inside a task as `context["params"]["environment"]` or in templates as `{{ params.environment }}`.

Use `enum=`, `minimum=`, `maximum=`, `pattern=` (regex) to validate on submission. The UI rejects bad input before the run is created — much nicer than crashing inside the task.

---

## 3. Pools, priorities, concurrency

Three levels of throttling, layered:

| Knob | Scope | Limit |
|---|---|---|
| `max_active_runs` | per DAG | concurrent DAG runs |
| `max_active_tasks` | per DAG | concurrent task instances within a run |
| `pool` (+ `pool_slots`) | global, across all DAGs | concurrent tasks against a shared resource |
| `max_active_tis_per_dag` | per task | mapped instances across DAG runs |

### Pools

A *pool* is a named bucket of N slots. You create them via Admin → Pools (or `airflow pools set api_pool 3 ""`). A task with `pool="api_pool"` consumes one slot while running. When the pool is full, additional tasks queue.

Use pools for:
- Rate-limiting calls to a flaky external API (`pool_slots=2`).
- Protecting a slow database from being overwhelmed by parallel writes.
- Ensuring exactly-one execution of a critical task across DAGs.

### Priority

When multiple tasks compete for the same slot, the **higher `priority_weight`** wins. The default rule sums weights along the DAG path (`weight_rule="downstream"`); set `weight_rule="absolute"` if you want the integer you typed to be the literal priority.

---

## 4. Retries, callbacks, and (deprecated) SLAs

### Retries

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}
```

Exponential backoff means the gap between retries doubles each time, capped at `max_retry_delay`. Almost always what you want for transient external errors.

To force a task to fail **without** retrying, raise `AirflowFailException` instead of a regular exception.

### Callbacks

Hooks that fire at lifecycle moments. They receive the task context dict.

| Callback | Fires when |
|---|---|
| `on_success_callback` | task / DAG succeeded |
| `on_failure_callback` | task / DAG failed (after final retry) |
| `on_retry_callback` | between retries |
| `on_skipped_callback` | task got skipped |
| `sla_miss_callback` (DAG only) | an SLA was breached |

You can pass a single callable or a list. Use them for Slack alerts, PagerDuty, Datadog events, custom cleanup.

### SLAs (note the deprecation)

Classic SLA support (`sla=timedelta(...)` on a task) is **deprecated** in 2.10 in favor of the upcoming "Deadline" feature in Airflow 3. For 2.10:

- You can still use `sla=` and `sla_miss_callback=`.
- Production-friendly alternative: do the breach detection yourself in `on_failure_callback` / `on_success_callback` based on `data_interval_end` and "now".

---

## 5. Custom Timetables

Cron can't express "trading days only", "skip US holidays", or "every 4 business hours". A custom `Timetable` can.

A timetable implements two methods:

```python
class MyTimetable(Timetable):
    def infer_manual_data_interval(self, *, run_after) -> DataInterval:
        ...   # what's the data interval when the user clicks Trigger?

    def next_dagrun_info(self, *, last_automated_data_interval, restriction) -> DagRunInfo | None:
        ...   # what's the next interval to schedule?
```

You then **register** the class via an `AirflowPlugin`:

```python
class MyTimetablePlugin(AirflowPlugin):
    name = "my_timetable_plugin"
    timetables = [MyTimetable]
```

Why a plugin? So Airflow's serializer can re-instantiate your timetable across scheduler restarts.

Use a timetable when cron + `start_date` aren't enough. Keep them simple — bugs in `next_dagrun_info` can wedge the scheduler.

---

## 6. Variables & Connections

| Concept | What it stores | Where you set it |
|---|---|---|
| `Variable` | any string or JSON value | Admin → Variables, or `AIRFLOW_VAR_<KEY>=...` env var |
| `Connection` | host + creds + extra metadata for an external system | Admin → Connections, or `AIRFLOW_CONN_<ID>=<uri>` env var |

Read variables programmatically:

```python
greeting = Variable.get("GREETING", default_var="hello")     # always set a default
config = Variable.get("FEATURE_FLAGS", default_var={}, deserialize_json=True)
```

Read in templates as `{{ var.value.GREETING }}` or `{{ var.json.FEATURE_FLAGS.foo }}`.

> ⚠️ Reading a `Variable.get` at the top level of a DAG file makes a DB query on **every parse cycle** (every few seconds). Always do it inside a task, or use the templated form.

`Connection`s are usually consumed indirectly — through Hooks (`PostgresHook(postgres_conn_id="postgres_default")`) and operators (`HttpOperator(http_conn_id="...")`).

---

## 7. Common gotchas

- **Top-level `Variable.get`** → bad scheduler performance. Move it into the task body.
- **`Param` enum mismatch** → trigger UI silently rejects values not in the enum; users get confused. Document the expected values.
- **Pool not created in the UI** → tasks that reference a missing pool get stuck in `scheduled` forever. Create the pool first.
- **Custom timetable not registered as a plugin** → DAG fails serialization on scheduler restart.
- **Callback errors swallowed silently** → if your `on_failure_callback` itself raises, you'll find a small log line but no alert. Test your callbacks.

---

## 8. Practice files

1. **`01_jinja_templating.py`** — see all the common context variables in a templated `bash_command`.
2. **`02_params_ui_form.py`** — typed `Param`s with enum, bounds, and arrays. Click "Trigger DAG w/ config" to see the form.
3. **`03_pools_and_priority.py`** — uses `pool="api_pool"`. Create the pool first (Admin → Pools, slots=3).
4. **`04_callbacks_and_retries.py`** — exponential backoff + DAG-level success/failure callbacks. Watch the retry intervals grow.
5. **`05_custom_timetable.py`** — custom timetable from `plugins/custom_timetable_plugin.py`.
6. **`06_variables_and_connections.py`** — reading a Variable defensively with a default.

Exercise: in `04_callbacks_and_retries.py`, replace `AirflowException` with `AirflowFailException`. Confirm the task fails immediately without retrying.

---

## 9. Self-check

1. What's the difference between `params` (DAG-level) and `op_kwargs` (task-level)?
2. Why is reading a `Variable` at module scope a problem?
3. What does `weight_rule="absolute"` change about priority?
4. What's the practical replacement for classic SLAs in Airflow 2.10?
5. Why must a custom timetable be registered through an `AirflowPlugin`?
