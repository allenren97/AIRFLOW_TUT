# Module 06 — Sensors

> **Goal**: wait for an external condition (a file, a record, another DAG) without blocking your whole worker fleet.

---

## 1. What is a sensor?

A sensor is just a special operator whose job is to **wait until a condition is true**. It implements `poke(context) -> bool`:

- `poke` returns `False` → wait `poke_interval` seconds, try again.
- `poke` returns `True`  → task succeeds, downstream tasks run.
- Total wait exceeds `timeout` → task fails (or skips, with `soft_fail=True`).

That's it. Everything else is plumbing around `poke`.

---

## 2. The three execution modes

This is the most important sensor concept. Same sensor logic, three radically different cost profiles.

### 2.1 `mode="poke"` (default)

Holds a **worker slot** for the entire wait. While `poke` is waiting between checks, the worker process is idle but reserved. Cheap to set up, but **a single waiting sensor permanently consumes one worker slot**. With 16 workers and 16 sensors waiting, you can't run any other task.

✅ Use for short waits (seconds → minutes).

### 2.2 `mode="reschedule"`

Releases the worker slot between pokes. Each poke re-queues the task on the scheduler. Negligible memory cost while waiting — you can have hundreds of these.

✅ Use for long waits (minutes → hours).
⚠️ Each "wake-up" costs a scheduler heartbeat; don't pair with `poke_interval=1`.

### 2.3 Deferrable / async

The sensor hands its work off to the **triggerer**, a separate Airflow process that runs asyncio coroutines. One triggerer can manage thousands of waiting tasks on tens of MBs of RAM.

```python
TimeDeltaSensorAsync(task_id="wait", delta=timedelta(hours=2))
S3KeySensor(..., deferrable=True)
HttpSensor(..., deferrable=True)
```

Most provider sensors expose `deferrable=True`. Use whenever you can — it's strictly better than `reschedule`. Requires the triggerer process to be running (the docker-compose in this repo does run it).

---

## 3. Common parameters

Every sensor accepts these (defined in `BaseSensorOperator`):

| Parameter | Meaning |
|---|---|
| `poke_interval` | Seconds between pokes (default 60). |
| `timeout` | Total wait limit in seconds (default 7 days). |
| `mode` | `"poke"` / `"reschedule"`. |
| `soft_fail` | If `True`, mark task as **skipped** on timeout instead of failed. |
| `exponential_backoff` | Increase the gap between pokes over time. |
| `silent_fail` | If `True`, exceptions raised by `poke` don't fail the task — keep retrying. |

---

## 4. The sensors you'll meet most often

| Sensor | What it waits for |
|---|---|
| `FileSensor` | A file/glob to exist on a filesystem connection. |
| `ExternalTaskSensor` | A task (or DAG) in another DAG to reach a target state. |
| `S3KeySensor` / `GCSObjectExistenceSensor` | Cloud object stores. |
| `HttpSensor` | An HTTP endpoint to return a particular status / body. |
| `SqlSensor` | A SQL query to return rows / a truthy value. |
| `TimeDeltaSensor` / `DateTimeSensor` | A clock-time threshold. |
| `@task.sensor` | Your own custom poke logic in a Python function. |

---

## 5. `@task.sensor` — TaskFlow style custom sensor

Decorate a Python function as a sensor. Return a bool, or a `PokeReturnValue` to push XCom too:

```python
@task.sensor(poke_interval=5, timeout=120, mode="reschedule")
def wait_for_record() -> PokeReturnValue:
    record = check_db()
    if record:
        return PokeReturnValue(is_done=True, xcom_value=record)
    return PokeReturnValue(is_done=False)
```

`PokeReturnValue.xcom_value` lets the sensor pass downstream the data it discovered, so you don't need a separate fetch task.

---

## 6. `ExternalTaskSensor` — wait on another DAG

Useful but a little fiddly. Key parameters:

- `external_dag_id`, `external_task_id` — what to wait for. Set `external_task_id=None` to wait for the entire DAG.
- `allowed_states` / `failed_states` — what counts as "done".
- `execution_delta` (timedelta) **or** `execution_date_fn` (callable) — align logical dates if the two DAGs run on different schedules.
- `mode="reschedule"` — almost always; you might wait hours.

> 💡 Modern alternative: use **Datasets** (module 08). The producer DAG declares an outlet; the consumer subscribes by `schedule=Dataset(...)`. You skip all the date-alignment headaches.

---

## 7. Common gotchas

- **`mode="poke"` for hour-long waits** quietly DOSes your worker fleet.
- **`timeout` defaults to 7 days.** A bug that makes `poke` always return False will leave the task pending for a week. Always set a sensible timeout.
- **`ExternalTaskSensor` with mismatched schedules** waits forever because the logical dates don't line up. Use `execution_delta` to map between them.
- **Deferrable sensors with no triggerer** stay deferred forever. Check that the triggerer process is running.
- **`poke_interval=0`** is invalid; use a positive number.

---

## 8. Practice files

1. **`01_file_sensor.py`** — wait for `include/heartbeat.txt` in `mode="reschedule"`. Run the DAG, then `touch` the file from your shell to satisfy it.
2. **`02_external_task_sensor.py`** — wait for `mod_01_05_default_args_and_catchup.echo_interval` of the same logical date.
3. **`03_python_sensor_decorator.py`** — `@task.sensor` returning `PokeReturnValue` so the next task receives the discovered payload.
4. **`04_deferrable_sensor.py`** — `TimeDeltaSensorAsync` — watch the task go to *deferred* state in the UI (greyish color), then resume.

Exercise: change `01_file_sensor.py` to use `soft_fail=True` and a 30-second timeout. Run it, don't create the file, and confirm the task ends up SKIPPED instead of FAILED.

---

## 9. Self-check

1. What's the practical difference between `mode="poke"` and `mode="reschedule"`?
2. When should you prefer a deferrable sensor over `mode="reschedule"`?
3. What's the default `timeout`, and why does that matter?
4. What does `soft_fail=True` do?
5. Why does `ExternalTaskSensor` need `execution_delta` when DAGs run on different schedules?
