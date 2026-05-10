# Module 04 — XCom (cross-communication)

> **Goal**: pass small amounts of data between tasks safely, and know what NOT to push through XCom.

---

## 1. What is XCom?

XCom = "cross-communication". It's Airflow's small message bus stored in the metadata database.

- One XCom entry = `(dag_id, task_id, run_id, key, value)`.
- Default key is `return_value`.
- Values are JSON-serialized by default (configurable to pickle, S3, custom backends).
- Values are scoped to a single DAG run — task #N's XCom doesn't leak into run #N+1.

You read it back with `xcom_pull(task_ids=..., key=...)` or, in TaskFlow, just by passing the upstream call as an argument.

---

## 2. Three ways to push

### 2.1 Implicit — return from `@task` or `python_callable`

```python
@task
def make() -> int:
    return 42                     # pushed under key "return_value"
```

### 2.2 Explicit — `ti.xcom_push`

```python
def push(**ctx):
    ctx["task_instance"].xcom_push(key="custom_key", value="hi")
```

### 2.3 Operator-specific

Some operators push automatically. `BashOperator` pushes the **last line of stdout**; `HttpOperator` pushes the response body when `do_xcom_push=True`. Read each operator's docs.

---

## 3. Three ways to pull

### 3.1 TaskFlow — pass as argument (preferred)

```python
@task
def use(x: int): ...
use(make())                       # implicit pull + dependency edge
```

### 3.2 Explicit pull inside a task

```python
@task
def lateral(**ctx):
    val = ctx["task_instance"].xcom_pull(task_ids="make", key="return_value")
```

> ⚠️ Explicit pull does **not** create a dependency edge. Add `make() >> lateral()` manually or the lateral task may run before `make` finishes.

### 3.3 Pull inside a Jinja-templated field

```python
BashOperator(
    bash_command='echo {{ ti.xcom_pull(task_ids="make") }}',
)
```

---

## 4. Multiple outputs

A `@task(multiple_outputs=True)` returning a dict pushes one XCom **per key**:

```python
@task(multiple_outputs=True)
def stats() -> dict:
    return {"total": 100, "avg": 50.0}

s = stats()
print_total(s["total"])           # this is a separate XCom, "total"
```

Type-annotating with `dict[str, X]` or a `TypedDict` infers the flag automatically.

---

## 5. Size & serialization rules

- Default XCom backend is the metadata DB (Postgres/SQLite/MySQL). The value column is typically a few MB max.
- **Aim for KB, not MB.**
- JSON-serializable types only (ints, floats, strings, lists, dicts, bools, None). For other types: convert (`datetime.isoformat()`, `set` → `list`, dataclass → `asdict()`).

### Large-payload pattern

When you have something heavy (a parquet file, a 100k-row dataframe), don't push the bytes:

```python
@task
def produce() -> str:
    path = "/tmp/big.parquet"
    df.to_parquet(path)
    return path                    # XCom carries just the path

@task
def consume(path: str):
    df = pd.read_parquet(path)
```

In a real pipeline replace `/tmp` with S3/GCS so workers on other machines can read it.

### Custom XCom backend

For systems where you ALWAYS push big stuff: implement a `BaseXCom` subclass that serializes to S3/GCS and stores only the URI in the DB. Configure with `xcom_backend = my_module.MyS3XCom` in `airflow.cfg`. (Out of scope here — mentioned so you know it's possible.)

---

## 6. Common gotchas

- **Forgetting the dependency** when using explicit `xcom_pull`. Always wire the `>>` too.
- **Passing dataframes / numpy arrays directly.** They're not JSON-serializable; you'll see a serialization error at task end.
- **Name collisions on `multiple_outputs`.** If your dict has a key `return_value`, weird things happen — pick another name.
- **Pulling across DAG runs.** `xcom_pull` defaults to the current run only. To reach another run you need `include_prior_dates=True` and `dag_id=...` — usually a sign you should use a Dataset (module 08) instead.
- **Templating order.** Jinja in operator fields renders BEFORE `execute()` runs, so `xcom_pull` in a template runs at the moment the operator is about to start, not when you build the DAG.

---

## 7. Practice files

1. **`01_classic_push_pull.py`** — implicit return-value vs explicit `xcom_push`/`xcom_pull` with custom keys.
2. **`02_taskflow_xcom.py`** — TaskFlow's implicit XCom + a lateral pull example.
3. **`03_xcom_size_and_serialization.py`** — the write-to-disk-and-pass-the-path pattern.

Exercise: in `02_taskflow_xcom.py`, change `lateral_pull` to be a *direct* downstream of `fetch_user_id` by passing the value as an argument instead of pulling. Notice how the `>>` line becomes unnecessary.

---

## 8. Self-check

1. What's the default XCom key when you `return` from a `@task`?
2. Why does `xcom_pull` not create a scheduling dependency?
3. What's the practical size limit for an XCom value, and what do you do for bigger payloads?
4. How does `multiple_outputs=True` change downstream code?
5. Does an XCom from yesterday's run leak into today's run by default?
