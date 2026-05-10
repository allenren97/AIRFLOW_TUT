# Module 02 — TaskFlow API

> **Goal**: write DAGs that look like ordinary Python code. The TaskFlow API is the *recommended* style for new Airflow 2.x DAGs.

---

## 1. Why TaskFlow exists

The classic style needs a lot of ceremony to pass data between tasks:

```python
def extract(**ctx):
    return {"orders": 42}

def transform(**ctx):
    raw = ctx["ti"].xcom_pull(task_ids="extract")
    return raw["orders"] - 5

PythonOperator(task_id="extract", python_callable=extract) >> \
    PythonOperator(task_id="transform", python_callable=transform)
```

You have to: name tasks twice, pull XCom by task id, manage dependencies manually. TaskFlow collapses that:

```python
@task
def extract() -> dict:
    return {"orders": 42}

@task
def transform(payload: dict) -> int:
    return payload["orders"] - 5

transform(extract())
```

That's it. Same DAG. Calling `transform(extract())` does two things automatically:

1. Sets `extract >> transform` as a dependency edge.
2. Sets up XCom so `transform` receives `extract`'s return value.

---

## 2. The two decorators

### 2.1 `@dag`

Wraps a Python function to turn it into a DAG factory.

```python
@dag(
    dag_id="mod_02_01_taskflow_basics",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def my_pipeline():
    ...

my_pipeline()    # IMPORTANT: must call the function for Airflow to find the DAG.
```

That last line trips up everyone once. The decorator returns a *callable*; you have to call it at module level so Airflow's DagBag actually sees a `DAG` object.

### 2.2 `@task`

Turns a function into a task. Calling it inside the DAG body does **not** execute the function — it returns an `XComArg`, a placeholder that resolves at runtime.

```python
@task
def hello(name: str) -> str:
    return f"hi {name}"

x = hello("alice")          # x is an XComArg, NOT a string.
@task
def shout(msg: str): print(msg.upper())
shout(x)                    # automatically wires hello -> shout
```

---

## 3. Multiple outputs

By default, `@task` pushes one return value to XCom under the key `return_value`. To split a dict into separate XComs (so downstream tasks can pick individual keys):

```python
@task(multiple_outputs=True)
def stats() -> dict:
    return {"total": 100, "average": 50.0}

s = stats()
print_total(s["total"])      # works because each key is its own XCom
print_average(s["average"])
```

`multiple_outputs=True` is **inferred automatically** if the function's return type annotation is a `TypedDict` or `dict[str, X]`. Use a `TypedDict` and you can drop the flag.

---

## 4. Mixing TaskFlow with classic operators

You will mix them constantly — TaskFlow for your Python logic, provider operators (`PostgresOperator`, `S3*Operator`, sensors) for the systems-y bits.

```python
@task
def pick_target() -> str:
    return "world"

target = pick_target()

say_hello = BashOperator(
    task_id="say_hello",
    bash_command='echo "Hello, {{ ti.xcom_pull(task_ids=\'pick_target\') }}!"',
)

target >> say_hello
```

Two patterns:

- Set dependencies between TaskFlow XComArgs and classic operators with `>>` — works in either direction.
- Inside a classic operator's templated field, pull an XCom with `{{ ti.xcom_pull(task_ids="...") }}`.

If you need to set non-decorator attributes on a TaskFlow task's underlying operator, reach in via `my_task.operator`.

---

## 5. Common gotchas

- **Forgetting to call the `@dag` function.** No call → no DAG → DAG doesn't appear in UI. Always end the file with `my_pipeline()`.
- **Returning unserializable values from `@task`.** Airflow JSON-serializes XComs by default. `datetime` → `.isoformat()`, `set` → `list`, custom classes → convert to dict.
- **Using `@task` on the wrong thing.** Decorate the function that does work, NOT a wrapper that builds the DAG.
- **Side-effects at parse time.** The body of your `@dag` function runs every time the scheduler parses the file. Anything in there should be cheap (object construction only). Heavy work goes inside `@task` bodies.

---

## 6. Practice files

1. **`01_taskflow_basics.py`** — the canonical extract → transform → load shape.
2. **`02_taskflow_multiple_outputs.py`** — fan a single dict-returning task into separate downstreams.
3. **`03_mixing_classic_and_taskflow.py`** — wire a `@task` to a `BashOperator` via Jinja XCom pull.

Try this exercise: rewrite `dags/01_basics/02_python_operator_dag.py` as a TaskFlow DAG. You should end up with about 60% fewer lines of code.

---

## 7. Self-check

1. What does calling a `@task`-decorated function inside a DAG body return?
2. What does `multiple_outputs=True` change about XCom?
3. Why does the file have to end with `my_pipeline()`?
4. How do you pull a TaskFlow task's return value inside a `BashOperator`?
5. When would you still pick a classic operator instead of `@task`?
