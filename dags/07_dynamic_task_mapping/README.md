# Module 07 — Dynamic Task Mapping

> **Goal**: produce N parallel task instances at runtime when N is only known after another task runs.

---

## 1. The problem it solves

Classic Airflow: the DAG graph is static — the number of tasks is decided at parse time. So you couldn't naturally express "for each file in S3 (count unknown), process it". You had to either: hardcode a fixed fan-out, or generate one DAG per file, or hack with `SubDagOperator`.

**Dynamic Task Mapping** (Airflow 2.3+) fixes this. You write a single mapped task; Airflow expands it into N task instances at runtime, one per element of an iterable.

---

## 2. The four primitives

### 2.1 `.expand(arg=iterable)` — vary one kwarg

```python
@task
def list_files() -> list[str]:
    return ["a.csv", "b.csv", "c.csv"]

@task
def process(filename: str) -> int:
    return len(filename)

process.expand(filename=list_files())
```

The upstream returns a list; Airflow creates one mapped instance of `process` per element. Each instance only sees ONE element.

When you have multiple `.expand` kwargs, you get the **cartesian product**:

```python
process.expand(region=["us", "eu"], table=["orders", "customers"])
# Creates 2 * 2 = 4 task instances
```

### 2.2 `.expand_kwargs(list_of_dicts)` — pair kwargs element-wise

When you don't want the cartesian product:

```python
process.expand_kwargs([
    {"region": "us", "table": "orders"},
    {"region": "us", "table": "customers"},
    {"region": "eu", "table": "orders"},
])
# Creates exactly 3 task instances
```

The list of dicts is usually produced by an upstream `@task` that builds the work plan.

### 2.3 `.partial(constant_kwarg=value)` — constants across instances

`.partial` provides values that are the SAME for every mapped instance, while `.expand` provides the values that VARY:

```python
download.partial(bucket="my-bucket").expand(filename=list_files())
# Every instance gets bucket="my-bucket"; filename varies.
```

### 2.4 Chaining mapped tasks

The output of an `.expand()` is itself an iterable of XComArgs you can feed into another `.expand()`:

```python
uris = download.partial(bucket="b").expand(filename=files)
lengths = parse.expand(uri=uris)         # one parse per download
```

---

## 3. Mapping a TaskGroup

Most powerful pattern: map an entire **TaskGroup** so each iteration runs a multi-step pipeline:

```python
@task_group(group_id="process_one_job")
def process_one_job(region: str, table: str):
    @task
    def stage(region, table): ...
    @task
    def commit(staged): ...
    commit(stage(region, table))

process_one_job.expand_kwargs(list_jobs())
```

Airflow expands the whole group, including internal dependencies, once per item. In the UI you see one collapsed group node with N copies inside.

---

## 4. Reductions — gather mapped outputs

When a downstream task takes a NON-mapped argument that came from a mapped task, Airflow gathers the mapped instances into a list automatically:

```python
sizes = process.expand(filename=list_files())  # mapped: many instances

@task
def total(sizes: list[int]) -> int:
    return sum(sizes)

total(sizes)         # receives [size_for_a, size_for_b, size_for_c]
```

This is map-reduce in 4 lines.

---

## 5. Limits

- A task can map up to `core.max_map_length` instances (default **1024**). Above that you'll get a parse-time error. Bump it via `AIRFLOW__CORE__MAX_MAP_LENGTH` if you really need more.
- The iterable must be JSON-serializable (it's an XCom).
- You cannot use both `.expand` and `.expand_kwargs` on the same task. Pick one.
- You can't map a `BranchPythonOperator` (mapping a task that returns task_ids would be ambiguous).

---

## 6. Common gotchas

- **`.expand(x=upstream())` where `upstream` is NOT iterable** → DAG fails at runtime when the mapping resolves. Make sure the upstream truly returns a list.
- **Empty iterable** → 0 mapped instances, downstream gets an empty list, which often skips downstream because there's nothing to wait on. Handle the empty case explicitly with `@task.short_circuit`.
- **Cartesian explosion**: `.expand(a=range(50), b=range(50))` = 2500 task instances. Watch the multiplication.
- **Each mapped instance is independent**, so a partial failure leaves N-k tasks marked successful — your downstream reduction sees only the successful results unless you set `trigger_rule="all_done"` on the reduction.
- **You can't ask for `task_instance_N`'s log from outside the DAG run easily**. Mapped tasks have a `map_index` (0..N-1) — use it in your prints to track which instance is which.

---

## 7. Practice files

1. **`01_simple_expand.py`** — single-kwarg `.expand`, then a reduction with `total(sizes)`.
2. **`02_expand_kwargs.py`** — list-of-dicts pattern; element-wise pairing.
3. **`03_partial_and_chained.py`** — constants via `.partial`, plus chaining one mapping into another.
4. **`04_zip_and_mapped_groups.py`** — map a whole `@task_group` so each job runs a stage→commit mini-pipeline.

Exercise: take `01_simple_expand.py`. Inside `process`, log `context["task_instance"].map_index`. Run the DAG and confirm each mapped instance sees a different map_index.

---

## 8. Self-check

1. What's the difference between `.expand(a=[...], b=[...])` and `.expand_kwargs([{"a":..,"b":..}, ...])`?
2. What does `.partial` do?
3. How do mapped task results reach a downstream non-mapped task?
4. What's the default cap on the number of mapped instances?
5. Why do you sometimes need `trigger_rule="all_done"` on the reduction step after a mapped task?
