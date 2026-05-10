# Module 08 — Datasets (data-aware scheduling)

> **Goal**: trigger DAGs based on **data** events, not the clock. This is one of the headline features of Airflow 2.10.

---

## 1. The problem

Imagine three pipelines:

- DAG A produces a "users" table at 02:00.
- DAG B produces an "orders" table at 02:30.
- DAG C joins both at 03:00.

If A or B is slow, DAG C runs on stale or missing data. The classic fix is `ExternalTaskSensor` — but you have to maintain it on every dependency, manage logical-date alignment, and the producer DAG has no idea who depends on it.

**Datasets** flip this around. The producer publishes a *dataset event*; any DAG subscribed to that dataset is triggered automatically.

---

## 2. The model in five lines of code

```python
ORDERS = Dataset("s3://lake/orders.parquet")            # the identity

@task(outlets=[ORDERS])                                 # producer
def write_orders(): ...

with DAG("consumer", schedule=ORDERS):                  # consumer
    ...
```

When `write_orders` succeeds, Airflow records a dataset event for `ORDERS`. Every DAG with `schedule=ORDERS` is then queued to run. That's the whole feature.

### 2.1 What's a `Dataset`?

A logical handle for a piece of data. The URI string is just an identifier — Airflow does NOT read the file itself; it just records "task X says this dataset is updated". You can use any URI scheme: `s3://`, `gs://`, `postgres://...`, `file:///...`, even custom strings.

> ⚠️ Two `Dataset(...)` instances with the same URI are equal. Define each dataset once at module scope and import it everywhere — that's how producer and consumer DAGs share identity.

---

## 3. Subscribing — four shapes

```python
schedule=ORDERS                       # one dataset
schedule=[ORDERS, USERS]              # ANY of these (OR)
schedule=ORDERS & USERS               # ALL of these  (AND)
schedule=ORDERS | (USERS & PROMOS)    # arbitrary boolean expression
```

The `&` and `|` operators on `Dataset` build a logical expression that the scheduler evaluates each time one of the referenced datasets receives an event.

This is how you say "trigger when (users AND orders) are both fresh, OR when promotions changes alone":

```python
schedule=(USERS & ORDERS) | PROMOS
```

You don't manage logical-date alignment, polling, or sensor pools. The scheduler does it for you.

---

## 4. Dataset events with `extra` metadata

When a producer publishes, it can attach arbitrary JSON-serializable metadata:

```python
@task(outlets=[ORDERS])
def write(*, outlet_events, **ctx):
    outlet_events[ORDERS].add(ORDERS, extra={"row_count": 1234, "schema": "v3"})
```

Consumers see the events that triggered them via `context["triggering_dataset_events"]`:

```python
@task
def react(**ctx):
    events = ctx["triggering_dataset_events"]            # dict[uri, list[event]]
    for uri, evs in events.items():
        for ev in evs:
            print(uri, ev.extra)
```

This lets you make decisions ("big batch → heavy path; small → light path") without re-querying the source.

---

## 5. `DatasetAlias` — NEW in Airflow 2.10

A `DatasetAlias` is a stable name that resolves to one or more concrete `Dataset` URIs **at runtime**.

### Why?

Your producer often doesn't know the exact URI until the task is running:

```
s3://bucket/year=2025/month=05/day=10/run-abc.parquet
```

You can't put that URI in `outlets=[Dataset(...)]` at parse time. `DatasetAlias` lets you declare the alias up front and resolve the concrete dataset inside the task:

```python
DAILY = DatasetAlias("daily-customer-report")

@task(outlets=[DAILY])
def write(*, outlet_events, **ctx):
    concrete = Dataset(f"s3://bucket/{ctx['ds']}/customers.parquet")
    outlet_events[DAILY].add(concrete, extra={"ds": ctx["ds"]})
```

Consumers can subscribe to either:
- the **alias** (trigger on any concrete dataset under it), or
- a specific concrete dataset (if they happen to know its URI).

### When to use what

| Situation | Use |
|---|---|
| Single, fixed URI known at parse time | `Dataset` |
| URI varies per run (date, run-id, partitions) | `DatasetAlias` |
| Producer wants to fan out events under one logical concept | `DatasetAlias` |

---

## 6. Inspecting datasets in the 2.10 UI

- **Datasets tab** — graph of producer DAGs ↔ datasets ↔ consumer DAGs.
- **Dataset URI page** — list of recent events with their `extra` payload and the run that produced them.
- **Dataset Aliases tab** (new in 2.10) — see all concrete datasets resolved under each alias.

If a consumer "doesn't trigger", look here first: did the event actually fire? Is the URI exactly the same string the consumer subscribes to?

---

## 7. Common gotchas

- **URIs must match EXACTLY** — `s3://bucket/foo` and `s3://bucket/foo/` are different datasets.
- **Producer task must succeed.** A failed task does NOT publish an event, so no consumer triggers.
- **Catchup doesn't apply.** Dataset-driven runs don't backfill historic intervals — they fire when an event arrives.
- **Conditional expressions and time schedules don't mix.** A DAG either has `schedule=` set to a time-based schedule OR a dataset expression, not both.
- **Dataset aliases need at least one concrete dataset added at runtime.** If a producer task succeeds but never calls `outlet_events[ALIAS].add(...)`, no event fires and consumers stay idle.

---

## 8. Practice files

1. **`01_producer_consumer.py`** — minimal end-to-end. Trigger the producer manually, watch the consumer queue itself.
2. **`02_conditional_dataset_scheduling.py`** — three producers, one consumer with `(USERS & ORDERS) | PROMOTIONS`. Trigger producers in different orders to see when the consumer fires.
3. **`03_dataset_aliases.py`** — alias resolved at runtime; the concrete URI changes per `ds`.
4. **`04_dataset_extras.py`** — pass `row_count` in the event metadata; consumer chooses heavy vs light path.

Exercise: change the consumer in `02_conditional_dataset_scheduling.py` to `(USERS | ORDERS) & PROMOTIONS`. Predict which producer combinations now trigger it. Verify in the UI.

---

## 9. Self-check

1. What's a "dataset event" and what publishes one?
2. Write the schedule expression for "any of A, but only when B is also fresh".
3. When would you reach for `DatasetAlias` instead of `Dataset`?
4. How does a consumer task read the metadata that came with a triggering event?
5. Why do two datasets with URIs `s3://b/foo` and `s3://b/foo/` not match each other?
