# Module 03 — Dependencies & Task Groups

> **Goal**: master every way of expressing "task A runs before task B" and learn how to organize big DAGs with `TaskGroup`.

---

## 1. Four equivalent ways to set a dependency

```python
a >> b                   # most common — reads left-to-right
b << a                   # right-to-left, occasionally clearer
a.set_downstream(b)      # programmatic
b.set_upstream(a)        # programmatic
```

All four create exactly the same edge. Pick one style per DAG and stick with it.

---

## 2. Lists short-circuit boring code

```python
a >> [b, c] >> d
```

Means: `a` runs first, then `b` and `c` in parallel, then `d`. The list on either side is treated as a set — every element on the left points to every element on the right.

```python
[a1, a2] >> [b1, b2]
# Equivalent edges: a1->b1, a1->b2, a2->b1, a2->b2
```

That's a fan-out + fan-in (a "cartesian" connection).

---

## 3. `chain` and `cross_downstream` helpers

When the bitshift syntax gets confusing, use the helpers from `airflow.models.baseoperator`.

### 3.1 `chain(...)`

`chain(t1, t2, t3)` is the same as `t1 >> t2 >> t3`. The interesting form is when you mix in lists:

```python
chain([a1, a2], [b1, b2], [c1, c2])
# Pairs them ELEMENT-WISE:  a1 -> b1 -> c1
#                           a2 -> b2 -> c2
```

That's not what `>>` does (it would fan-out). Use `chain` when you specifically want parallel "lanes".

### 3.2 `cross_downstream(upstream_list, downstream_list)`

Connects every task in `upstream_list` to every task in `downstream_list` — the cartesian fan-in/fan-out. Same as `[a1, a2] >> [b1, b2, b3]`, but reads more clearly when the lists are big or come from separate variables.

```python
cross_downstream([a1, a2], [b1, b2, b3])    # 2 * 3 = 6 edges
```

---

## 4. `TaskGroup` — UI grouping

A `TaskGroup` is a **visual** wrapper. It does NOT change execution semantics; it just collapses related tasks into one node in the Graph view and prefixes their IDs with the group name (`etl.extract`, `etl.transform`, …).

```python
with TaskGroup("etl", tooltip="Daily ETL") as etl:
    extract = ...
    transform = ...
    extract >> transform

start >> etl >> end       # set deps on the GROUP — applies to every member
```

There's a decorator form too:

```python
@task_group(group_id="transform")
def transform_group(users, orders):
    @task
    def normalize_users(u): ...
    @task
    def normalize_orders(o): ...
    return normalize_users(u), normalize_orders(o)
```

### Why TaskGroup, not SubDAG?

`SubDagOperator` is **deprecated**. SubDAGs were full DAGs themselves, with their own scheduler entry, their own paused/unpaused state, and a long history of weird bugs. TaskGroups are pure UI sugar — same scheduling, same DAG run — and that's exactly what you want 99% of the time.

---

## 5. Setting deps across groups

When you set `group >> some_task`, every task inside the group becomes an upstream of `some_task`. To depend on a single task in the group, reference it directly:

```python
with TaskGroup("ingest") as ingest:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")

ingest >> downstream            # waits for BOTH a AND b
a >> downstream                 # waits for a only
```

---

## 6. Common gotchas

- **Cycles**: Airflow rejects DAGs with cycles at parse time. If you accidentally do `a >> b >> a`, the DAG won't load.
- **Hidden edges**: pulling an XCom inside a task body creates a *data* dependency but NOT a scheduling edge. Always use `>>` (or pass the XComArg as an argument) so the scheduler waits.
- **Group IDs and task IDs nest**: a task `extract` inside a TaskGroup `daily` has the full ID `daily.extract`. Use the full ID with `xcom_pull(task_ids="daily.extract")`.
- **`chain` with mismatched list sizes**: `chain([a, b], [c])` raises — the lists must be the same length.

---

## 7. Practice files

1. **`01_bitshift_deps.py`** — fan-out/fan-in with `>>` and lists.
2. **`02_chain_and_cross_downstream.py`** — see in the Graph view how `chain` differs from `>>` on lists.
3. **`03_task_groups.py`** — both styles of TaskGroup, and the dependency rule "set on the group".

Exercise: take `dags/02_taskflow_api/01_taskflow_basics.py` and wrap `extract` and `transform` in a TaskGroup called `etl`.

---

## 8. Self-check

1. What's the difference between `>>` on two lists and `chain` on two lists?
2. Does a TaskGroup affect when tasks run?
3. Why is `SubDagOperator` deprecated in favor of TaskGroup?
4. How do you pull XCom from a task that lives inside a TaskGroup?
5. When you set `groupA >> task_x`, what happens?
