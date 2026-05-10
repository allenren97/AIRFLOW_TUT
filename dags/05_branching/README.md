# Module 05 — Branching & Trigger Rules

> **Goal**: pick which downstream tasks run, and control when "join" tasks fire after a branch.

---

## 1. The two branching tools

### 1.1 `@task.branch` — pick a path

A branching task returns the **task_id** (or list of task_ids) of the downstream(s) to follow. Every other downstream is **skipped**.

```python
@task.branch
def pick() -> str:
    return "high_priority" if needs_attention() else "low_priority"

@task
def high_priority(): ...
@task
def low_priority(): ...

pick() >> [high_priority(), low_priority()]
```

The classic equivalent is `BranchPythonOperator` — same idea, more boilerplate.

### 1.2 `@task.short_circuit` — skip everything if condition false

A short-circuit task returns truthy/falsy:

- truthy → downstream proceeds normally.
- falsy → all downstream tasks are SKIPPED.

```python
@task.short_circuit
def has_records(count: int) -> bool:
    return count > 0
```

Use it for **preconditions**: "did upstream produce data?", "is today a business day?", "is the API healthy?". One-liner pruning of the rest of the run.

---

## 2. The "join after branch" problem

After a branch, one path is skipped. If you have a downstream task that should run when **either** branch finishes, the default trigger rule (`all_success`) breaks it — the skipped parent makes the join skip too.

```python
join = EmptyOperator(
    task_id="join",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,    # the magic
)
```

This says "run me as long as nothing failed AND at least one parent succeeded". Skipped parents are fine.

---

## 3. Trigger rules — the full set

Every task has a `trigger_rule` that decides when it can start, based on the state of its **direct upstream tasks**.

| Rule | Run when |
|---|---|
| `all_success` (default) | every upstream succeeded |
| `all_failed` | every upstream failed |
| `all_done` | every upstream is in a terminal state |
| `all_skipped` | every upstream was skipped |
| `one_success` | at least one upstream succeeded |
| `one_failed` | at least one upstream failed |
| `one_done` | at least one upstream finished (success/failed/skipped) |
| `none_failed` | no upstream failed (skipped is OK) |
| `none_failed_min_one_success` | no failures and at least one success — common after a branch |
| `none_skipped` | no upstream was skipped |
| `always` | unconditional |

### Patterns you'll actually use

- **Cleanup task that always runs** → `all_done` (so you tear down even after failures).
- **Alert task that fires on failure** → `one_failed`.
- **Join after a branch** → `none_failed_min_one_success`.
- **"Run only if everything went perfectly"** → `all_success` (default).

---

## 4. State propagation

Skipped propagates **downstream** by default — if `b` is skipped and `c` depends on `b`, `c` is also skipped (because `all_success` requires success). That's why a branched path can naturally "die out" without you doing anything extra.

`ShortCircuitOperator` has an extra knob: `ignore_downstream_trigger_rules=True` (default) which forces skip propagation through tasks even if they have a non-default trigger rule. Set it `False` if you want a branch like "always-run cleanup" to still fire after a short-circuit.

---

## 5. Common gotchas

- **Branch returning a non-existent task_id** → DAG fails parsing or the branch task fails at runtime. Always make sure the returned id matches a direct downstream.
- **Forgetting the join's trigger rule** → join silently gets skipped along with the unused branch.
- **Short-circuiting a sensor's downstream** → If the sensor is upstream and times out (state = failed), `ShortCircuitOperator` doesn't help. Use `soft_fail=True` on the sensor instead so it skips on timeout.
- **Branching on randomness without setting `do_xcom_push=False`** → branch tasks DO push their decision to XCom (the chosen task_id), so you can see it in the UI. That's intentional, just be aware.

---

## 6. Practice files

1. **`01_branch_python.py`** — `@task.branch` plus a `none_failed_min_one_success` join.
2. **`02_short_circuit.py`** — flip the integer in `detect_records()` and watch downstream skip vs run.
3. **`03_trigger_rules.py`** — three upstreams (success, success, failed) feeding into four downstreams with different rules. Trigger it and look at the Graph: see exactly which colors land on which trigger rule.

Exercise: add a `cleanup` task to `01_branch_python.py` that runs `all_done` after the join, even if the branch path failed.

---

## 7. Self-check

1. What does `@task.branch` return?
2. Why might a "join" task get skipped after a branch, and how do you fix it?
3. When would you choose `short_circuit` over `branch`?
4. Which trigger rule would you use for a Slack-on-failure alert task?
5. What does `all_done` mean — and what does it NOT mean?
