# Module 11 — External systems & a real ETL

> **Goal**: tie everything together by talking to real systems — HTTP APIs and Postgres — and orchestrating a complete extract → transform → load pipeline.

---

## 1. The provider package model

Airflow ships a tiny core. Everything else (Postgres, S3, Snowflake, GCP, AWS, dbt, Slack, …) lives in **provider packages** you install separately. The provider you install determines:

- Which **operators** you have (`PostgresOperator`, `S3CreateObjectOperator`, …).
- Which **hooks** you have (`PostgresHook`, `S3Hook`, …).
- Which **connection types** appear in Admin → Connections.

This repo's `requirements.txt` installs the providers we use:

```
apache-airflow-providers-postgres
apache-airflow-providers-http
apache-airflow-providers-common-sql
```

Every concept from previous modules works the same — these providers just give you more operators to use as task primitives.

---

## 2. Connections — the credential store

A **Connection** bundles host + port + login + password + schema + extra (free-form JSON) + connection type.

Hooks consume them by `conn_id`:

```python
PostgresHook(postgres_conn_id="postgres_default")
HttpHook(http_conn_id="http_jsonplaceholder")
```

Three ways to create a connection:

1. UI: Admin → Connections → +.
2. CLI: `airflow connections add postgres_default --conn-uri 'postgres://airflow:airflow@postgres:5432/airflow'`.
3. Env var: `AIRFLOW_CONN_POSTGRES_DEFAULT='postgres://airflow:airflow@postgres:5432/airflow'`.

Conventions:

- Use stable conn_ids you reference everywhere: `postgres_default`, `s3_default`, `snowflake_main`.
- Put non-credential metadata (region, role) in the `extra` JSON, not the host.
- Never commit secrets — env vars or a real secrets backend (Vault, AWS Secrets Manager) only.

---

## 3. SQL with the Postgres provider

### Using `SQLExecuteQueryOperator`

The modern, vendor-neutral SQL runner (replaces `PostgresOperator`):

```python
SQLExecuteQueryOperator(
    task_id="setup",
    conn_id="postgres_default",
    sql="""
        CREATE TABLE IF NOT EXISTS users (id INT, name TEXT);
        INSERT INTO users VALUES (1, 'alice');
    """,
)
```

`sql` can be a string, a list of strings, or a path to a `.sql` file (when `template_searchpath=` is set on the DAG).

### Using `PostgresHook` inside a `@task`

When you need pandas, batched inserts, or branching logic:

```python
@task
def insert_rows() -> int:
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.insert_rows(table="users", rows=[(1, "a"), (2, "b")], target_fields=["id", "name"])
    total = hook.get_first("SELECT COUNT(*) FROM users;")[0]
    return total
```

`PostgresHook` (any DB hook, really) gives you `run`, `get_records`, `get_first`, `get_pandas_df`, `insert_rows`, `bulk_load`, `copy_expert`. Use the Pandas one for analytics, `insert_rows` for OLTP-style batches.

### Idempotency matters

ETL tasks **will** be retried, manually re-run, and backfilled. Write SQL that's safe to run twice:

- `CREATE TABLE IF NOT EXISTS`
- `DELETE WHERE date = '{{ ds }}'` immediately followed by `INSERT`
- `MERGE` / `INSERT ... ON CONFLICT DO UPDATE` if the DB supports it

---

## 4. HTTP with the HTTP provider

### Style 1: `HttpOperator`

```python
HttpOperator(
    task_id="fetch",
    method="GET",
    endpoint="users",
    http_conn_id="http_jsonplaceholder",
    log_response=True,
    do_xcom_push=True,
)
```

The `http_conn_id` connection's host should be the API base URL (`https://jsonplaceholder.typicode.com`). The operator concatenates it with `endpoint` to form the request URL.

### Style 2: plain `requests` inside `@task`

```python
@task
def fetch_users() -> list[dict]:
    base = Variable.get("API_BASE_URL")
    resp = requests.get(f"{base}/users", timeout=10, headers={"Authorization": ...})
    resp.raise_for_status()
    return resp.json()
```

Use this when you want pagination, retries with custom backoff, streaming downloads, or anything beyond a single GET/POST.

For polling endpoints (wait for job completion), use `HttpSensor` (deferrable for free).

---

## 5. The "real" ETL pattern

Combining everything from this module + previous lessons:

1. **Extract** — fetch from HTTP / S3 / DB / API. Return a small payload OR a path/URI for a big one (module 04).
2. **Transform** — pure Python. Easy to unit-test if you keep the logic in `include/`.
3. **Load** — `insert_rows` / `MERGE` / file write. Idempotent.
4. **Publish** — emit a Dataset event so downstream DAGs trigger automatically (module 08).
5. **Audit** — write a row to an `*_audit` table with `run_id`, row counts, finished_at. Invaluable for debugging.

Wrap retries with exponential backoff (module 09). Add `on_failure_callback` for alerting. Use `max_active_runs=1` to prevent concurrent writes if the pipeline isn't safe for that.

---

## 6. Common gotchas

- **Hardcoding hostnames** instead of using a Connection. Now you have to redeploy to switch environments.
- **Returning a giant DataFrame from `@task`.** XCom won't fit it. Write to S3, return the URI.
- **Forgetting `do_xcom_push` semantics**: `HttpOperator` pushes the response *body as a string*; you usually `json.loads` it downstream.
- **Not handling 4xx/5xx**: `HttpOperator`'s default `response_check` is `None` — it considers any non-network-error a success. Set `response_filter`/`response_check` or use plain `requests` with `raise_for_status()`.
- **Logging credentials**: Airflow's logs go everywhere. Don't print the connection password.
- **DB connection saturation**: a heavily-mapped task hammering Postgres can exhaust connections. Use a Pool (module 09) to cap concurrency.

---

## 7. Practice files

1. **`01_http_taskflow.py`** — both styles side-by-side: `HttpOperator` and `requests` inside `@task`. Combine results in a summary task.
2. **`02_postgres_pipeline.py`** — `SQLExecuteQueryOperator` for DDL + `PostgresHook` for data + a follow-up SELECT to confirm.
3. **`03_full_etl_pipeline.py`** — HTTP → transform → Postgres → Dataset event → consumer DAG. Production-shaped: retries, callback, audit table, idempotent reload.

To make these runnable end-to-end:

```bash
# Postgres connection (the docker-compose Postgres is reachable as host=postgres):
airflow connections add postgres_default --conn-uri 'postgres://airflow:airflow@postgres:5432/airflow'

# HTTP connection (or set the AIRFLOW_VAR_API_BASE_URL variable instead):
airflow connections add http_jsonplaceholder --conn-type http --conn-host 'https://jsonplaceholder.typicode.com'
```

Exercise: in `03_full_etl_pipeline.py`, add a branch after `extract` that short-circuits if the API returned zero users. Use the `@task.short_circuit` you learned in module 05.

---

## 8. Self-check

1. Why are providers separate packages instead of being in core?
2. What's the difference between `SQLExecuteQueryOperator` and `PostgresHook`?
3. Why is idempotency a non-negotiable property of load tasks?
4. When would you choose `HttpOperator` over plain `requests` inside a `@task`?
5. How does the dataset event in `03_full_etl_pipeline.py` make life easier for downstream DAGs?
