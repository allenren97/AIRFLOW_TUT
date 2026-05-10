# Airflow 2.10 — Hands-on Learning Curriculum

A modular, practice-first learning repo targeting **Apache Airflow 2.10**. Each module is a folder of small, runnable DAGs that you can place into `$AIRFLOW_HOME/dags` and explore in the UI.

> Recommended: Read `notes/cheatsheet.md` whenever you're stuck.

---

## How to use this repo

Each module has a `README.md` lesson **next to** the practice `.py` files.
The recommended loop:

1. Read the module's `README.md` (10–15 min).
2. Open each `.py` file in order, run it in the UI, watch what it does.
3. Do the **Exercise** at the end of the lesson.
4. Answer the **Self-check** questions out loud — if any feel shaky, re-read the relevant section.

## What you will learn

| # | Lesson | Concepts |
|---|--------|----------|
| 01 | [Basics](dags/01_basics/README.md) | Anatomy of a DAG, `BashOperator`, `PythonOperator`, scheduling, catchup, `default_args`, `Param` |
| 02 | [TaskFlow API](dags/02_taskflow_api/README.md) | `@dag`/`@task` decorators, implicit XCom, `multiple_outputs`, mixing with classic ops |
| 03 | [Dependencies & Task Groups](dags/03_dependencies/README.md) | `>>` / `<<`, `chain`, `cross_downstream`, `TaskGroup` |
| 04 | [XCom](dags/04_xcom/README.md) | Push/pull, custom keys, TaskFlow XCom, large-payload pattern |
| 05 | [Branching & Trigger Rules](dags/05_branching/README.md) | `@task.branch`, `ShortCircuitOperator`, all 11 trigger rules |
| 06 | [Sensors](dags/06_sensors/README.md) | `FileSensor`, `ExternalTaskSensor`, `@task.sensor`, **deferrable** sensors |
| 07 | [Dynamic Task Mapping](dags/07_dynamic_task_mapping/README.md) | `.expand()`, `.expand_kwargs()`, `.partial()`, mapped TaskGroups |
| 08 | [Datasets](dags/08_datasets/README.md) | Producer/consumer, **AND/OR conditional scheduling**, **`DatasetAlias`** (2.10) |
| 09 | [Advanced](dags/09_advanced/README.md) | Jinja templating, `Params` UI form, Pools, callbacks, retries, custom timetables |
| 10 | [Custom components](dags/10_custom_components/README.md) | Custom `Operator`, `Hook`, `Sensor`, `Timetable` via `AirflowPlugin` |
| 11 | [External systems / ETL](dags/11_external_systems/README.md) | Postgres + HTTP + Dataset-event end-to-end ETL |
| ✓ | Tests + cheatsheet | DAG validity tests under `tests/`; quick reference at `notes/cheatsheet.md` |

---

## Quick start (Docker)

The fastest way to run all DAGs locally is the official compose file we ship:

```bash
cp .env.example .env

mkdir -p ./logs ./plugins
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose up airflow-init
docker compose up -d
```

Open <http://localhost:8080> and log in with `airflow` / `airflow`.

To stop and clean up:

```bash
docker compose down --volumes --remove-orphans
```

## Quick start (local venv)

```bash
python3.11 -m venv .venv
source .venv/bin/activate

AIRFLOW_VERSION=2.10.5
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$(pwd)/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone
```

> `airflow standalone` writes the admin password to `$AIRFLOW_HOME/standalone_admin_password.txt`.

---

## Repo layout

```
airflow/
├── dags/                    # Learning DAGs, grouped by topic, each folder has a README.md lesson
│   ├── 01_basics/           ├── README.md   ├── 01_*.py ... 05_*.py
│   ├── 02_taskflow_api/     ├── README.md   ├── 01_*.py ... 03_*.py
│   ├── 03_dependencies/     ├── README.md   └── ...
│   ├── 04_xcom/
│   ├── 05_branching/
│   ├── 06_sensors/
│   ├── 07_dynamic_task_mapping/
│   ├── 08_datasets/
│   ├── 09_advanced/
│   ├── 10_custom_components/
│   └── 11_external_systems/
├── plugins/                 # Custom Hook / Operator / Sensor / Timetable used in module 10
├── include/                 # Shared helpers
├── tests/                   # pytest suite (DAG validity + task unit tests)
├── notes/cheatsheet.md      # One-page reference
├── requirements.txt
├── docker-compose.yml
└── .env.example
```

---

## Suggested study path

1. **Read** `dags/01_basics/README.md`, then run those 5 DAGs in the UI.
2. Walk through modules 02 → 09 the same way: lesson first, then practice files, then the exercise at the end of the lesson.
3. **Build** module 10 (custom components) — best way to internalize Airflow's plugin system.
4. **Tie it together** with module 11 (real ETL pattern).
5. Run `pytest tests/` — try breaking a DAG and watching the validity test fail.

Every DAG ID is namespaced like `mod_<NN>_<name>` so it's easy to find in the UI.

When you're stuck, the one-page reference at [`notes/cheatsheet.md`](notes/cheatsheet.md) has the syntax for everything in this repo.
