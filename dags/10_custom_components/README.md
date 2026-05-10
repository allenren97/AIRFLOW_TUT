# Module 10 — Custom Components (Plugins)

> **Goal**: build your own Hook, Operator, Sensor, and Timetable, register them through an `AirflowPlugin`, and use them from a DAG.

---

## 1. Why custom components?

Airflow ships with hundreds of provider operators, but you'll eventually want:

- A **Hook** wrapping a private internal API.
- An **Operator** that bundles 3 of your team's standard steps into one node.
- A **Sensor** for a bespoke "is the upstream system ready?" check.
- A **Timetable** matching your business calendar.

Building them properly (instead of dropping helper functions into a DAG) means: reuse, testability, type safety, and a tidy operator block in the Graph view.

---

## 2. The plugin folder

Airflow scans a configured plugins folder (in this repo: `plugins/`, mounted at `/opt/airflow/plugins`). Every `.py` file inside is added to `sys.path`, so you can `from custom_operator import GreetOperator` directly from a DAG.

Two ways to expose components:

1. **Just a Python module on the path** — works for Operators, Hooks, Sensors. Import directly.
2. **Through an `AirflowPlugin`** — required for Timetables, Listeners, Macros, Web extensions, and anything else the scheduler needs to be aware of by name.

---

## 3. Hooks — wrap an external system

A Hook subclasses `BaseHook` and pulls credentials from an Airflow Connection.

```python
class GreetingHook(BaseHook):
    conn_name_attr = "greeting_conn_id"
    default_conn_name = "greeting_default"
    conn_type = "greeting"               # appears in Admin → Connections
    hook_name = "Greeting"               # display name

    def __init__(self, greeting_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.greeting_conn_id = greeting_conn_id

    def get_conn(self):
        conn = self.get_connection(self.greeting_conn_id)
        return some_client(host=conn.host, **conn.extra_dejson)
```

Conventions:

- Implement `get_conn()` returning the underlying client.
- One method per logical operation: `run_query`, `upload_file`, `list_objects`, etc.
- Don't open the connection in `__init__` — open lazily in `get_conn`/operation methods so DAG parsing stays cheap.

---

## 4. Operators — wrap one logical action

An Operator subclasses `BaseOperator` and implements `execute(self, context)`.

```python
class GreetOperator(BaseOperator):
    template_fields = ("name",)          # which fields render Jinja
    ui_color = "#a4d8e0"                 # node color in the Graph view

    def __init__(self, *, name: str, greeting_conn_id: str = "greeting_default", **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.greeting_conn_id = greeting_conn_id

    def execute(self, context) -> str:
        hook = GreetingHook(self.greeting_conn_id)
        message = hook.greet(self.name)
        self.log.info(message)
        return message                   # return value goes to XCom
```

Checklist for a good custom operator:

- Subclass `BaseOperator`.
- Constructor takes only the operator-specific kwargs; pass `**kwargs` through to `super().__init__()`.
- Declare `template_fields` for any field you want Jinja-rendered.
- Implement `execute(context)`. Return the result you want pushed to XCom.
- Use `self.log` (the per-task logger) instead of `print` so logs end up where the UI shows them.
- For long-running work that should be cancellable, implement `on_kill(self)` too.

---

## 5. Sensors — wrap a "wait for X" check

A Sensor subclasses `BaseSensorOperator` and implements `poke(self, context) -> bool`.

```python
class CounterSensor(BaseSensorOperator):
    template_fields = ("threshold",)

    def __init__(self, *, threshold: int, **kwargs):
        super().__init__(**kwargs)
        self.threshold = threshold
        self._counter = 0

    def poke(self, context) -> bool:
        self._counter += 1
        self.log.info("Poke #%d", self._counter)
        return self._counter >= self.threshold
```

You inherit `poke_interval`, `timeout`, `mode`, `soft_fail`, `exponential_backoff` — all the standard sensor knobs from module 06. For deferrable behavior, additionally implement `execute(self, context)` that calls `self.defer(trigger=..., method_name="execute_complete")` and a corresponding async Trigger class — out of scope here.

---

## 6. Timetables — register through a plugin

```python
class WeekdayMorningTimetable(Timetable):
    def infer_manual_data_interval(self, *, run_after): ...
    def next_dagrun_info(self, *, last_automated_data_interval, restriction): ...

class WeekdayMorningTimetablePlugin(AirflowPlugin):
    name = "weekday_morning_timetable_plugin"
    timetables = [WeekdayMorningTimetable]
```

The plugin class is what lets Airflow re-instantiate your timetable from a serialized DAG (it stores the class path by name). Without the plugin registration, the scheduler will refuse to load any DAG that references your timetable across a restart.

Other things you can register on an `AirflowPlugin`: `operators`, `hooks`, `sensors` (for legacy auto-discovery), `flask_blueprints` (UI extensions), `appbuilder_views`, `listeners`, `macros`.

---

## 7. Loading your plugin

If `plugins/` is on Airflow's plugins folder, classes are picked up automatically. You can verify:

```bash
airflow plugins
```

This lists every registered plugin and what it exposes.

In a Python file inside `dags/`, you import these modules just like normal Python files:

```python
from custom_operator import GreetOperator
from custom_sensor import CounterSensor
from custom_timetable_plugin import WeekdayMorningTimetable
```

(Airflow adds the plugins folder to `sys.path`.)

---

## 8. Testing custom components

Without spinning up Airflow, you can:

- Instantiate the operator with fake arguments and call `execute({...})` directly. Pass a stub `context` dict.
- Test the hook against a fake `Connection` by setting the env var `AIRFLOW_CONN_<ID>=...` in your test fixture.
- For sensors, call `.poke({})` repeatedly and assert it returns `True` after N calls.

Keep heavy I/O inside the hook — that way unit tests of the operator can mock the hook.

---

## 9. Common gotchas

- **Naming collisions**: putting `class Operator(BaseOperator)` in `plugins/operator.py` clashes with any third-party `operator` module. Use specific names.
- **Shared state across mapped instances**: a class attribute is shared! In the `CounterSensor` example we used an instance attribute `self._counter`, which is correct.
- **Forgetting `**kwargs` to `super().__init__`** breaks all the BaseOperator behavior (retries, callbacks, etc.).
- **`template_fields` referencing a field your `__init__` doesn't store** — Airflow tries to render `self.foo` and crashes if it doesn't exist.
- **Plugin not loaded** — check `airflow plugins` and confirm the plugins folder is mounted/configured.

---

## 10. Practice files

The plugin classes live in `plugins/`:

- `plugins/custom_hook.py` — `GreetingHook`.
- `plugins/custom_operator.py` — `GreetOperator` using the hook.
- `plugins/custom_sensor.py` — `CounterSensor`.
- `plugins/custom_timetable_plugin.py` — `WeekdayMorningTimetable` registered as a plugin.

The DAG is in this folder:

- **`01_use_custom_components.py`** — `CounterSensor` waits for 3 pokes, then `GreetOperator` runs, then a `@task` consumes its return value.

To make the operator/hook part actually run end-to-end, create a connection:

```bash
airflow connections add greeting_default \
  --conn-uri 'greeting://my-greeting-service?prefix=Howdy'
```

Or via env var: `AIRFLOW_CONN_GREETING_DEFAULT='greeting://my-greeting-service?prefix=Howdy'`.

Exercise: add a `goodbye` method to `GreetingHook` and a corresponding `FarewellOperator`. Wire it into the DAG after `GreetOperator`.

---

## 11. Self-check

1. What's the difference between just dropping a Python file into `plugins/` and registering an `AirflowPlugin`?
2. Why must Timetables go through a plugin class but Operators/Hooks usually don't?
3. What's the role of `template_fields` on a custom operator?
4. Why do you implement `poke(context) -> bool` for a sensor instead of `execute`?
5. How do you verify your plugin is loaded?
