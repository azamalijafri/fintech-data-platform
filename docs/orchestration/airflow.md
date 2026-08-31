# Airflow Integration

## Overview

Airflow is the **orchestration layer above the dbt/Snowflake transformation pipeline**. It does not replace Kafka, S3, Snowpipe, or dbt — it schedules, triggers, retries, and monitors the dbt workflow that runs after data has already landed in Snowflake `RAW`.

```
fintech-data-platform/
│
├── airflow/
│   ├── docker-compose.yml
│   ├── dags/
│   │   └── fintech_dbt_pipeline.py
│   ├── logs/
│   └── plugins/
│
├── dbt/
│   ├── models/
│   ├── tests/
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml
│
└── ...
```

| Layer          | Owns                                                                              |
| -------------- | --------------------------------------------------------------------------------- |
| Docker Compose | Runs the Airflow services as containers                                           |
| Airflow        | Scheduling, triggering, retries, run history, monitoring                          |
| dbt            | Transformation logic + data quality (tests)                                       |
| Snowflake      | Storage and query execution for all layers (RAW → STAGING → INTERMEDIATE → MARTS) |

---

## How it fits together

```
Docker Compose
     │
     ├── airflow-scheduler   (reads DAGs, schedules task instances)
     ├── airflow-webserver   (UI: DAGs, runs, logs, task state)
     ├── airflow-worker      (executes tasks — Celery executor)
     ├── airflow-triggerer
     ├── postgres            (Airflow metadata: DAG runs, task state)
     └── redis                (Celery message broker)
              │
              ▼
    fintech_dbt_pipeline.py  (DAG, schedule = */5 * * * *)
              │
              ▼
         dbt_build  (BashOperator → `dbt build`)
              │
              ▼
     dbt build (models + tests, interleaved, dbt's own internal DAG)
              │
     ┌────────┼────────┐
     ▼        ▼         ▼
 STAGING  INTERMEDIATE  MARTS
              │
              ▼
     Airflow SUCCESS / FAILURE
```

Important distinction to keep straight when documenting or discussing this: **Docker starts containers; Airflow runs inside them.** The Docker Compose layer has no awareness of DAGs, tasks, or schedules — that's entirely Airflow's job once the containers are up.

---

## 1. Docker Layer

Airflow is containerized via Docker Compose rather than installed directly on the host — the standard approach for local Airflow development, splitting the scheduler, webserver, worker, metadata DB, and broker into separate services.

| Service             | Responsibility                                                                   |
| ------------------- | -------------------------------------------------------------------------------- |
| `airflow-scheduler` | Reads DAG definitions, checks dependencies, creates and schedules task instances |
| `airflow-webserver` | Serves the Airflow UI (DAGs, task logs, run history, schedules)                  |
| `airflow-worker`    | Executes tasks assigned by the scheduler (Celery executor)                       |
| `airflow-triggerer` | Handles deferrable/async tasks                                                   |
| `postgres`          | Airflow metadata store (DAG runs, task instances, task state)                    |
| `redis`             | Message broker for the Celery executor                                           |
| `airflow-init`      | One-time DB migration + initial user creation                                    |

**Start the environment:**

```bash
cd ~/Projects/fintech-data-platform
docker compose -f airflow/docker-compose.yml up -d
```

**Run Airflow CLI commands** (Airflow lives inside the container, so every CLI call is prefixed accordingly):

```bash
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags list
```

The actual command is `airflow dags list` — the Docker prefix is just how you reach it.

---

## 2. DAG Configuration

```python
with DAG(
    dag_id="fintech_dbt_pipeline",
    default_args=default_args,
    description="Run dbt transformations for the FinTech transaction pipeline",
    start_date=datetime(2026, 8, 31),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["fintech", "dbt", "snowflake"],
) as dag:
```

| Setting       | Value                  | Why                                                                                                   |
| ------------- | ---------------------- | ----------------------------------------------------------------------------------------------------- |
| `dag_id`      | `fintech_dbt_pipeline` | Unique identifier used across every Airflow CLI/UI reference                                          |
| `schedule`    | `*/5 * * * *`          | Every 5 minutes                                                                                       |
| `catchup`     | `False`                | Only run going forward from now — don't backfill every interval between `start_date` and today        |
| `retries`     | `2`                    | Retry transient failures (Snowflake connectivity, network blips) rather than failing the run outright |
| `retry_delay` | `2 minutes`            | Gap between retry attempts                                                                            |
| `owner`       | `data-engineering`     | Ownership tag on tasks                                                                                |

```python
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}
```

---

## 3. Task Definition

```python
dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=(
        "cd /opt/airflow/dbt && "
        "dbt build --profiles-dir /opt/airflow/dbt"
    ),
)
```

**Why `BashOperator`, not a dbt-specific operator or Python wrapper:** dbt is a CLI application, so Airflow's job here is limited to invoking `dbt build` and reacting to its exit code — nothing about the transformation logic lives in Airflow. This keeps the boundary clean:

- **Airflow** = orchestration (when/whether to run, retries, monitoring)
- **dbt** = transformation + data quality (what actually happens to the data)
- **Snowflake** = execution/storage

> **Open item:** a `hello_world` `PythonOperator` task was added at one point to confirm both operator types work in this DAG. The source material is inconsistent on whether it's wired as a dependency _before_ `dbt_build` or running in parallel alongside it — confirm the actual DAG code before writing this into a final architecture diagram, since the two mean different things (a real pre-check vs. an unrelated demo task).

**dbt's own internal DAG** determines execution order for models — Airflow only sees one task (`dbt_build`) succeed or fail as a unit:

```
stg_transactions → int_transactions → fct_fraud_transactions
```

`dbt build` runs models and their associated tests **interleaved** in this order, not as a separate models-then-tests pass — a failing test on `stg_transactions` can block the dependent models from building.

---

## 4. Known Trade-off: No Freshness Gate Before `dbt_build`

The DAG currently triggers `dbt build` on a fixed 5-minute schedule with no check for whether Snowpipe has finished loading everything expected for that window — Snowpipe ingestion is continuous and event-driven, while this DAG is a fixed interval.

This is an **eventual-consistency** design, not an oversight, but it should be stated explicitly rather than assumed:

- A transaction landing in Snowflake seconds before a scheduled run may not be reflected in the marts until the _next_ run.
- Whether that's fully harmless depends on whether the mart models are incremental (late data naturally gets picked up next run) or full-refresh (same behavior, just re-scanning more data each time). **This should be confirmed and documented** — it's not clear from the current implementation which one is in place.
- If stronger guarantees are ever needed, the fix is a `dbt source freshness` check (or a `SYSTEM$PIPE_STATUS`/`COPY_HISTORY` check) as a task before `dbt_build`, not a change to the schedule interval.

---

## 5. Troubleshooting Notes

Real issues hit while building this, worth keeping as a record:

### NULL values failing dbt tests

Staging tests failed on non-null constraints. Investigated directly in Snowflake:

```sql
select *
from {{ ref('stg_transactions') }}
where amount is null
   or isFlaggedFraud is null
   or isFraud is null
   or nameOrig is null
   or step is null
   or type is null
```

Found 11 of 90 rows affected across those columns — a source data-quality issue, not an Airflow or scheduling problem.

**Fix applied:** removed the invalid records from the Snowflake source table directly. This was a pragmatic call to get the pipeline running, not a production-quality pattern.

**Better long-term design (not yet implemented):** don't delete bad records — split RAW into valid/invalid paths so nothing is silently lost:

```
RAW
 ├── valid records   → STAGING
 └── invalid records → QUARANTINE / DQ_FAILURE
```

### DAG not running on schedule

`airflow dags details fintech_dbt_pipeline` showed `is_paused | True`. The DAG code was valid the whole time — it was an Airflow _state_ issue, not a code issue. Unpausing it let the `*/5 * * * *` schedule take effect.

### CLI flag mismatch

`airflow dags list-runs -d fintech_dbt_pipeline` failed — this Airflow version takes `dag_id` as a **positional** argument, not a `-d` flag:

```bash
airflow dags list-runs fintech_dbt_pipeline
```

---

## 6. Validation Commands

```bash
# Confirm the DAG was parsed with no import errors
docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
  airflow dags list | grep fintech_dbt_pipeline

# Full DAG config: schedule, pause state, owners, import errors
airflow dags details fintech_dbt_pipeline

# Run a specific task directly, without waiting for the schedule
airflow tasks test fintech_dbt_pipeline dbt_build 2026-08-31

# Confirm a DAG run actually completed
airflow dags list-runs fintech_dbt_pipeline
```

`airflow dags details` output worth checking specifically:

```
has_import_errors | False     ← DAG file parsed successfully
is_paused         | False     ← must be False for the schedule to fire
timetable_summary | */5 * * * *
```

Independently verifying dbt (outside Airflow) before wiring it into the DAG:

```bash
cd dbt && dbt build --profiles-dir /opt/airflow/dbt
```

Result once fixed: `PASS=20 WARN=0 ERROR=0 SKIP=0 TOTAL=20` (3 table models + 17 tests).

---

## 7. Interview Summary

> Airflow is containerized via Docker Compose, running the scheduler, webserver, worker, metadata database (Postgres), and Celery broker (Redis) as separate services.
>
> The DAG (`fintech_dbt_pipeline`) runs every 5 minutes with catchup disabled, and its main task invokes `dbt build` via `BashOperator`. dbt owns the transformation and data-quality layer — it builds `stg_transactions → int_transactions → fct_fraud_transactions` and runs 17 tests interleaved with the model builds. Snowflake is the execution/storage layer underneath.
>
> Task-level retries (2 retries, 2-minute delay) handle transient failures like Snowflake connectivity blips. The DAG has no explicit freshness gate before `dbt build` — it currently accepts eventual consistency on a 5-minute cadence, which is a stated trade-off rather than an oversight.
>
> Validated the setup using Airflow's CLI: `dags list` (parsed correctly), `dags details` (schedule + pause state), `tasks test` (direct task execution, bypassing the schedule), and `dags list-runs` (confirming completed runs). One real issue hit along the way — a data-quality failure traced to NULL source fields via direct SQL investigation, not an Airflow problem — and one operational issue, a paused DAG that looked like a scheduling bug but was just DAG state.

---

## Open Questions / Follow-ups

- [ ] Confirm whether mart models are incremental or full-refresh — affects how late-arriving Snowpipe loads are handled under the current no-freshness-gate design
- [ ] Confirm actual wiring of the `hello_world` Python task (dependency before `dbt_build`, or unrelated parallel task) — or remove it if it was scaffolding only
- [ ] Decide whether the quarantine-vs-delete pattern for bad source records is worth implementing, or stays a documented "known simplification" for this project
- [ ] Decide whether a `dbt source freshness` (or Snowpipe status) check belongs as a task ahead of `dbt_build`
