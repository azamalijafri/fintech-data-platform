# FinTech Data Platform

A local FinTech data engineering project that streams synthetic financial transactions through Kafka into Amazon S3, auto-loads them into Snowflake via Snowpipe, transforms them with dbt into analytics-ready marts, and orchestrates the pipeline with Apache Airflow.

## Architecture

The platform is split into distinct layers, each owning a single responsibility:

| Layer | Component | Responsibility |
| ----- | --------- | -------------- |
| Ingest | Kafka producer | Reads the PaySim CSV and streams JSON transaction events to a Kafka topic |
| Ingest | Kafka consumer | Consumes events, batches them, and uploads JSONL files to S3 |
| Store | Amazon S3 | Raw landing zone under `raw/transactions/{YYYY}/{MM}/{DD}/{HH}/batch-*.jsonl` |
| Notify | S3 → SNS | Emits an `ObjectCreated` event on new files and delivers it via SNS |
| Load | Snowpipe | Auto-ingests new S3 files into `RAW.TRANSACTIONS` (event-driven, `AUTO_INGEST=TRUE`) |
| Transform | dbt | Builds and tests `stg → int → fct` models in Snowflake |
| Orchestrate | Airflow | Schedules, triggers, retries, and monitors the dbt workflow |

**Key boundary:** Kafka only handles streaming/buffering. S3 is the raw store. Snowpipe handles automatic S3→RAW loading. dbt owns the transformation and data-quality layer. Airflow is purely orchestration on top — it has no awareness of the raw data journey; it simply runs `dbt build` on a cadence and reports success or failure.

## Repository layout

```
fintech-data-platform/
│
├── src/
│   └── fintech/                       # shared Python package (config + event schema)
│       ├── config.py                  # env-driven configuration helpers
│       ├── events.py                  # canonical event envelope builder
│       └── __init__.py
│
├── services/
│   ├── ingest/
│   │   ├── producer/                  # PaySim CSV → Kafka topic (Dockerfile, producer.py)
│   │   └── consumer/                  # Kafka → batched JSONL upload to S3 (Dockerfile, consumer.py)
│   └── orchestrator/                  # Apache Airflow 3
│       ├── Dockerfile                 # apache/airflow:3.0.6 + dbt-core + dbt-snowflake
│       ├── dags/
│       │   └── fintech_dbt_pipeline.py
│       ├── plugins/
│       │   └── fintech/tasks.py
│       └── logs/
│
├── dbt/                               # dbt project (transformation logic)
│   ├── dbt_project.yml
│   ├── profiles.yml                   # Snowflake connection (env-var driven)
│   ├── models/
│   │   ├── staging/                   # stg_transactions (+ data tests)
│   │   ├── intermediate/              # int_transactions
│   │   └── marts/                     # fct_fraud_transactions
│   └── macros/generate_schema_name.sql
│
├── infra/
│   └── docker/
│       └── docker-compose.yml         # single platform stack (ingest + orchestration)
│
├── docs/
│   ├── orchestration/airflow.md
│   ├── ingestion/kafka.md
│   └── cloud/
│       ├── s3-snowflake-storage-integration.md
│       └── snowpipe-notification-integration.md
│
├── scripts/                           # helper scripts
├── tests/                             # test suite
├── data/
│   └── paysim-dataset.csv             # local source dataset (gitignored)
│
├── Makefile                           # common dev tasks
├── pyproject.toml                     # shared Python package definition
├── .python-version
├── .github/workflows/ci.yml
└── .env.example
```

## Prerequisites

- Docker and Docker Compose
- An external Docker network named `fintech-network`
- AWS credentials (for S3 reads/writes from the consumer)
- A Snowflake account with a configured S3 storage integration and Snowpipe
- Python 3.14 (only needed for local dbt / script runs)

## Configuration

Copy `.env.example` to `.env` and fill in your values. `.env` is gitignored and injected into containers via `env_file`.

```bash
# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=FINTECH
SNOWFLAKE_SCHEMA=RAW

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=financial-transactions
KAFKA_CONSUMER_GROUP=transaction-consumer-group

# S3
S3_BUCKET=fintech-data-platform-azam-2026
S3_PREFIX=raw/transactions

# PaySim
PAYSIM_FILE=/data/paysim-dataset.csv
```

## Getting started

The whole platform is defined in a single compose file at `infra/docker/docker-compose.yml`. The Airflow (orchestration) services sit behind the `orchestrate` compose profile, so you can run the ingest stack and the orchestration stack together or independently.

### 1. Create the shared network

```bash
docker network create fintech-network
```

### 2. Start the platform

Everything (ingest + Airflow):

```bash
cd ~/Projects/fintech-data-platform
docker compose -f infra/docker/docker-compose.yml --profile orchestrate up -d
```

Ingest only (Kafka, producer, consumer):

```bash
docker compose -f infra/docker/docker-compose.yml up -d
```

Payload-oriented containers:

| Container | Purpose |
| --------- | ------- |
| `fintech-kafka` | Kafka broker |
| `fintech-producer` | Streams PaySim events to the topic on startup |
| `fintech-consumer` | Subscribes, batches events, and uploads JSONL to S3 |

Airflow containers:

| Container | Responsibility |
| --------- | -------------- |
| `fintech-airflow-init` | One-time DB migration (exits after completing) |
| `fintech-airflow-postgres` | Airflow metadata database |
| `fintech-airflow-api-server` | FastAPI server (UI + Execution API) on port `8080` |
| `fintech-airflow-scheduler` | Reads DAGs, schedules and executes task instances |
| `fintech-airflow-dag-processor` | Parses/serializes DAG files for the scheduler |

**Airflow UI:** http://localhost:8080 \
**Credentials:** the username/password pair defined by `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` in `infra/docker/docker-compose.yml` (default `admin:admin`).

## The Airflow DAG

File: `services/orchestrator/dags/fintech_dbt_pipeline.py`

```python
with DAG(
    dag_id="fintech_dbt_pipeline",
    schedule="*/5 * * * *",
    catchup=False,
    ...
):
    start_pipeline_task = PythonOperator(task_id="start_pipeline", python_callable=start_pipeline)
    dbt_build = BashOperator(task_id="dbt_build", bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt")

    start_pipeline_task >> dbt_build
```

- **Schedule:** every 5 minutes
- **catchup:** disabled (no backfill from `start_date`)
- **Tasks:** `start_pipeline` (hello-world check) then `dbt_build` (runs the dbt project)
- **Retries:** 2 retries, 2-minute delay (handles transient Snowflake/network blips)

The `dbt_build` task runs `dbt build`, which builds dbt's internal DAG (`stg_transactions → int_transactions → fct_fraud_transactions`) and runs its data tests interleaved. The dbt project is mounted into the containers at `/opt/airflow/dbt`.

## dbt models

| Layer | Model | Purpose |
| ----- | ----- | ------- |
| Staging | `stg_transactions` | Clean, typed view over `RAW.TRANSACTIONS` |
| Intermediate | `int_transactions` | Adds derived fields: `fraud_status`, `transaction_amount_category` |
| Marts | `fct_fraud_transactions` | Aggregated fact: counts, totals, averages, and fraud amounts by type/category/status |

Models are materialized as tables (`+materialized: table`) with per-layer schemas `STAGING`, `INTERMEDIATE`, and `MARTS` (via the `generate_schema_name` macro).

## Architecture deep-dive / runbooks

Detailed setup and troubleshooting guides live in `docs/`:

- `docs/orchestration/airflow.md` — Airflow orchestration details, DAG config, and troubleshooting
- `docs/ingestion/kafka.md` — Kafka producer/consumer integration
- `docs/cloud/s3-snowflake-storage-integration.md` — S3 ↔ Snowflake storage integration runbook
- `docs/cloud/snowpipe-notification-integration.md` — event-driven Snowpipe auto-ingest runbook

## Useful commands

### Makefile

Most day-to-day tasks are wrapped in the `Makefile`. Run `make help` to list them.

```bash
make up                # start whole stack (ingest + Airflow)
make ingest-up         # start ingest only
make orchestrate-up    # start Airflow only
make logs              # tail all logs
make dbt-build         # run dbt locally
make validate          # validate compose file
```

### Docker Compose

```bash
# Overall status
docker compose -f infra/docker/docker-compose.yml --profile orchestrate ps

# Scheduler logs
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  logs airflow-scheduler --tail 100

# List / inspect DAGs
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow dags list
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow dags details fintech_dbt_pipeline

# Pause / unpause a pipeline
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow dags pause fintech_dbt_pipeline
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow dags unpause fintech_dbt_pipeline

# Run a task directly (bypass schedule)
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow tasks test fintech_dbt_pipeline dbt_build 2026-08-31

# List completed runs
docker compose -f infra/docker/docker-compose.yml --profile orchestrate \
  exec airflow-scheduler airflow dags list-runs fintech_dbt_pipeline

# Ingest logs
docker compose -f infra/docker/docker-compose.yml logs -f producer consumer
```

## Notes and known considerations

- **Eventual consistency:** the Airflow DAG runs on a fixed 5-minute schedule with no freshness gate ahead of `dbt_build`. A transaction that lands in Snowflake just before a run may not appear in the marts until the next run. See `docs/orchestration/airflow.md` for the discussion.
- **Source data quality:** raw PaySim data once contained NULLs that broke dbt `not_null` tests; invalid rows were removed from the source rather than quarantined. A future improvement is to split RAW into valid/quarantine paths.
- **Secrets** (`AWS_ACCESS_KEY_ID`, `SNOWFLAKE_PASSWORD`, etc.) live only in `.env` (gitignored) and are injected via `env_file` into the containers.

## Extending

The clean separation between layers means you can change each piece independently:

- **Add a source** → Kafka producer/consumer, S3 prefix, or Snowpipe changes
- **Add a model** → dbt `models/` (staging → intermediate → marts), then the Airflow `dbt_build` task runs it automatically
- **Change cadence** → the Airflow DAG `schedule` and/or task retries
- **Add an ingest service** → drop it under `services/ingest/` and declare it in `infra/docker/docker-compose.yml`
