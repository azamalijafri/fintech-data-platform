# dbt project — FinTech Data Platform

dbt models for transforming raw Snowflake transaction data into analytics-ready marts.

## Structure

```
models/
├── staging/       # stg_transactions — clean, typed view over RAW.TRANSACTIONS
├── intermediate/  # int_transactions — derived fields (fraud_status, amount category)
└── marts/         # fct_fraud_transactions — aggregated fraud fact counts/metrics
macros/            # generate_schema_name
profiles.yml       # Snowflake connection (env-var driven)
```

## Configuration

Connection values come from environment variables (see `profiles.yml`). Set the same variables used by the rest of the platform:

- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

## Usage

Run from the repo root (uses `.venv` if present, or your own dbt install):

```bash
# build models + run tests interleaved
make dbt-build

# data tests only
make dbt-test

# or directly
cd dbt
dbt build --profiles-dir .
```

## Deployment

In production this project is mounted into the Airflow scheduler container at
`/opt/airflow/dbt` and run by the `dbt_build` task of the `fintech_dbt_pipeline` DAG.
