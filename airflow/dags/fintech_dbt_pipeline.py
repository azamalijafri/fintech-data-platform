from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from fintech.tasks import start_pipeline


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="fintech_dbt_pipeline",
    default_args=default_args,
    description="Run dbt transformations for the FinTech transaction pipeline",
    start_date=datetime(2026, 8, 31),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["fintech", "dbt", "snowflake"],
) as dag:

    start_pipeline_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt build --profiles-dir /opt/airflow/dbt"
        ),
    )

    start_pipeline_task >> dbt_build
