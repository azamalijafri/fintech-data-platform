FROM apache/airflow:3.0.6

# Airflow containers mount host directories (dags, logs, plugins, dbt).
# The image's airflow user is uid 50000 by default; recreate it with the
# host UID so the container can write to host-owned mounts without sudo.
ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=0

USER root
RUN groupadd -g ${AIRFLOW_GID} airflow 2>/dev/null || true \
    && useradd -l -m -u ${AIRFLOW_UID} -g ${AIRFLOW_GID} -s /bin/bash airflow 2>/dev/null \
    || usermod -u ${AIRFLOW_UID} airflow
USER airflow

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake
