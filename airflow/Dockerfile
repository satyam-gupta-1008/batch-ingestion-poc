FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.5}
USER airflow

RUN pip install "apache-airflow-providers-airbyte[http]==3.0.0"

RUN pip install astronomer-cosmos==1.11.0a1
# should not use pre-release version in production

RUN pip install --no-cache-dir --upgrade dbt-core==1.8.2 dbt-postgres==1.8.2