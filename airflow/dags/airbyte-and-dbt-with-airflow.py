from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path
import uuid
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig, RenderConfig, LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping
import requests
import psycopg2

AIRBYTE_CONN_ID = "airbyte-with-airflow"  # connection in Airflow UI - manually created
AIRBYTE_API_URL = "http://host.docker.internal:8001/api/v1"

# --- Paths inside container ---
dbt_project_path = Path("/opt/airflow/dbt/my_project_dbt")
dbt_executable = Path("/home/airflow/.local/bin/dbt")  # venv dbt

# --- Profile Config (Postgres external DB) ---
dbt_profile = ProfileConfig(
    profile_name="dbt_postgres_profile",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="test-airbyte-and-dbt-with-airflow",
        profile_args={"schema": "dbt"},
    ),
)

def create_custom_source_and_discover_schema(**context):
    dag_run_conf = context["dag_run"].conf
    unique_source_name = dag_run_conf.get("source_name", "Daterange_Partitioned_source") + "-" + str(uuid.uuid4())[:8]

    http_source_definition_id = dag_run_conf.get(
        "sourceDefinitionId",
        "8b5b7a4c-3a5f-4b06-8d03-5f953f077b55"  # constant custom connector built -> source definition Id 
    )

    payload = {
        "name": unique_source_name,
        "workspaceId": dag_run_conf["workspaceId"],
        "sourceDefinitionId": http_source_definition_id,
        "connectionConfiguration": {
            "__injected_declarative_manifest": {},
            "__injected_components_py_checksums": {},
            "api_key": "eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.B8dyEsq10xhvpkj6GXHu-A0yxwfWV99QCp1B1xL9evRUUL_O_99ENQ.QA82olA_-337vqP_.NH07DwKS26c-MSzkkIt_GANyBxjwOW9ASdZKM0AiW_dv6lpNz15TqeSTTcj0jQUZCE-PVbCdJR38DySlrxPhyaOHUnKabU9kmVaKOwQOLzpePRkJDEuhBA4C70YmqtJ8HAbGViJlTGU1tjU8M8qIogUN8VlBRUPT9k-jLsiEUPuijxx_OBPT_gmd41rwSyEiDgrCsMFZZGhuBkiQ5VsHK-jt7d84oRoFzCW-TAiDZCFG7GmDB3xutqcjMA9oJdxTR14u5Hv_KJaATNCguHFqA5JJppbu7p0gb85P8_ZS5SngHVwbuZxpbR8TrrmyUDv62jsfJR0myTXbhXY6zZKaJcPN1Ho.p1ExvW_kLs8w7fV-ZyU2ow",
            "start_date": dag_run_conf.get("start_date"),
            "end_date": dag_run_conf.get("end_date"),
        }
    }

    resp = requests.post(f"{AIRBYTE_API_URL}/sources/create", json=payload)
    resp.raise_for_status()
    source_id = resp.json()["sourceId"]

    resp = requests.post(
        f"{AIRBYTE_API_URL}/sources/discover_schema",
        json={"sourceId": source_id},
    )
    if not resp.ok:
      elf.log.info("Airbyte API error:", resp.status_code, str(resp.text))
    resp.raise_for_status()

    response = resp.json()
    if "catalog" not in response:
        raise AirflowException(f"Airbyte discovery failed, response={response}")

    catalog = response["catalog"]
    context["ti"].xcom_push(key="source_id", value=source_id)
    context["ti"].xcom_push(key="discovered_catalog", value=catalog)

def create_destination(**context):
    dag_run_conf = context["dag_run"].conf
    unique_destination_name = dag_run_conf.get("destination_name", "new_destination") + "-" + str(uuid.uuid4())[:8]
    
    payload = {
        "name": unique_destination_name,
        "workspaceId": dag_run_conf["workspaceId"],
        "destinationDefinitionId": dag_run_conf.get(
            "destinationDefinitionId",
            "25c5221d-dce2-4163-ade9-739ef790f503"  # constant default Postgres definition
        ),
        "connectionConfiguration": {
            "host": dag_run_conf["host"],
            "port": dag_run_conf.get("port", 5432),
            "database": dag_run_conf["database"],
            "username": dag_run_conf["username"],
            "password": dag_run_conf["password"],
            "schema": dag_run_conf.get("schema", "public"),
            "ssl": dag_run_conf.get("ssl", False),
            "drop_cascade": True
        },
    }

    resp = requests.post(f"{AIRBYTE_API_URL}/destinations/create", json=payload)
    resp.raise_for_status()
    destination_id = resp.json()["destinationId"]

    context["ti"].xcom_push(key="destination_id", value=destination_id)


# --- Sensor Operator for Airbyte Jobs ---
class AirbyteConnectionCreator(BaseSensorOperator):
    connection_id = None  # Store the created connection ID
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_airbyte_connection(self, context):
        dag_run_conf = context["dag_run"].conf
        catalog = context["ti"].xcom_pull(key="discovered_catalog", task_ids="create_custom_source_and_discover_schema")
        source_id = context["ti"].xcom_pull(key="source_id", task_ids="create_custom_source_and_discover_schema")
        destination_id = context["ti"].xcom_pull(key="destination_id", task_ids="create_destination")

        payload = {
          "sourceId": source_id,
          "destinationId": destination_id,
          "name": "[Airflow 1.6] Custom Source → Postgres",
          "nonBreakingChangesPreference": "propagate_columns",
          "geography": "AUTO",
          "syncCatalog": catalog,
          "prefix": dag_run_conf.get("table_prefix", ""), #appended to stream name to get final table name in destination
          "notifySchemaChanges": "true",
          "backfillPreference": "disabled",
          "tags": [],
          "scheduleType": "basic",
          "scheduleData": { "basicSchedule": { "units": 24, "timeUnit": "hours" } },
          "status": "active",
        }


        resp = requests.post(
            f"{AIRBYTE_API_URL}/connections/create",
            json=payload
        )
        resp.raise_for_status()
        AirbyteConnectionCreator.connection_id = resp.json()["connectionId"]

        context["ti"].xcom_push(key="airbyte_connection_id", value=AirbyteConnectionCreator.connection_id)
        self.log.info("✅ Created Airbyte connection: %s", AirbyteConnectionCreator.connection_id)

    def poke(self, context):
        if not AirbyteConnectionCreator.connection_id:
            self.create_airbyte_connection(context)
            return False  # First time, just created the connection, return False to poke again
        else:
            self.log.info("Airbyte connection already created: %s", AirbyteConnectionCreator.connection_id)
            resp = requests.post(
                f"{AIRBYTE_API_URL}/jobs/list",
                json={
                    "configId": AirbyteConnectionCreator.connection_id,
                    "configTypes": ["sync", "reset_connection"]
                },
            )
            if not resp.ok:
              self.log.info("Airbyte API error:", resp.status_code, str(resp.text))
            resp.raise_for_status()
            jobs = resp.json().get("jobs", [])
            context["ti"].xcom_push(key="jobs", value=jobs)

            running_jobs = [j for j in jobs if j.get("job", {}).get("status") == "running"]

            if running_jobs:
                self.log.info("Job(s) still running for connection %s", AirbyteConnectionCreator.connection_id)
                return False
            else:
                self.log.info("No running jobs for connection %s. Proceeding.", AirbyteConnectionCreator.connection_id)
                return True    


with DAG(
    "airbyte_dynamic_connection",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    create_custom_source_and_discover_schema = PythonOperator(
        task_id="create_custom_source_and_discover_schema",
        python_callable=create_custom_source_and_discover_schema,
    )

    create_destination = PythonOperator(
        task_id="create_destination",
        python_callable=create_destination,
    )

    create_airbyte_connection = AirbyteConnectionCreator(
        task_id="create_airbyte_connection",
        poke_interval=20,  # poll every 20s
        timeout=600,       # fail if busy >10min
    )

    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id="{{ ti.xcom_pull(task_ids='create_airbyte_connection', key='airbyte_connection_id') }}",
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    perform_dbt_tasks = DbtTaskGroup(
        group_id="dbt_dynamic_alias",
        project_config=ProjectConfig(dbt_project_path=dbt_project_path),
        profile_config=dbt_profile,
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
        ),
        operator_args={
            "vars": {
                "source_name": "airbyte",
                "table_name": "{{ dag_run.conf.get('table_prefix', 'September_5th_') ~ 'weekly_split_with_full_refresh_test' }}",
            },
        },
    )

[create_custom_source_and_discover_schema, create_destination] >> create_airbyte_connection >> trigger_sync >> perform_dbt_tasks

