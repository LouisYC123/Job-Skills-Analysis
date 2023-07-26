from airflow.decorators import dag
from datetime import datetime, timedelta
from cosmos.task_group import DbtTaskGroup

# -------------- CONFIG
DAG_VERSION = "0.0.1"

# dbt
DBT_PROJECT_NAME = "dbt_proj"
DBT_ROOT_PATH = "/usr/local/airflow/dbt"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/.local/bin/dbt"
SNOW_CONN_ID = "snowflake_default"

todays_date = datetime.today().strftime("%Y-%m-%d")
# -------------- DAG
default_args = {
    "owner": "louis",
    "retries": 5,
    "provide_context": True,
}


@dag(
    dag_id="dag_run_dbt",
    start_date=datetime(2023, 7, 2),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing", f"v{DAG_VERSION}"],
)
def dag():

    dbt = DbtTaskGroup(
        group_id="transform_data",
        dbt_project_name=DBT_PROJECT_NAME,
        conn_id=SNOW_CONN_ID,
        dbt_root_path=DBT_ROOT_PATH,
        dbt_args={"dbt_executable_path": DBT_EXECUTABLE_PATH},
    )

    dbt


dag()
