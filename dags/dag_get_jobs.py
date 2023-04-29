from datetime import datetime, timedelta
import pendulum
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from extract.extract_utils import get_query_params, query_jobs_from_api
from s3.s3_utils import (
    create_aws_conn_id,
    create_snowflake_conn_id,
    save_json_to_s3,
    archive_raw_data,
)
from snow.snow_utils import copy_from_stage
from extract.api_targets import COUNTRIES_LIST
from cosmos.providers.dbt.task_group import DbtTaskGroup

load_dotenv()

DAG_VERSION = "1.0.1"

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("S3_RAW_DATA_BUCKET_NAME")
# Snowflake
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")
SNOW_DB = os.getenv("SNOW_DB")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA")
# dbt
DBT_ROOT_PATH = "/usr/local/airflow/dbt/dbt_proj"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/.local/bin/dbt"
# Google Jobs
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SEARCH_TERM = "data engineer"
MAX_PAGES_PER_COUNTRY = 2

# -------------- DAG
default_args = {
    "owner": "louis",
    "retries": 5,
    "start_date": pendulum.today("Australia/Sydney"),
    "provide_context": True,
}


@dag(
    dag_id=f"dag_get_jobs_v{DAG_VERSION}",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing"],
)
def dag():
    @task.python()
    def project_vars_to_xcom(**context):
        """Pushes variables used at various stages of the pipeline to Airflow
        xcoms for easy retrieval."""
        context["ti"].xcom_push(key="bucket_name", value=BUCKET_NAME)

    @task.python()
    def save_conn_ids():
        # AWS
        create_aws_conn_id(
            aws_access_key=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        # Snowflake
        create_snowflake_conn_id(
            account=SNOW_ACCOUNT,
            snow_user=SNOW_USER,
            password=SNOW_PASSWORD,
            database=SNOW_DB,
            db_schema=SNOW_SCHEMA,
        )

    @task.python()
    def build_query_params():
        params = get_query_params(
            api_key=SERPAPI_KEY,
            search_term=SEARCH_TERM,
            search_locations=COUNTRIES_LIST,
        )
        return params

    @task.python()
    def query_google_jobs(query_params, **context):
        data = query_jobs_from_api(
            query_params=query_params,
            max_pages_per_country=MAX_PAGES_PER_COUNTRY,
            context=context,
        )
        return data

    @task.python()
    def save_to_s3(data, **context):
        saved_s3_key = save_json_to_s3(
            data=data,
            timestamp=datetime.now().strftime("%Y%m%d-%H%M%S"),
            context=context,
        )
        context["ti"].xcom_push(
            key=f"saved_s3_key_for_{context['run_id']}", value=saved_s3_key
        )

    # copy to raw table
    @task.python()
    def copy_raw_data_from_stage(**context):
        target_file = context["ti"].xcom_pull(
            key=f"saved_filename_for_{context['run_id']}"
        )
        copy_from_stage(filename=target_file)

    # dbt
    dbt_tg = DbtTaskGroup(
        group_id="transform_data",
        dbt_project_name="dbt_proj",
        conn_id="snowflake_default",
        dbt_args={"dbt_executable_path": DBT_EXECUTABLE_PATH},
    )

    # TODO create 'clean up' section
    # truncate snowflake raw staging tables

    # archive raw data in s3
    @task.python()
    def archive(**context):
        """Archives imported data."""
        archive_raw_data(context)

    # Clear xcoms

    # Task dependencies
    (
        project_vars_to_xcom()
        >> save_conn_ids()
        >> save_to_s3(query_google_jobs(build_query_params()))
        >> copy_raw_data_from_stage()
        # >> truncate_raw_data_table()
        >> dbt_tg
        >> archive()
    )


dag()
