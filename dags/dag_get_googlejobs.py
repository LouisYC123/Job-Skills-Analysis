from datetime import datetime, timedelta
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
from extract.api_targets import COUNTRIES
from airflow.operators.dagrun_operator import TriggerDagRunOperator

load_dotenv()

# -------------- CONFIG
DAG_VERSION = "1.1.0"
JOBSITE = "google_jobs"

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("S3_RAW_DATA_BUCKET_NAME")
S3_SUBFOLDER = "raw_jobs_data"
# Snowflake
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")
SNOW_DB = os.getenv("SNOW_DB")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA")
SNOW_CONN_ID = "snowflake_default"

# Google Jobs
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SEARCH_TERM = "data engineer"
MAX_PAGES_PER_COUNTRY = 10  # Temp setting, use 10 in prod

RAW_DATA_TABLE = f"{JOBSITE}_raw"

# -------------- DAG
default_args = {
    "owner": "louis",
    "retries": 5,
    "provide_context": True,
}


@dag(
    dag_id=f"dag_get_googlejobs",
    start_date=datetime(2023, 7, 22),
    schedule_interval="@weekly",  #  once a week at midnight on Sunday morning
    default_args=default_args,
    catchup=True,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing", f"v{DAG_VERSION}"],
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
    def query_google_jobs(**context):
        result_list = []
        for country_params in COUNTRIES.values():
            query_params = get_query_params(
                api_key=SERPAPI_KEY,
                search_term=SEARCH_TERM,
                location=country_params,
            )

            result = query_jobs_from_api(
                query_params=query_params,
                max_num_pages=MAX_PAGES_PER_COUNTRY,
                context=context,
            )
            result_list.append(result)

        data = {
            "run_id": context["run_id"],
            "extract_date": datetime.now().strftime("%Y%m%d-%H%M%S"),
            "data": [item for sublist in result_list for item in sublist],
        }
        saved_s3_key = save_json_to_s3(
            data=data,
            timestamp=datetime.now().strftime("%Y%m%d-%H%M%S"),
            jobsite=JOBSITE,
            s3_subfolder=S3_SUBFOLDER,
            context=context,
        )
        context["ti"].xcom_push(
            key=f"saved_{JOBSITE}_filename_for_{context['run_id']}",
            value=saved_s3_key,
        )

    @task.python()
    def copy_raw_data_from_stage(**context):
        target_file = context["ti"].xcom_pull(
            key=f"saved_{JOBSITE}_filename_for_{context['run_id']}"
        )
        copy_from_stage(filename=target_file, target_table=RAW_DATA_TABLE)

    @task.python()
    def archive(**context):
        """Archives imported data."""
        archive_raw_data(
            bucket_name=context["ti"].xcom_pull(key="bucket_name"),
            subfolder=S3_SUBFOLDER,
            target_file=context["ti"].xcom_pull(
                key=f"saved_{JOBSITE}_filename_for_{context['run_id']}"
            ),
        )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="dag_get_cwjobs",
    )

    # TODO truncate snowflake raw staging tables

    # Task dependencies
    (
        project_vars_to_xcom()
        >> save_conn_ids()
        >> query_google_jobs()
        >> copy_raw_data_from_stage()
        >> archive()
        >> trigger_next_dag
    )


dag()
