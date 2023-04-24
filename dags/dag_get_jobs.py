from datetime import datetime, timedelta
import pendulum
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from extract.extract_utils import get_query_params, query_jobs_from_api
from s3.s3_utils import create_aws_conn_id, save_json_to_s3, archive_raw_data
from extract.api_targets import COUNTRIES_LIST
from snowflake.snow_utils import truncate_table, copy_from_stage, flatten_into_new_table


load_dotenv()

DAG_VERSION = "1.0.0"
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("S3_RAW_DATA_BUCKET_NAME")

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
    def save_aws_conn():
        create_aws_conn_id(
            aws_access_key=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
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

    # Select into flat raw table
    @task.python
    def flatten_data():
        flatten_into_new_table()

    # dbt run
    # TODO

    # TODO create 'clean up' section
    # truncate snowflake raw staging tables
    @task.python()
    def truncate_raw_data_table():
        truncate_table(table_name="raw_jobs_data")

    # archive raw data in s3
    @task.python()
    def archive(**context):
        """Archives imported data."""
        archive_raw_data(context)

    # Clear xcoms

    # Task dependencies
    (
        project_vars_to_xcom()
        >> save_aws_conn()
        >> save_to_s3(query_google_jobs(build_query_params()))
        >> copy_raw_data_from_stage()
        >> flatten_data()
        >> truncate_raw_data_table()
        >> archive()
    )


dag()
