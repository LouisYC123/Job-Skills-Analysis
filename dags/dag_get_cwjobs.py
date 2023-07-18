from airflow.decorators import dag, task
from snow.snow_utils import copy_from_stage
from webscrapers.spider_utils import start_spider
from datetime import datetime, timedelta
import os

# from webscrapers.cwjobs.cwjobs.spiders.cwjobs_spider import (
#     CWJobsSpider,
# )

# from webscrapers.cwjobs.cwjobs.settings_B import cwjobs_settings
from cosmos.task_group import DbtTaskGroup

# -------------- CONFIG
DAG_VERSION = "0.0.1"
BUCKET_NAME = os.getenv("S3_RAW_DATA_BUCKET_NAME")
JOBSITE = "cwjobs"
RAW_DATA_TABLE = f"{JOBSITE}_raw"

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
    dag_id=f"dag_{JOBSITE}_scrape_v{DAG_VERSION}",
    start_date=datetime(2023, 7, 18),
    schedule_interval=None,
    # schedule_interval="@weekly",  #  once a week at midnight on Sunday morning
    default_args=default_args,
    # catchup=True,
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
    def run_scrapy_spider(**context):
        output_filename = f"{JOBSITE}_jobs_{todays_date}.json"
        start_spider()
        context["ti"].xcom_push(
            key=f"saved_{JOBSITE}_filename_for_{context['run_id']}",
            value=output_filename,
        )

    (project_vars_to_xcom() >> run_scrapy_spider())


dag()
