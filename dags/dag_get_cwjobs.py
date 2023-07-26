from datetime import datetime, timedelta
import os
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from snow.snow_utils import copy_from_stage
from webscrapers.spider_utils import run_spider
from snow.snow_utils import copy_from_stage
from s3.s3_utils import archive_raw_data

# -------------- CONFIG
DAG_VERSION = "1.0.0"
JOBSITE = "cwjobs"
RAW_DATA_TABLE = f"{JOBSITE}_raw"

BUCKET_NAME = os.getenv("S3_RAW_DATA_BUCKET_NAME")
S3_SUBFOLDER = "raw_jobs_data"
# -------------- DAG
default_args = {
    "owner": "louis",
    "retries": 5,
    "provide_context": True,
}


@dag(
    dag_id=f"dag_get_{JOBSITE}",
    start_date=datetime(2023, 7, 18),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
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
    def run_scrapy_spider(**context):
        todays_date = datetime.today().strftime("%Y-%m-%d")
        output_filename = f"{JOBSITE}_jobs_{todays_date}.json"
        run_spider(output_filename)
        context["ti"].xcom_push(
            key=f"saved_{JOBSITE}_filename_for_{context['run_id']}",
            value=output_filename,
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
        trigger_dag_id="dag_run_dbt",
    )

    (
        project_vars_to_xcom()
        >> run_scrapy_spider()
        >> copy_raw_data_from_stage()
        >> archive()
        >> trigger_next_dag
    )


dag()
