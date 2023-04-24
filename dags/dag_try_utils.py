from airflow.decorators import dag, task_group, task
from datetime import timedelta
import pendulum
from extract.extract_utils import get_query_params


# -------------- DAG
default_args = {
    "owner": "me",
    "retries": 5,
    "start_date": pendulum.today("Australia/Sydney"),
    "provide_context": True,
}


@dag(
    dag_id=f"testname_dag_v1",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing"],
)
def dag():


    @task.python()
    def extract_Masterdata(**context):
        params = get_query_params(
            api_key="testKEY", search_term="TestTerm", search_locations=["TestCopuntry"]
        )
        print(params)

    extract_Masterdata()


dag()