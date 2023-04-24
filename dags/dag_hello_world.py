
# %%-------------- IMPORTS
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from extract import extract_utils
# --- PROJECT VARIABLES

PROJECT_NAME = 'hello_world'
DAG_AUTHOR = "Louis"
DAG_VERSION = "1.0.1"
AIRFLOW_ENV = 'local' # local | mwaa

# -------------- DAG

def print_msg(message: str):
    """simple print message function
    """
    print(message)
    return


default_args = {
    "owner": f"{DAG_AUTHOR}_dev",
    "retries": 0,
    'start_date': days_ago(0),
}

with DAG(
    dag_id=f'{PROJECT_NAME}_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    run_id = '{{ run_id }}'
    import_date = '{{ ds }}'
    import_timestamp = '{{ ts_nodash }}'

    start = DummyOperator(task_id='start')

    print_msg = PythonOperator(
        task_id='print_message',
        python_callable=print_msg,
        op_kwargs={
            'message': 'Hello World!'
        },
    )

    end = DummyOperator(task_id='end')

    start >> print_msg >> end
