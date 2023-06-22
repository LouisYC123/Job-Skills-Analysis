from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from scrapy.crawler import CrawlerProcess
from webscrapers.indeed_scraper.indeed_spider.indeed_scrapy_spider.spiders.indeed_uk import IndeedJobSpider
from datetime import datetime, timedelta
import pendulum

def run_scrapy_spider():
    process = CrawlerProcess()
    process.crawl(YourSpider)
    process.start()

# -------------- CONFIG
DAG_VERSION = "0.0.1"


# -------------- DAG
default_args = {
    "owner": "louis",
    "retries": 5,
    "provide_context": True,
}

@dag(
    dag_id=f"dag_indeed_scrape_v{DAG_VERSION}",
    start_date=datetime(2023, 6, 25),
    schedule_interval=None,
    # schedule_interval="@weekly",  #  once a week at midnight on Sunday morning
    default_args=default_args,
    catchup=True,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing"],
)

scrapy_task = PythonOperator(
    task_id='scrapy_task',
    python_callable=run_scrapy_spider,
    dag=dag
)
