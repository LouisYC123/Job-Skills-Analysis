from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from scrapy.crawler import CrawlerProcess
from webscrapers.indeed_scraper.indeed_scrapy_spider.spiders.indeed_uk import (
    IndeedJobSpider,
)
from scrapy.utils.project import get_project_settings
from datetime import datetime, timedelta
import pendulum


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
    start_date=datetime(2023, 7, 2),
    schedule_interval=None,
    # schedule_interval="@weekly",  #  once a week at midnight on Sunday morning
    default_args=default_args,
    catchup=True,
    dagrun_timeout=timedelta(hours=1),
    tags=["testing"],
)
def dag():
    @task.python()
    def run_scrapy_spider():
        process = CrawlerProcess(
            settings={
                "FEED_URI": "s3://lg-job-skills-data-lake/raw_jobs_data/indeed_jobs/test_(time).json",
                "FEED_FORMAT": "json",
            }
        )
        process.crawl(IndeedJobSpider)
        process.start()

    run_scrapy_spider()


dag()
