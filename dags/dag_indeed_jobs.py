from airflow.decorators import dag, task
from scrapy.crawler import CrawlerProcess
from webscrapers.indeed_jobs.indeed_jobs.spiders.indeeduk import (
    IndeedJobSpider,
)
from webscrapers.indeed_jobs.indeed_jobs.settings_B import scrapy_settings
from datetime import datetime, timedelta


# -------------- CONFIG
DAG_VERSION = "0.0.2"


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
        process = CrawlerProcess(scrapy_settings)
        process.crawl(IndeedJobSpider)
        process.start()

    run_scrapy_spider()


dag()
