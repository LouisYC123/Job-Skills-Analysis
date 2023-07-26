from webscrapers.cwjobs.cwjobs.spiders.cwjobs_spider import (
    CWJobsSpider,
)
from webscrapers.cwjobs.cwjobs.settings_B import cwjobs_settings
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor


def run_spider(output_filename):

    cwjobs_settings["FEEDS"] = {
        f"s3://lg-job-skills-data-lake/raw_jobs_data/{output_filename}": {
            "format": "json",
        }
    }

    def setup_spider():
        configure_logging()
        runner = CrawlerRunner(cwjobs_settings)
        deferred = runner.crawl(CWJobsSpider)
        deferred.addBoth(lambda _: reactor.stop())

    def start_spider():
        reactor.callWhenRunning(setup_spider)
        reactor.run()

    start_spider()
