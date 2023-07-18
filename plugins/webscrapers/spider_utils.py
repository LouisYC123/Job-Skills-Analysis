from webscrapers.cwjobs.cwjobs.spiders.cwjobs_spider import (
    CWJobsSpider,
)
from webscrapers.cwjobs.cwjobs.settings_B import cwjobs_settings
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor

todays_date = "20230718"


def run_spider():
    configure_logging()
    runner = CrawlerRunner(cwjobs_settings)
    deferred = runner.crawl(CWJobsSpider)
    deferred.addBoth(lambda _: reactor.stop())


def start_spider():
    reactor.callWhenRunning(run_spider)
    reactor.run()
