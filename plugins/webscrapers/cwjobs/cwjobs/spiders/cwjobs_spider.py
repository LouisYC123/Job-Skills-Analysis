import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from webscrapers.cwjobs.cwjobs.items import JobItem
from dotenv import load_dotenv
import os

load_dotenv()
PROXY = os.getenv("PROXY")


class CWJobsSpider(CrawlSpider):
    name = "cwjobs_spider"
    allowed_domains = ["www.cwjobs.co.uk"]
    search_term = "data-engineer"

    def start_requests(self):
        yield scrapy.Request(
            "https://www.cwjobs.co.uk/jobs/data-engineer/in-united-kingdom?radius=30&postedWithin=7&q=%22Data+Engineer%22",
        )

    rules = (
        Rule(  # follow all links and parse the content
            # This was the old one, but was extracting company logo links as well:
            # LinkExtractor(restrict_xpaths='//*[@data-at="job-item"]'), # <-
            LinkExtractor(
                restrict_xpaths='//a[@class="res-1xuicga"]'
            ),  # <- This xpath could prove brittle
            callback="parse_job",
            follow=True,
        ),
    )

    def parse_job(self, response):
        job_item = JobItem()
        job_title = response.xpath("//div[@class='row title']/h1/text()").get()
        if not job_title:
            job_title = response.xpath(
                "//div[@class='job-content-top']//h1/text()"
            ).get()
        job_location = response.xpath(
            "//section[@class='job-summary']//li[@class='location icon']/div/text()"
        ).get()
        if not job_location or job_location in [",", ", ", ",  "]:
            job_location = "London"
        salary = response.xpath(
            "//section[@class='job-summary']//li[@class='salary icon']/div/text()"
        ).get()
        job_type = response.xpath(
            "//section[@class='job-summary']//li[@class='job-type icon']/div/text()"
        ).get()
        company_name = response.xpath("//a[@id='companyJobsLink']/text()").get()
        date_posted = response.xpath(
            "//section[@class='job-summary']//li[@class='date-posted icon']/div/span/text()"
        ).get()

        job_description = response.xpath(
            '//*[@class="job-description"]//text()'
        ).getall()
        clean_descriptions = "".join(
            [desc.strip() for desc in job_description if desc.strip()]
        )

        job_item["job_title"] = job_title
        job_item["company_name"] = company_name
        job_item["country"] = "UK"
        job_item["job_location"] = job_location
        job_item["salary"] = salary
        job_item["job_type"] = job_type
        job_item["date_posted"] = date_posted
        job_item["url"] = response.url
        job_item["job_description"] = clean_descriptions

        yield job_item
