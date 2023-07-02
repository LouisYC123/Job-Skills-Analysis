import re
import json
import scrapy
from webscrapers.indeed_scraper.indeed_scrapy_spider.items import JobItem

"""scraped 53 jobs (posted in last 7 days) in 20 minutes"""


class IndeedJobSpider(scrapy.Spider):
    name = "indeeduk"
    keyword_list = ["Data+Engineer"]
    location_list = ["London"]
    days_ago = 7
    max_jobs = 200

    def get_indeed_search_url(self, keyword, location):
        """Gets url with encoded search parameters"""
        return f"https://uk.indeed.com/jobs?q=Title%3A+%22{keyword}%22&l={location}&fromage={str(self.days_ago)}"

    def start_requests(self):
        """Request jobs from each keyword and location url and call
        parse_search_results on response"""
        for keyword in self.keyword_list:
            for location in self.location_list:
                yield scrapy.Request(
                    url=self.get_indeed_search_url(keyword, location),
                    callback=self.parse_search_results,
                    meta={"keyword": keyword, "location": location, "offset": 0},
                )

    def parse_search_results(self, response):
        location = response.meta["location"]
        keyword = response.meta["keyword"]
        offset = response.meta["offset"]
        script_tag = re.findall(
            r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});',
            response.text,
        )
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])

            ## Extract Jobs From Search Page
            jobs_list = json_blob["metaData"]["mosaicProviderJobCardsModel"]["results"]
            for index, job in enumerate(jobs_list):
                if job.get("jobkey") is not None:
                    job_url = (
                        "https://www.indeed.com/m/basecamp/viewjob?viewtype=embedded&jk="
                        + job.get("jobkey")
                    )
                    yield scrapy.Request(
                        url=job_url,
                        callback=self.parse_job,
                        meta={
                            "keyword": keyword,
                            "location": location,
                            "page": round(offset / 10) + 1 if offset > 0 else 1,
                            "position": index,
                            "jobKey": job.get("jobkey"),
                        },
                    )
            # Paginate Through Jobs Pages
            if offset == 0:
                num_results = self.max_jobs
                for offset in range(10, num_results + 10, 10):
                    yield scrapy.Request(
                        url=self.get_indeed_search_url(keyword, location)
                        + f"&start={offset}",
                        callback=self.parse_search_results,
                        meta={
                            "keyword": keyword,
                            "location": location,
                            "offset": offset,
                        },
                    )

    def parse_job(self, response):
        page = response.meta["page"]
        script_tag = re.findall(r"_initialData=(\{.+?\});", response.text)
        job_item = JobItem()
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])
            jobInfoModel = json_blob["jobInfoWrapperModel"]["jobInfoModel"]
            job_item["url"] = response.url
            job_item["page"] = page
            job_item["job_title"] = jobInfoModel["jobInfoHeaderModel"].get("jobTitle")
            job_item["company_name"] = jobInfoModel["jobInfoHeaderModel"].get(
                "companyName"
            )
            job_item["job_location"] = json_blob.get("jobLocation")
            job_item["job_type"] = jobInfoModel["jobMetadataHeaderModel"].get("jobType")
            job_item["jobkey"] = response.meta["jobKey"]
            job_item["date_posted"] = json_blob["hiringInsightsModel"].get("age")
            job_item["jobDescription"] = jobInfoModel["sanitizedJobDescription"].get(
                "content"
            )

            yield job_item
