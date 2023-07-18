# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    job_title = scrapy.Field()
    company_name = scrapy.Field()
    job_location = scrapy.Field()
    salary = scrapy.Field()
    job_type = scrapy.Field()
    date_posted = scrapy.Field()
    job_description = scrapy.Field()
