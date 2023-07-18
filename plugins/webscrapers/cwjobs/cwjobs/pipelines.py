# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re


CLEANR = re.compile("<.*?>")


def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, "", raw_html)
    cleantext = cleantext.replace("\n", "")
    # Clean extra whitespace into a single space
    cleantext = re.sub(r"\s+", " ", cleantext)
    return cleantext.strip()


class CwjobsScraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip tags from text
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != "job_description":
                if adapter[field_name]:
                    adapter[field_name] = cleanhtml(adapter.get(field_name))
        return item
