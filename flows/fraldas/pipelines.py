# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from datetime import datetime


class FraldasPipeline:
    def process_item(self, item, spider):
        return item

class JsonWriterPipeline:
    def open_spider(self, spider):
        # Better to save on a cloud storage, for better scalability
        # Saving locally for test
        self.file = open(f"/root/lake/{spider.name}/data.json", "w") 

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False)+ "\n"
        self.file.write(line)
        return item