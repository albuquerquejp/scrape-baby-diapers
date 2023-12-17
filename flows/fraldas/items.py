# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

import json

class FraldasItem(scrapy.Item):
    # define the fields for your item here like:
    price = scrapy.Field()
    product_name = scrapy.Field()
    link = scrapy.Field()


