import scrapy
import json
import pandas as pd 
from ..items import FraldasItem

class DrogasilSpider (scrapy.Spider):
    """
    Spider intended to recover data from Diapers on Drogasil.
    Most of the data is dynamically generated, so it is necessary
    to scrape from the API responses, not by CSS Selectors
    """
    name = 'drogasil'
    page = 1
    limit = 48
    offset=0
    start_urls = [
        'https://api-gateway-prod.drogasil.com.br/search/v2/store/DROGASIL/channel/SITE/product/search?term=fraldas&origin=undefined&p=1&limit=48&offset=0&sort_by=relevance:desc',
          ]

    def parse(self, response):
        itens = FraldasItem()

        data = json.loads(response.text)
        for products in data["results"]["products"]:
            
            itens['price']= products["valueTo"],
            itens['product_name']=products["name"],
            itens['link'] = "".join(products["urlKey"].split('/')[-2:])
                
            yield itens
            
        if  data["metadata"]["links"]["next"]:
            self.page += 1
            self.offset += 48
            url = f"https://api-gateway-prod.drogasil.com.br/search/v2/store/DROGASIL/channel/SITE/product/search?term=fraldas&origin=undefined&p={self.page}&limit={self.limit}&offset={self.offset}&sort_by=relevance:desc"
            yield scrapy.Request(url=url, callback=self.parse)
        