import scrapy
from ..items import FraldasItem

class AmericanasSpider (scrapy.Spider):
    """
    Spider intended to recover data from Diapers on Americanas
    """

    name = 'americanas'

    start_urls = [
        'https://www.americanas.com.br/busca/fralda?limit=24&offset=0',
          ]

    def parse(self, response, **kwargs):

        itens = FraldasItem()

        if response.css('a.inStockCard__Link-sc-8xyl4s-1.ffLdXK'):
            for product in response.css('a.inStockCard__Link-sc-8xyl4s-1.ffLdXK'):

                price = product.css('span.src__Text-sc-154pg0p-0.price__PromotionalPrice-sc-i1illp-1.BCJl.price-info__ListPriceWithMargin-sc-z0kkvc-2.juBAtS::text').get().replace('R$', '')
                name = product.css('h3.product-name__Name-sc-1jrnqy1-0.kYncIC::text').get()
                try:
                    link = product.css('a.inStockCard__Link-sc-8xyl4s-1.ffLdXK::attr(href)').get()
                except KeyError as e:
                    link = None
                
                itens['price']= price,
                itens['product_name']=name,
                itens['link'] = f"https://www.americanas.com.br{link}"
                
                yield itens
                
        else: 
            response = None
        
        if response:
            next_page_url = self.get_next_page_url(response)
            yield scrapy.Request(next_page_url, callback=self.parse)
        else:
            print("No more pages left, scrape completed!")


    def get_next_page_url(self, response):
        offset = int(response.url.split("offset=")[1])
        next_offset = offset + 24
        next_page_url = f"https://www.americanas.com.br/busca/fralda?limit=24&offset={next_offset}"

        return next_page_url