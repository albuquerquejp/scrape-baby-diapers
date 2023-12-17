import scrapy
from scrapy import crawler
from ..items import FraldasItem

class AmazonSpider (scrapy.Spider):
    """
    Spider intended to recover data from Diapers on Amazon
    """

    name = 'amazon'
    HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
    urls = [
        'https://www.amazon.com.br/s?k=fralda',
          ]
    
    def start_requests(self):
        
        for url in self.urls:
            yield scrapy.Request(url, callback = self.parse, headers = self.HEADERS) 

    def parse(self, response, **kwargs):

        itens = FraldasItem()

        for product in response.css('div.sg-col-inner'):

            price_whole = product.css('span.a-price-whole::text').get()
            price_fraction = product.css('span.a-price-fraction::text').get()
            name = product.css('.a-size-base-plus.a-color-base.a-text-normal::text').get()
            try:
                link = product.css('.a-link-normal.s-no-outline').attrib['href']
            except KeyError as e:
                link = None
            
            full_prince = f"{price_whole},{price_fraction}"
            if name:
                itens['price']= full_prince,
                itens['product_name']=name,
                itens['link'] = f"https://www.amazon.com.br{link}"
                
                yield itens

        next_page = response.css('a.s-pagination-item.s-pagination-next.s-pagination-button.s-pagination-separator').attrib['href']
        if next_page is not None:
            try:
                next_page = response.urljoin(next_page)
                yield scrapy.Request(next_page, callback=self.parse, headers = self.headers)
            except AttributeError as e:
                print("No more pages left")
        

if __name__ == "__main__":

    process = crawler.CrawlerProcess()
    process.crawl(AmazonSpider)
    process.start()