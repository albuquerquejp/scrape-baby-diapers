import os
from datetime import datetime
import json
import pandas as pd
from sqlalchemy.sql import text
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from prefect import task,flow
from twisted.internet import reactor
from fraldas.spiders import amazon, drogasil, americanas  # Import your Scrapy spider
from utils.util_postgres import PostGres

# Our application's data warehouse secrets 
DB_USER=os.environ['DW_USER']
DB_PASSWORD=os.environ['DW_PASSWORD']


@task(name='scrape_data')
def scrape_data():
    """Simple function to call scrappy crawlers."""
    settings = get_project_settings()

    configure_logging({"LOG_FORMAT": "%(levelname)s: %(message)s"})
    runner = CrawlerRunner(settings)

    amazon_spider = amazon.AmazonSpider
    drogasil_spider = drogasil.DrogasilSpider
    americanas_spider = americanas.AmericanasSpider
    runner.crawl(amazon_spider)
    runner.crawl(drogasil_spider)
    runner.crawl(americanas_spider)
    
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()  # the script will block here until the crawling is finished

    return [amazon_spider.name, drogasil_spider.name,americanas_spider.name]

@task(name="load_data_raw_schema", 
      description="This task will insert date into the raw schema, \
                   directly from the datalake for each spider",
      task_run_name="load_data_raw-{spider_name}-on-{date:%A}")
def load_data_raw_schema(spider_name, date):
    """Load the raw schema tables from data on the Datalake."""

    post_gres = PostGres('host.docker.internal',
                         DB_USER, DB_PASSWORD,
                         'babyproducts')
    postgres__engin = post_gres.engine()

    with open(f'/root/lake/{spider_name}/data.json', 'r') as f:
        data = [json.loads(line) for line in f]

    if data:
        for item in data:
            item['price'] = item.get('price')[0]
            item['product_name'] = item.get('product_name')[0]

        df = pd.DataFrame(data)
        if df['price'].dtype == object:
            df['price'] = df['price'].str.replace(',','.')
            df['price'] = pd.to_numeric(df['price'], errors='coerce')

        with postgres__engin.connect() as conn:
            df.to_sql(f'{spider_name}produto', conn, 'raw', if_exists='append', index=False)

@task(name="load_dimension_tables", 
      description="This task will insert date into the dimension table Diapers, \
                   on the DATA WAREHOUSE, for each spider's data on the lake",
      task_run_name="load_dimension_table-{spider_name}-on-{date:%A}")
def load_dim_diapers_name(spider_name, date):
    """Load data from raw schema into Diapers table.
    This table will have all the names of the products
    """
    insert_statement = f"""INSERT INTO Diapers (name, createdAt)
                            SELECT DISTINCT ON (R.product_name)
                                R.product_name AS name,
                                NOW() AS createdAt
                            FROM raw.{spider_name}produto AS R
                            LEFT JOIN Diapers AS D ON R.product_name = D.name
                            WHERE D.name IS NULL;"""
    
    post_gres = PostGres('host.docker.internal',DB_USER, DB_PASSWORD,'babyproducts')
    postgres__engin = post_gres.engine()

    with postgres__engin.connect() as conn:
        print(f"Inserting data from Spider '{spider_name}' to Diapers Table")
        conn.execute(text(insert_statement))
        conn.commit()
        

@task(name="load_fact_tables", 
      description="This task will insert date into \
                   the Fact table DiapersSales on the DATA WAREHOUSE",
      task_run_name="load_fact_tables-{spider_name}-on-{date:%A}")
def load_fact_tables(spider_name, date):
    """Load data from the dimension tables directly to the fact table"""

    insert_statement = f"""with prd_with_quantity as (
                            SELECT *,
                                CAST(COALESCE(
                                    SUBSTRING(lower(R.product_name) FROM 
                                            '([0-9]{{1,3}})\s*(unidades|un|uni|unids|u)'),
                                    SUBSTRING(lower(R.product_name) FROM 
                                            '(?<=(c|c)/)\s*\d{{1,3}}'),
                                    SUBSTRING(lower(R.product_name) FROM 
                                            '([0-9]{{1,3}})\s*(fraldas)')
                                ) AS INTEGER)as q
                                FROM raw.{spider_name}produto AS R
                            )
                            INSERT INTO DiapersSales (price, BrandId, SizeId, nameId, quantityId, createdAt)
                            SELECT 
                                R.price AS price, 
                                COALESCE(DB.id, (SELECT id FROM DiaperBrands WHERE description = 'Outros')) AS BrandId,
                                COALESCE(DS.id, (SELECT id FROM DiaperSizes WHERE description = 'Outros')) AS SizeId,
                                DN.id as nameId,
                                DQ.id as quantityid,
                                now() as createdAt
                            FROM  prd_with_quantity AS R
                            INNER JOIN Diapers as DN on R.product_name = DN.name
                            LEFT JOIN DiaperBrands AS DB ON 
                                (R.product_name ILIKE '%' || DB.description || '%' OR 
                                R.product_name ILIKE '%' || REPLACE(DB.description, 'Monica', 'Mônica') || '%')
                            LEFT JOIN DiaperSizes AS DS ON 
                                (R.product_name ~* ('\y' || DS.description || '\y'))
                            LEFT JOIN DiapersQuantity as DQ on 
							    R.q = dq.Quantity
                            WHERE r.price is not null;"""
    
    post_gres = PostGres('host.docker.internal',
                         DB_USER, 
                         DB_PASSWORD,
                         'babyproducts')
    postgres__engin = post_gres.engine()

    with postgres__engin.connect() as conn:
        print(f"Inserting Data Into DiapersSales, from Spider {spider_name}")
        conn.execute(text(insert_statement))
        conn.commit()


@flow(name='retrive_data', log_prints=True)
def run_fraldas_pipeline():
    spiders_name = scrape_data()
    for name in spiders_name:
        print(f"Loading data from Spider: {name}")
        load_data_raw_schema(spider_name=name, date=datetime.utcnow())
        load_dim_diapers_name(spider_name=name, date=datetime.utcnow())
        load_fact_tables(spider_name=name, date=datetime.utcnow())

if __name__ == "__main__":
    run_fraldas_pipeline.serve(name='retrive_data_v3', cron='5 4 * * *')