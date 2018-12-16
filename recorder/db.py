from time import sleep

from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError
from loguru import logger


def create_table(config):
    while True:
        try:
            client = Client(config['db']['host'], config['db']['port'])
            client.execute(
                '''CREATE TABLE IF NOT EXISTS LOB(   
                                   symbol String,                   
                                   event_date DateTime,
                                   update_id UInt64,
                                   price Float64,
                                   amount Float64,
                                   is_bid UInt8
                                   ) 
                                   ENGINE = MergeTree() 
                                   PARTITION BY toYYYYMM(event_date)
                                   ORDER BY (symbol, event_date, update_id, price)'''
            )
        except NetworkError:
            logger.error('Clickhouse DB connection error. Retry after 5s...')
            sleep(5)
        else:
            break
