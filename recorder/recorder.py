import asyncio
import traceback
from typing import Dict, List, Any

from aioch import Client as CHClient
from binance.client_async import AsyncClient as BClient
from loguru import logger

from recorder.depth_cache import DepthCache
from recorder.depth_cache import MultiplexDepthCacheManager


class LOBRecorder:
    def __init__(self, loop: asyncio.AbstractEventLoop, config: Dict[str, Any]) -> None:
        self._loop = loop
        self.config = config
        self.client = BClient(
            self.config['exchange']['api_key'], self.config['exchange']['api_secret']
        )

    async def run(self) -> None:
        symbols = await self.get_symbols()
        logger.info(f'Watching symbols: {symbols}')
        mdcm = MultiplexDepthCacheManager(
            self.client,
            self._loop,
            symbols,
            coro=self.process_depth,
            coro_throttle_count=self.config['db']['throttle_count'],
        )
        logger.info('Connecting to Binance ws endpoint')
        await mdcm.connect()

    async def get_symbols(self) -> List[str]:
        response = await self.client.get_exchange_info()
        symbols = [
            s['symbol']
            for s in response['symbols']
            if s['symbol'].endswith('BTC')
            and not s['symbol'] in self.config['exchange']['pair_blacklist']
        ]
        return symbols

    async def process_depth(self, depth_cache: DepthCache) -> None:
        """
        Websocket event callback coroutine
        :param depth_cache:
        :return:
        """
        logger.info(f'Depth cache update for symbol: {depth_cache.symbol}')
        try:
            client = CHClient(self.config['db']['host'], self.config['db']['port'])
            if depth_cache is not None:
                orders = depth_cache.get_orders(self.config['db']['price_limit'])
                await client.execute(
                    'INSERT INTO LOB (price, amount, is_bid, symbol, event_date, update_id) VALUES',
                    orders,
                )
                logger.info(f'Number of records inserted: {len(orders)} ')
                await client.disconnect()
        except Exception:
            logger.error(traceback.format_exc())
