import asyncio
import time
from datetime import datetime
from operator import itemgetter
from typing import Callable, Dict, List, Union, Optional, Any

import pandas as pd
from binance.client_async import AsyncClient
from binance.websockets_async import BinanceSocketManager
from loguru import logger


class DepthCache(object):
    """DepthCache

    """

    def __init__(self, client: AsyncClient, symbol: str) -> None:
        self._client = client
        self.symbol = symbol
        self._updating = False
        self.last_updated_id = None
        self.timestamp = None
        self._bids = {}
        self._asks = {}
        self.throttle_counter = 0

    async def init_from_rest(self) -> None:
        self.last_updated_id = None
        self._updating = True

        res = await self._client.get_order_book(symbol=self.symbol, limit='500')

        # process bid and asks from the order book
        for bid in res['bids']:
            self.add_bid(bid)
        for ask in res['asks']:
            self.add_ask(ask)

        # set first update id
        self._updating = False
        self.last_updated_id = res['lastUpdateId']
        self.timestamp = datetime.timestamp(datetime.now())

    async def update(self, msg: Dict[str, Any]) -> bool:
        """Update cache using websocket diff depth message
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly

        :param msg:
        :return:
        """
        # wait initial update
        while self._updating:
            await asyncio.sleep(1)

        if msg['u'] <= self.last_updated_id:
            # ignore any updates before the initial update id
            return False
        elif msg['U'] != self.last_updated_id + 1:
            # if not buffered check we get sequential updates
            # otherwise init cache again
            await self.init_from_rest()

        # add any bid or ask values
        for bid in msg['b']:
            self.add_bid(bid)
        for ask in msg['a']:
            self.add_ask(ask)

        self.last_updated_id = msg['u']
        self.timestamp = msg['E']
        return True

    def add_bid(self, bid: List[str]) -> None:
        """Add a bid to the cache

        :param bid:
        :return:
        """
        self._bids[bid[0]] = float(bid[1])
        if bid[1] == "0.00000000":
            del self._bids[bid[0]]

    def add_ask(self, ask: List[str]) -> None:
        """Add an ask to the cache

        :param ask:
        :return:
        """
        self._asks[ask[0]] = float(ask[1])
        if ask[1] == "0.00000000":
            del self._asks[ask[0]]

    def get_bids(self) -> List[List[float]]:
        """Get the current bids

        :return: list of bids with price and quantity as floats

        .. code-block:: python

            [
                [
                    0.0001946,  # Price
                    45.0        # Quantity
                ],
            ]

        """
        return DepthCache.sort_depth(self._bids, reverse=True)

    def get_asks(self) -> List[List[float]]:
        """Get the current asks

        :return: list of asks with price and quantity as floats

        .. code-block:: python

            [
                [
                    0.0001955,  # Price
                    57.0'       # Quantity
                ],
            ]

        """
        return DepthCache.sort_depth(self._asks, reverse=False)

    def get_orders(
        self, price_limit: float = 1
    ) -> List[List[Union[float, int, str, datetime]]]:
        """Get LOB cache limited by percent current price

        :param price_limit
        :return:
        """
        columns = ['price', 'amount']

        bids = pd.DataFrame(self.get_bids(), columns=columns)
        asks = pd.DataFrame(self.get_asks(), columns=columns)
        bids['is_bid'] = 1
        asks['is_bid'] = 0

        percent_bid = (1 - bids["price"].min() / bids["price"].max()) * 100
        percent_ask = (1 - asks["price"].min() / asks["price"].max()) * 100
        logger.debug(f'Bottom bid: {percent_bid:.2f} Top ask: {percent_ask:.2f}')

        bids = bids.loc[bids['price'] > bids['price'].max() * (1 - price_limit)]
        asks = asks.loc[asks['price'] < asks['price'].min() * (1 + price_limit)]
        orders = pd.concat([bids, asks])
        orders['symbol'] = self.symbol
        logger.debug(f'Orders in current price limit: {len(orders)} ')

        orders_list = [
            x + [datetime.fromtimestamp(self.timestamp / 1000), self.last_updated_id]
            for x in list(map(list, orders.itertuples(index=False)))
        ]

        return orders_list

    @staticmethod
    def sort_depth(vals: Dict[str, float], reverse: bool = False) -> List[List[float]]:
        """Sort bids or asks by price
        """
        lst = [[float(price), quantity] for price, quantity in vals.items()]
        lst = sorted(lst, key=itemgetter(0), reverse=reverse)
        return lst


class MultiplexDepthCacheManager(object):
    """
    Connect to multiple depth streams with one ws connection

    """

    _default_refresh = 60 * 30  # 30 minutes

    def __init__(
        self,
        client: AsyncClient,
        loop: asyncio.AbstractEventLoop,
        symbols: List[str],
        coro: Optional[Callable] = None,
        coro_throttle_count: int = 0,
        refresh_interval: int = _default_refresh,
    ) -> None:
        """
        :param client: Binance API client
        :type client: binance.Client
        :param loop: Event loop
        :type loop:
        :param symbol: Symbol to create depth cache for
        :type symbol: string
        :param coro: Optional coroutine to receive depth cache updates
        :type coro: async coroutine
        :param coro_throttle_count: Optional throttling coroutine calls
        :type coro_throttle_count: int
        :param refresh_interval: Optional number of seconds between cache refresh, use 0 or None to disable
        :type refresh_interval: int

        """
        self._client = client
        self._loop = loop
        self._symbols = symbols
        self._coro = coro
        self._coro_throttle_count = coro_throttle_count
        self._depth_cache = {
            symbol: DepthCache(client, symbol) for symbol in self._symbols
        }
        self._depth_message_buffer = []
        self._bm = None
        self._refresh_interval = refresh_interval
        self._refresh_time = None

    async def connect(self) -> None:
        await self._start_socket()

        await self._init_cache()

    async def _init_cache(self) -> None:
        """Initialise the depth cache calling REST endpoint

        :return: None
        """
        self._depth_message_buffer = []

        for symbol in self._symbols:
            await self._depth_cache[symbol].init_from_rest()

        # set a time to refresh the depth cache
        if self._refresh_interval:
            self._refresh_time = int(time.time()) + self._refresh_interval

        # Apply any updates from the websocket
        for msg in self._depth_message_buffer:
            await self._process_depth_message(msg)

        # clear the depth buffer
        del self._depth_message_buffer

    async def _start_socket(self) -> None:
        """Start the depth cache socket

        :return: None
        """
        self._bm = BinanceSocketManager(self._client, self._loop)

        # ['ethbtc@depth', 'xrpbtc@depth',]
        streams = [f'{s.lower()}@depth' for s in self._symbols]
        await self._bm.start_multiplex_socket(streams, self._handle_depth_event)

        # wait for some socket responses
        while not len(self._depth_message_buffer):
            await asyncio.sleep(1)

    async def _handle_depth_event(self, msg: Dict[str, Union[str, Any]]) -> None:
        """Handle a depth event

        :param msg:
        :return: None
        """
        if 'e' in msg['data'] and msg['data']['e'] == 'error':
            # close the socket
            await self.close()

        # Initial depth snapshot fetch not yet performed
        # Buffer messages until init from REST API completed
        if self._refresh_time is None:
            self._depth_message_buffer.append(msg['data'])
        else:
            await self._process_depth_message(msg['data'])

    async def _process_depth_message(self, msg: Dict[str, Any]) -> None:
        """Process a depth event message.

        :param msg: Depth event message.
        :return: None
        """

        if not (await self._depth_cache[msg['s']].update(msg)):
            # ignore any updates before the initial update
            return

        # Check throttle counter and call the callback with the updated depth cache
        if (
            self._coro
            and self._depth_cache[msg['s']].throttle_counter
            == self._coro_throttle_count
        ):
            await self._coro(self._depth_cache[msg['s']])
            self._depth_cache[msg['s']].throttle_counter = 0
        self._depth_cache[msg['s']].throttle_counter += 1

        # after processing event see if we need to refresh the depth cache
        if self._refresh_interval and int(time.time()) > self._refresh_time:
            await self._init_cache()

    def get_depth_cache(self) -> Dict[str, DepthCache]:
        """Get the current depth cache

        :return: DepthCache object
        """
        return self._depth_cache

    async def close(self) -> None:
        """Close the open socket for this manager

        :return: None
        """
        await self._bm.close()
        self._depth_cache = None
