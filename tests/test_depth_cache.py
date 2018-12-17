import json

import pytest
from aioresponses import aioresponses
from binance.client_async import AsyncClient as Client

from recorder.depth_cache import DepthCache


@pytest.fixture
async def depth_cache():
    with open('tests/xrp_depth.json') as f:
        res_depth = json.load(f)

    client = Client('api_key', 'api_secret')

    dc = DepthCache(client, 'XRPBTC')

    with aioresponses() as m:
        m.get(
            'https://api.binance.com/api/v1/depth?limit=500&symbol=XRPBTC',
            payload=res_depth,
        )
        await dc.init_from_rest()
        return dc


@pytest.mark.asyncio
async def test_depth_cache_init_from_rest(depth_cache):
    assert len(depth_cache.get_bids()) == 500
    assert len(depth_cache.get_asks()) == 500


@pytest.mark.asyncio
async def test_depth_cache_update(depth_cache):
    msg = {
        'e': 'depthUpdate',
        'E': 1544893446185,
        's': 'XRPBTC',
        'U': 158868280,
        'u': 158868282,
        'b': [
            ['0.00008830', '51614.00000000', []],
            ['0.00008765', '1780.00000000', []],
        ],
        'a': [['0.00008872', '0.00000000', []]],
    }

    await depth_cache.update(msg)
    bids = depth_cache.get_bids()
    asks = depth_cache.get_bids()

    b1 = [p for p, q in bids if p == 0.00008830 and q == 51614.0]
    b2 = [p for p, q in bids if p == 0.00008765 and q == 1780.0]

    a1 = [p for p, q in asks if p == 0.00008872 and q == 0.0]

    assert len(b1) == 1
    assert len(b2) == 1
    assert len(a1) == 0


def test_get_orders(depth_cache):
    orders = depth_cache.get_orders()
    assert len(orders) == 1000
    orders = depth_cache.get_orders(price_limit=0)
    assert len(orders) == 0
    orders = depth_cache.get_orders(price_limit=0.02)
    assert len(orders) == 292
