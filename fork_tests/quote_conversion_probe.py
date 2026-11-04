"""Subprocess-only spot-market quote conversion observations; synthetic loopback."""
import asyncio
import json
import resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from aiohttp import web
from nautilus_trader.core import nautilus_pyo3 as native
from fork_tests.test_native_report_completeness import row, instrument, venue

async def scenario(size, average_price, *, strict=True):
    inst = instrument('live')
    inst.update(instType='SPOT', instId='TEST0-USDT', baseCcy='TEST0', quoteCcy='USDT',
                ctVal='', ctMult='', ctValCcy='', ctType='', settleCcy='', uly='', instFamily='',
                lotSz='0.00000001', minSz='0.00000001')
    good, candidate = row('orders'), row('orders', 2)
    for value in (good, candidate):
        value.update(instType='SPOT', instId='TEST0-USDT')
    candidate.update(tgtCcy='quote_ccy', ordType='market', state='partially_filled',
                     sz=size, px='', avgPx=average_price, accFillSz='0.01')
    paths = []
    async def respond(request):
        paths.append(request.path)
        data = [inst] if request.path.endswith('/instruments') else [good, candidate]
        return web.json_response(dict(code='0', msg='', data=data))
    app = web.Application()
    for path in ['/api/v5/public/instruments', '/api/v5/trade/orders-pending']:
        app.router.add_get(path, respond)
    async with venue(app) as address:
        client = native.OKXHttpClient(api_key='offline', api_secret='offline', api_passphrase='offline',
                                      base_url=f'http://{address}', max_retries=0, timeout_secs=2)
        instruments, errors = await client.request_instruments(native.OKXInstrumentType.SPOT)
        assert len(instruments) == 1, repr((instruments, errors))
        client.cache_instruments(instruments)
        method = 'request_order_status_reports' + ('_with_errors' if strict else '')
        result = await getattr(client, method)(account_id=native.AccountId('OKX-001'),
                        instrument_type=native.OKXInstrumentType.SPOT, open_only=True)
        reports, errors = result if strict else (result, [])
        assert paths.count('/api/v5/trade/orders-pending') == 1
        return dict(size=size, average_price=average_price, strict=strict,
                    quantities=[str(r.quantity) for r in reports],
                    filled_quantities=[str(r.filled_qty) for r in reports], errors=errors)

async def main():
    tiny = '0.' + '0'*28 + '1'
    smallest = '0.' + '0'*27 + '1'
    larger = '0.' + '0'*26 + '1'
    results = [await scenario(tiny, smallest),
               await scenario(smallest, smallest),
               await scenario(smallest, larger),
               await scenario(tiny, smallest, strict=False)]
    assert results[1]['quantities'] == ['1.00000000', '1.00000000'] and not results[1]['errors']
    assert results[2]['quantities'] == ['1.00000000', '0.10000000'] and not results[2]['errors']
    print(json.dumps(results, indent=2))

asyncio.run(main())
