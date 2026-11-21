"""Installed transport observations through a real client and loopback websocket.

Synthetic venue only; response acknowledgment is not native application or storage.
"""
import asyncio

from aiohttp import web, WSMsgType
from nautilus_trader.core import nautilus_pyo3 as native
import pytest

from fork_tests.support import instrument, venue


@pytest.mark.parametrize('reply,expected', [
    ('success', 'acknowledged'), ('refused', 'rejected'),
    ('missing_code', 'response_unresolved'), ('wrong_client', 'response_unresolved'),
    ('empty', 'response_unresolved'), ('contradictory', 'response_unresolved'),
])
def test_installed_submit_observation(reply, expected):
    async def scenario():
        observations, requests = [], []
        finished = asyncio.Event()

        def receive(message):
            if isinstance(message, native.OKXRequestObservation):
                observations.append(message)
                if message.outcome in ('acknowledged', 'rejected', 'response_unmatched', 'response_unresolved'):
                    finished.set()

        async def instruments(request):
            return web.json_response(dict(code='0', msg='', data=[instrument('live')]))

        async def websocket(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for message in ws:
                if message.type != WSMsgType.TEXT:
                    continue
                if message.data == 'ping':
                    await ws.send_str('pong')
                    continue
                frame = message.json()
                if frame['op'] == 'login':
                    await ws.send_json(dict(event='login', code='0', msg=''))
                    continue
                assert frame['op'] == 'order'
                requests.append(frame)
                item = dict(clOrdId=frame['args'][0]['clOrdId'], ordId='12345', sCode='0')
                code, data = '0', [item]
                if reply == 'refused':
                    item.update(sCode='51000', sMsg='Synthetic refusal')
                elif reply == 'missing_code':
                    item.pop('sCode')
                elif reply == 'wrong_client':
                    item['clOrdId'] = 'AnotherOrder'
                elif reply == 'empty':
                    data = []
                elif reply == 'contradictory':
                    code = '1'
                await ws.send_json(dict(id=frame['id'], op='order', code=code, msg='', data=data))
            return ws

        app = web.Application()
        app.router.add_get('/api/v5/public/instruments', instruments)
        app.router.add_get('/ws', websocket)
        async with venue(app) as address:
            http = native.OKXHttpClient(api_key='offline', api_secret='offline', api_passphrase='offline', base_url=f'http://{address}', max_retries=0, timeout_secs=2)
            instruments_, _ = await http.request_instruments(native.OKXInstrumentType.SWAP)
            client = native.OKXWebSocketClient(url=f'ws://{address}/ws')
            await asyncio.wait_for(client.connect(asyncio.get_running_loop(), instruments_, receive), 5)
            try:
                await client.submit_order(
                    native.TraderId('TRADER-001'), native.StrategyId('STRATEGY-001'),
                    native.InstrumentId.from_str('TEST0-USDT-SWAP.OKX'), native.OKXTradeMode.CROSS,
                    native.ClientOrderId('SyntheticOrder'), native.OrderSide.BUY, native.OrderType.LIMIT,
                    native.Quantity.from_str('2'), price=native.Price.from_str('100.0'))
                await asyncio.wait_for(finished.wait(), 5)
                assert len(requests) == 1
                assert [row.outcome for row in observations] == [
                    'handler_received', 'send_completed', 'response_received', expected]
                for row in observations:
                    assert row.request_id == requests[0]['id']
                    assert row.client_order_id == 'SyntheticOrder'
                    assert row.instrument_id == 'TEST0-USDT-SWAP.OKX'
                    assert row.operation == 'Order'
                    assert row.requested_price == '100.0' and row.requested_size == '2'
                    assert row.connection_id == observations[0].connection_id
                    assert row.connection_generation == observations[0].connection_generation
                assert observations[1].ts_send_completed_ns >= observations[0].ts_handler_ns > 0
                assert observations[2].ts_received_ns >= observations[1].ts_send_completed_ns
                status = client.capture_status()
                assert status.observations_dropped == 0 and status.state == 'active'
            finally:
                await asyncio.wait_for(client.close(), 5)
    asyncio.run(scenario())
