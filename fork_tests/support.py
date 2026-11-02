from contextlib import asynccontextmanager
from aiohttp import web

def instrument(state, index=0):
    """Synthetic linear swaps; no captured exchange or account records."""
    base = f'TEST{index}'
    return dict(
        instType='SWAP', instId=f'{base}-USDT-SWAP', instIdCode=100 + index,
        uly=f'{base}-USDT', instFamily=f'{base}-USDT', baseCcy='', quoteCcy='',
        settleCcy='USDT', ctVal='1', ctMult='1', ctValCcy=base, optType='',
        stk='', listTime='1700000000000', expTime='', lever='10', tickSz='0.1',
        lotSz='1', minSz='1', ctType='linear', state=state, ruleType='normal',
    )


@asynccontextmanager
async def venue(app):
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        yield f'127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
    finally:
        await runner.cleanup()
