"""Installed native HTTP oracle; no venue, mocked converter or second raw request.

Unqualified legacy wheels have strict expected failures. Use --runxfail when
qualifying the new capability; a green xfail run is NOT qualification.
"""
import asyncio
from datetime import datetime, timezone

from aiohttp import web
from nautilus_trader.core import nautilus_pyo3 as native
import pytest

from fork_tests.support import instrument, venue


METHODS = dict(orders='request_order_status_reports', fills='request_fill_reports',
               positions='request_position_status_reports')
PATHS = dict(orders='/api/v5/trade/orders-pending', fills='/api/v5/trade/fills',
             positions='/api/v5/account/positions')
HAS_CAPABILITY = hasattr(native.OKXHttpClient, METHODS['orders'] + '_with_errors')
GAP = pytest.mark.xfail(not HAS_CAPABILITY, strict=True,
                        reason='Native raw-row completeness capability absent')


def row(kind, index=1):
    common = dict(instId='TEST0-USDT-SWAP', instType='SWAP')
    if kind == 'orders':
        return dict(common, ordId=str(index), clOrdId=f'Synthetic{index}', tag='',
                    ordType='limit', sz='1', px='100', side='buy', posSide='net',
                    tdMode='cross', reduceOnly='false', state='live', avgPx='',
                    fee='0', feeCcy='USDT', fillSz='0', fillPx='', tradeId='',
                    fillTime='0', accFillSz='0', category='normal',
                    uTime='1700000000000', cTime='1700000000000')
    if kind == 'fills':
        return dict(common, tradeId=str(index), ordId=str(index), clOrdId=f'Synthetic{index}',
                    billId=str(index), fillPx='100', fillSz='1', side='buy', execType='T',
                    feeCcy='USDT', fee='0', ts='1700000000000')
    fields = ('baseBal ccy fee lever last markPx liqPx mmr interest notionalUsd avgPx '
              'upl uplRatio margin mgnRatio adl cTime realizedPnl uplLastPx uplRatioLastPx '
              'availPos bePx fundingFee idxPx liqPenalty optVal pendingCloseOrdLiabVal pnl '
              'posCcy quoteBal quoteBorrowed quoteInterest spotInUseAmt spotInUseCcy usdPx')
    return dict(dict.fromkeys(fields.split(), ''), **common, posId=str(index),
                mgnMode='cross', posSide='net', pos='1', tradeId=str(index),
                uTime='1700000000000')


async def observe(kind, rows, *, strict=True, history=None, status=200, **kwargs):
    requests = []

    async def respond(request):
        requests.append(request.path)
        if request.path.endswith('/instruments'):
            data, code = [instrument('live'), instrument('live', 1)], 200
        elif request.path.endswith('/orders-history'):
            data, code = history or [], 200
        else:
            data, code = rows, status
        return web.json_response(dict(code='0', msg='', data=data), status=code)

    app = web.Application()
    for path in ('/api/v5/public/instruments', PATHS[kind], '/api/v5/trade/orders-history'):
        app.router.add_get(path, respond)
    async with venue(app) as address:
        client = native.OKXHttpClient(api_key='offline', api_secret='offline',
            api_passphrase='offline', base_url=f'http://{address}', max_retries=0, timeout_secs=2)
        instruments, _ = await client.request_instruments(native.OKXInstrumentType.SWAP)
        client.cache_instruments(instruments)
        method = METHODS[kind]
        if strict and HAS_CAPABILITY:
            method += '_with_errors'
        result = await asyncio.wait_for(getattr(client, method)(
            account_id=native.AccountId('OKX-001'),
            instrument_type=native.OKXInstrumentType.SWAP, **kwargs), 3)
        assert requests.count(PATHS[kind]) == 1
        if strict and HAS_CAPABILITY:
            return result
        return result, []  # Exposes the legacy false-success defect, never used in PT.


@pytest.mark.parametrize('kind', METHODS)
@pytest.mark.parametrize('count', [0, 1, 2])
def test_empty_and_valid(kind, count):
    rows = [row(kind, i) for i in range(count)]
    if kind == 'positions':
        # Net positions share venue_position_id=None within an instrument.
        for index, item in enumerate(rows):
            item['instId'] = f'TEST{index}-USDT-SWAP'
    reports, errors = asyncio.run(observe(kind, rows))
    assert len(reports) == count and errors == []


@GAP
@pytest.mark.parametrize('kind,field', [('orders', 'sz'), ('fills', 'fillSz'), ('positions', 'pos')])
@pytest.mark.parametrize('bad_first', [False, True])
def test_mixed_conversion_cannot_certify_success(kind, field, bad_first):
    good, bad = row(kind), row(kind, 2)
    bad[field] = 'not-a-quantity'
    reports, errors = asyncio.run(observe(kind, [bad, good] if bad_first else [good, bad]))
    assert len(reports) == 1, 'Usable positive evidence must survive the failed neighbor'
    assert errors, 'A partial native vector cannot certify source completion'


@GAP
@pytest.mark.parametrize('kind', METHODS)
def test_unknown_in_scope_is_failure(kind):
    bad = row(kind, 2)
    bad['instId'] = 'UNKNOWN-USDT-SWAP'
    reports, errors = asyncio.run(observe(kind, [row(kind), bad]))
    assert len(reports) == 1 and errors


@pytest.mark.parametrize('kind', METHODS)
def test_transport_is_not_successful_empty(kind):
    with pytest.raises(ValueError):
        asyncio.run(observe(kind, [], status=503))


@pytest.mark.parametrize('kind', METHODS)
def test_schema_error_is_not_successful_empty(kind):
    bad = row(kind, 2)
    bad['instType'] = 'unknown-type'
    with pytest.raises(ValueError):
        asyncio.run(observe(kind, [row(kind), bad]))


@pytest.mark.parametrize('kind', ['orders', 'fills'])
def test_explicit_time_filter(kind):
    reports, errors = asyncio.run(observe(kind, [row(kind)],
        start=datetime(2024, 1, 1, tzinfo=timezone.utc)))
    assert reports == [] and errors == []


def test_order_deduplication():
    good = row('orders')
    reports, errors = asyncio.run(observe('orders', [good], history=[good]))
    assert len(reports) == 1 and errors == []


@pytest.mark.parametrize('quantity', ['0', '-1'])
def test_explicit_nonpositive_fill_filter(quantity):
    value = row('fills')
    value['fillSz'] = quantity
    reports, errors = asyncio.run(observe('fills', [value]))
    assert reports == [] and errors == []


@GAP
@pytest.mark.parametrize('kind,field', [('orders', 'sz'), ('fills', 'fillSz'), ('positions', 'pos')])
def test_conflicting_duplicate_retains_both_and_fails(kind, field):
    first, second = row(kind), row(kind)
    second[field] = '2'
    reports, errors = asyncio.run(observe(kind, [first, second]))
    assert len(reports) == 2 and any('duplicate:conflict' in e for e in errors)


@GAP
@pytest.mark.parametrize('field', ['posId', 'tradeId'])
def test_position_raw_identity_change_cannot_certify_completeness(field):
    first, second = row('positions'), row('positions')
    second[field] = '2'
    reports, errors = asyncio.run(observe('positions', [first, second, second]))
    assert len(reports) == 2
    assert all(report.venue_position_id is None for report in reports)
    assert len(errors) == 1 and 'duplicate:conflict' in errors[0]


@GAP
@pytest.mark.parametrize('bad_first', [True, False])
def test_malformed_duplicate_cannot_hide_behind_dedup(bad_first):
    good, bad = row('orders'), row('orders')
    bad['sz'] = 'bad'
    reports, errors = asyncio.run(observe('orders', [bad, good] if bad_first else [good, bad]))
    assert len(reports) == 1 and errors


@pytest.mark.parametrize('kind', [pytest.param('orders', marks=GAP), 'fills',
                                  pytest.param('positions', marks=GAP)])
def test_missing_requested_instrument_cannot_certify_empty(kind):
    other = row(kind, 2)
    other['instId'] = 'UNKNOWN-USDT-SWAP'
    with pytest.raises(ValueError):
        asyncio.run(observe(kind, [row(kind), other],
            instrument_id=native.InstrumentId.from_str('OTHER-USDT-SWAP.OKX')))


@pytest.mark.parametrize('kind', METHODS)
def test_known_scope_can_filter_unknown_neighbor(kind):
    other = row(kind, 2)
    other['instId'] = 'UNKNOWN-USDT-SWAP'
    reports, errors = asyncio.run(observe(kind, [row(kind), other],
        instrument_id=native.InstrumentId.from_str('TEST0-USDT-SWAP.OKX')))
    assert len(reports) == 1 and not errors


@pytest.mark.parametrize('kind', ['orders', 'fills'])
def test_out_of_time_malformed_economics_are_filtered(kind):
    bad = row(kind)
    bad['sz' if kind == 'orders' else 'fillSz'] = 'bad'
    reports, errors = asyncio.run(observe(kind, [bad],
        start=datetime(2024, 1, 1, tzinfo=timezone.utc)))
    assert reports == [] and not errors


@pytest.mark.parametrize('kind', METHODS)
def test_invalid_filter_field_is_schema_failure(kind):
    bad = row(kind)
    bad['ts' if kind == 'fills' else 'uTime'] = 'not-a-timestamp'
    with pytest.raises(ValueError):
        asyncio.run(observe(kind, [bad]))


@GAP
@pytest.mark.parametrize('kind', METHODS)
def test_error_identity_is_bounded_and_specific(kind):
    bad = row(kind, 2)
    bad['instId'] = 'UNKNOWN-' + 'X' * 2000
    reports, errors = asyncio.run(observe(kind, [row(kind), bad]))
    assert len(reports) == 1 and len(errors) == 1
    assert errors[0].startswith(f'{kind}:row=1:instrument=')
    assert ':id=2:instrument:missing' in errors[0] and len(errors[0]) < 250


@GAP
@pytest.mark.parametrize('field', ['px', 'avgPx'])
def test_present_malformed_order_prices_are_not_absence(field):
    bad = row('orders', 2)
    bad[field] = 'bad-price'
    reports, errors = asyncio.run(observe('orders', [row('orders'), bad]))
    assert len(reports) == 1 and errors


@pytest.mark.parametrize('quantity', ['-1', '0', '0.00000001'])
def test_signed_zero_dust_position_conversion(quantity):
    value = row('positions')
    value['pos'] = quantity
    reports, errors = asyncio.run(observe('positions', [value]))
    assert len(reports) == 1 and not errors


@pytest.mark.parametrize('kind', METHODS)
def test_legacy_mixed_result_stays_a_list(kind):
    good, bad = row(kind), row(kind, 2)
    bad[dict(orders='sz', fills='fillSz', positions='pos')[kind]] = 'bad'
    reports, errors = asyncio.run(observe(kind, [good, bad], strict=False))
    assert isinstance(reports, list) and len(reports) == 1 and not errors


@pytest.mark.parametrize('quantity', ['1', '0', 'not-a-number'])
def test_native_wallet_preserves_raw_numbers_for_python_owner(quantity):
    async def scenario():
        balance_fields = ('availBal availEq borrowFroz cashBal crossLiab disEq eq eqUsd '
            'smtSyncEq spotCopyTradingEq fixedBal frozenBal imr interest isoEq isoLiab '
            'isoUpl liab maxLoan mgnRatio mmr notionalLever ordFrozen rewardBal spotInUseAmt '
            'clSpotInUseAmt maxSpotInUseAmt spotIsoBal stgyEq twap upl uplLiab spotBal '
            'openAvgPx accAvgPx spotUpl spotUplRatio totalPnl totalPnlRatio')
        balance = dict(dict.fromkeys(balance_fields.split(), '0'), ccy='TEST0', uTime='1700000000000')
        balance['cashBal'] = quantity
        account_fields = ('adjEq borrowFroz imr isoEq mgnRatio mmr notionalUsdForBorrow '
            'notionalUsdForFutures notionalUsdForOption notionalUsdForSwap notionalUsd ordFroz totalEq upl')
        account = dict(dict.fromkeys(account_fields.split(), '0'),
            details=[balance], uTime='1700000000000')
        async def respond(request):
            return web.json_response(dict(code='0', msg='', data=[account]))
        app = web.Application()
        app.router.add_get('/api/v5/account/balance', respond)
        async with venue(app) as address:
            client = native.OKXHttpClient(api_key='offline', api_secret='offline',
                api_passphrase='offline', base_url=f'http://{address}', max_retries=0, timeout_secs=2)
            details = await client.get_balance()
            assert len(details) == 1 and details[0].cash_bal == quantity
    asyncio.run(scenario())




@GAP
@pytest.mark.parametrize('kind', ['fills', 'positions'])
def test_identical_strict_duplicates_collapse(kind):
    good = row(kind)
    reports, errors = asyncio.run(observe(kind, [good, good]))
    assert len(reports) == 1 and not errors


@GAP
def test_unconvertible_quote_quantity_is_not_a_base_quantity():
    bad = row('orders')
    bad.update(tgtCcy='quote_ccy', px='', avgPx='')
    reports, errors = asyncio.run(observe('orders', [bad]))
    assert not reports and errors


@pytest.mark.parametrize('kind', METHODS)
def test_explicit_type_filter(kind):
    other = row(kind, 2)
    other.update(instType='SPOT', instId='OTHER-USDT')
    reports, errors = asyncio.run(observe(kind, [row(kind), other]))
    assert len(reports) == 1 and not errors


@GAP
@pytest.mark.parametrize('quantity', ['', 'NaN', 'inf'])
def test_invalid_fill_quantity_is_not_a_zero_filter(quantity):
    bad = row('fills', 2)
    bad['fillSz'] = quantity
    reports, errors = asyncio.run(observe('fills', [row('fills'), bad]))
    assert len(reports) == 1 and errors


@GAP
@pytest.mark.parametrize('kind,field', [('orders', 'sz'), ('fills', 'fillSz'), ('positions', 'pos')])
def test_duplicate_conflict_cannot_hide_in_native_rounding(kind, field):
    first, second = row(kind), row(kind)
    first[field], second[field] = '1.1', '1.2'  # Both round to 1 for this instrument.
    reports, errors = asyncio.run(observe(kind, [first, second]))
    assert len(reports) == 2 and any('duplicate:conflict' in e for e in errors)


@GAP
@pytest.mark.parametrize('kind', METHODS)
def test_explicit_scope_excludes_a_cached_neighbor(kind):
    other = row(kind, 2)
    other['instId'] = 'TEST1-USDT-SWAP'
    reports, errors = asyncio.run(observe(kind, [row(kind), other],
        instrument_id=native.InstrumentId.from_str('TEST0-USDT-SWAP.OKX')))
    assert len(reports) == 1 and not errors
