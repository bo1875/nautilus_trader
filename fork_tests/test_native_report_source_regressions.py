"""Promoted implementation review assertions plus strict-source boundary controls."""
import asyncio
import pytest
from aiohttp import web
from nautilus_trader.core import nautilus_pyo3 as native
from fork_tests.test_native_report_completeness import GAP, observe, row, instrument, venue

@pytest.mark.parametrize('name,kind,edits', [
    ('creation_timestamp_overflow', 'orders', {'cTime': '18446744073710'}),
    ('nonzero_fee_without_currency', 'fills', {'fee': '-0.1', 'feeCcy': ''}),
    ('positive_fill_outside_decimal_domain', 'fills', {'fillSz': '0.' + '0'*28 + '1'}),
])
@GAP
def test_strict_does_not_certify_lossy_conversion(name, kind, edits):
    good, bad = row(kind), row(kind, 2)
    bad.update(edits)
    reports, errors = asyncio.run(observe(kind, [good, bad]))
    assert reports, 'Valid sibling must survive'
    assert errors, name


@GAP
def test_strict_nonzero_margin_with_unknown_quantity_currency():
    reports, errors = asyncio.run(margin_position())
    assert errors, 'A nonzero position cannot certify FLAT because posCcy is absent'


@pytest.mark.parametrize('kind,field', [('orders','sz'), ('fills','fillSz'), ('positions','pos')])
@GAP
def test_strict_repeated_conflicting_variant_collapses(kind, field):
    first, second = row(kind), row(kind)
    second[field] = '2'
    reports, errors = asyncio.run(observe(kind, [first, second, second]))
    assert len(reports) == 2, 'Retain each distinct validated snapshot exactly once'
    assert any('duplicate:conflict' in e for e in errors)

@pytest.mark.parametrize('kind', ['orders', 'fills', 'positions'])
@GAP
def test_empty_timestamp_is_not_valid_scope_evidence(kind):
    from datetime import datetime, timezone
    field = 'ts' if kind == 'fills' else 'uTime'
    good, bad = row(kind), row(kind, 2)
    good[field] = '1735689600000'
    bad[field] = ''
    kwargs = {} if kind == 'positions' else {'start': datetime(2024, 1, 1, tzinfo=timezone.utc)}
    try:
        reports, errors = asyncio.run(observe(kind, [good, bad], **kwargs))
    except ValueError:
        return  # Accepted whole-envelope rejection.
    assert errors, 'Empty timestamp cannot become zero and certify filtered completion'


async def margin_position():
    inst = instrument('live')
    inst.update(instType='SPOT', instId='TEST0-USDT', baseCcy='TEST0', quoteCcy='USDT',
                ctVal='', ctMult='', ctValCcy='', ctType='', settleCcy='', uly='', instFamily='')
    bad = row('positions')
    bad.update(instType='MARGIN', instId='TEST0-USDT', pos='10', posCcy='')
    async def respond(request):
        data = [inst] if request.path.endswith('instruments') else [bad]
        return web.json_response(dict(code='0', msg='', data=data))
    app = web.Application()
    app.router.add_get('/api/v5/public/instruments', respond)
    app.router.add_get('/api/v5/account/positions', respond)
    async with venue(app) as address:
        client = native.OKXHttpClient(api_key='offline', api_secret='offline', api_passphrase='offline',
            base_url=f'http://{address}', max_retries=0, timeout_secs=2)
        instruments, _ = await client.request_instruments(native.OKXInstrumentType.SPOT)
        client.cache_instruments(instruments)
        method = 'request_position_status_reports'
        if hasattr(client, method + '_with_errors'):
            return await getattr(client, method + '_with_errors')(account_id=native.AccountId('OKX-001'), instrument_type=native.OKXInstrumentType.MARGIN)
        return await getattr(client, method)(account_id=native.AccountId('OKX-001'), instrument_type=native.OKXInstrumentType.MARGIN), []


@GAP
@pytest.mark.parametrize('open_only', [False, True])
def test_empty_creation_time_rejects_strict_envelope(open_only):
    bad = row('orders', 2)
    bad['cTime'] = ''
    with pytest.raises(ValueError):
        asyncio.run(observe('orders', [row('orders'), bad], open_only=open_only))


@GAP
def test_empty_history_timestamp_rejects_strict_envelope():
    bad = row('orders', 2)
    bad['uTime'] = ''
    with pytest.raises(ValueError):
        asyncio.run(observe('orders', [row('orders')], history=[bad]))


@pytest.mark.parametrize('kind', ['orders', 'fills', 'positions'])
def test_explicit_epoch_is_valid_timestamp_evidence(kind):
    from datetime import datetime, timezone
    value = row(kind)
    value['ts' if kind == 'fills' else 'uTime'] = '0'
    kwargs = {} if kind == 'positions' else {'start': datetime(2024, 1, 1, tzinfo=timezone.utc)}
    reports, errors = asyncio.run(observe(kind, [value], **kwargs))
    assert not errors
    if kind == 'positions':
        assert len(reports) == 1 and reports[0].ts_last == 0
    else:
        assert reports == []


@pytest.mark.parametrize('kind', ['orders', 'fills', 'positions'])
def test_legacy_empty_timestamp_behavior_unchanged(kind):
    value = row(kind)
    value['ts' if kind == 'fills' else 'uTime'] = ''
    reports, errors = asyncio.run(observe(kind, [value], strict=False))
    assert len(reports) == 1 and not errors
    timestamp = reports[0].ts_event if kind == 'fills' else reports[0].ts_last
    assert timestamp == 0


@GAP
@pytest.mark.parametrize('quantity', ['0.' + '0'*28 + '1', '0.' + '0'*28 + '9',
                                    '1.' + '0'*28 + '1'])
def test_nonexact_fill_quantity_retains_valid_sibling_and_errors(quantity):
    good, bad = row('fills'), row('fills', 2)
    bad['fillSz'] = quantity
    reports, errors = asyncio.run(observe('fills', [good, bad]))
    assert [str(r.trade_id) for r in reports] == ['1']
    assert len(errors) == 1 and errors[0].endswith(':fill_quantity:conversion')


@pytest.mark.parametrize('quantity,count', [
    ('0', 0), ('-0', 0), ('-1', 0), ('-0.00000001', 0),
    ('0.' + '0'*27 + '1', 1), ('0.00000001', 1), ('1', 1),
])
def test_exact_fill_domain_preserves_zero_and_representable_dust(quantity, count):
    value = row('fills')
    value['fillSz'] = quantity
    reports, errors = asyncio.run(observe('fills', [value]))
    assert len(reports) == count and not errors


@GAP
@pytest.mark.parametrize('kind,field', [('orders','sz'), ('fills','fillSz'), ('positions','pos')])
@pytest.mark.parametrize('sequence', [(1, 2, 2), (2, 1, 2), (2, 2, 1), (1, 2, 3, 2, 3, 1)])
def test_each_distinct_conflict_variant_retained_once(kind, field, sequence):
    values = []
    for qty in sequence:
        value = row(kind)
        value[field] = str(qty)
        values.append(value)
    reports, errors = asyncio.run(observe(kind, values))
    quantities = [str(r.last_qty if kind == 'fills' else r.quantity) for r in reports]
    assert quantities == [str(q) for q in dict.fromkeys(sequence)]
    assert len(errors) == len(set(sequence)) - 1
    assert all(e.endswith(':duplicate:conflict') for e in errors)


async def observe_spot_positions(rows, *, kind='MARGIN', strict=True):
    """Synthetic spot instrument for the native SPOT/MARGIN position-report API."""
    inst = instrument('live')
    inst.update(instType='SPOT', instId='TEST0-USDT', baseCcy='TEST0', quoteCcy='USDT',
        ctVal='', ctMult='', ctValCcy='', ctType='', settleCcy='', uly='', instFamily='')
    paths = []
    async def respond(request):
        paths.append(request.path)
        data = [inst] if request.path.endswith('instruments') else rows
        return web.json_response(dict(code='0', msg='', data=data))
    app = web.Application()
    app.router.add_get('/api/v5/public/instruments', respond)
    app.router.add_get('/api/v5/account/positions', respond)
    async with venue(app) as address:
        client = native.OKXHttpClient(api_key='offline', api_secret='offline', api_passphrase='offline',
            base_url=f'http://{address}', max_retries=0, timeout_secs=2)
        instruments, _ = await client.request_instruments(native.OKXInstrumentType.SPOT)
        client.cache_instruments(instruments)
        method = 'request_position_status_reports'
        capable = strict and hasattr(client, method + '_with_errors')
        if capable:
            method += '_with_errors'
        result = await getattr(client, method)(account_id=native.AccountId('OKX-001'),
            instrument_type=getattr(native.OKXInstrumentType, kind))
        assert paths.count('/api/v5/account/positions') == 1
        return result if capable else (result, [])


def spot_position(index=1, *, kind='MARGIN', quantity='10', currency='TEST0'):
    value = row('positions', index)
    value.update(instType=kind, instId='TEST0-USDT', pos=quantity, posCcy=currency,
                 avgPx='10', markPx='10')
    return value


@GAP
@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
@pytest.mark.parametrize('quantity', ['10', '-10', '0.00000001'])
def test_nonzero_position_cannot_flatten_without_currency(kind, quantity):
    good = spot_position(kind=kind)
    bad = spot_position(2, kind=kind, quantity=quantity, currency='')
    reports, errors = asyncio.run(observe_spot_positions([good, bad], kind=kind))
    assert len(reports) == 1 and str(reports[0].quantity) == '10'
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')


@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
@pytest.mark.parametrize('currency', ['   ', 'OTHER'])
def test_nonzero_position_unknown_currency_stays_error(kind, currency):
    value = spot_position(kind=kind, currency=currency)
    reports, errors = asyncio.run(observe_spot_positions([value], kind=kind))
    assert not reports
    # Legacy also skips this row; only strict capability exposes the error.
    if hasattr(native.OKXHttpClient, 'request_position_status_reports_with_errors'):
        assert errors


@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
@pytest.mark.parametrize('quantity,currency,expected', [
    ('0', '', '0'), ('-0', '', '0'), ('10', 'TEST0', '10'), ('-10', 'TEST0', '10'),
    ('0.00000001', 'TEST0', '0'), ('10', 'USDT', '1'),
])
def test_spot_margin_valid_zero_signed_dust_and_quote_positions(kind, quantity, currency, expected):
    value = spot_position(kind=kind, quantity=quantity, currency=currency)
    reports, errors = asyncio.run(observe_spot_positions([value], kind=kind))
    assert len(reports) == 1 and str(reports[0].quantity) == expected and not errors


@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
def test_legacy_nonzero_empty_position_currency_stays_legacy_only(kind):
    value = spot_position(kind=kind, currency='')
    reports, errors = asyncio.run(observe_spot_positions([value], kind=kind, strict=False))
    assert len(reports) == 1 and str(reports[0].quantity) == '0' and not errors


@GAP
@pytest.mark.parametrize('field', ['pos', 'fee'])
def test_nonzero_underflow_cannot_prove_position_or_commission_zero(field):
    tiny = '0.' + '0'*28 + '1'
    if field == 'pos':
        good, bad = spot_position(), spot_position(2, quantity=tiny, currency='')
        reports, errors = asyncio.run(observe_spot_positions([good, bad]))
    else:
        good, bad = row('fills'), row('fills', 2)
        bad.update(fee=tiny, feeCcy='')
        reports, errors = asyncio.run(observe('fills', [good, bad]))
    assert len(reports) == 1 and errors


@GAP
def test_position_quote_overflow_retains_sibling_as_row_error():
    import subprocess
    import sys
    code = '''
import asyncio, resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from fork_tests.test_native_report_source_regressions import observe_spot_positions, spot_position
good, bad = spot_position(), spot_position(2, quantity='79228162514264337593543950335', currency='USDT')
bad.update(avgPx='0.1')
reports, errors = asyncio.run(observe_spot_positions([good, bad]))
assert len(reports) == 1 and errors
'''
    result = subprocess.run([sys.executable, '-c', code], text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr[-1200:]
