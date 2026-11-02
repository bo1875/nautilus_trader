"""Exact raw decimals precede precision rounding in strict native reports."""
import asyncio
import json
import subprocess
import sys

import pytest

from fork_tests.test_native_report_completeness import GAP, observe, row
from fork_tests.test_native_report_source_regressions import (
    observe_spot_positions, spot_position,
)


@GAP
@pytest.mark.parametrize('kind,field', [
    ('orders','sz'), ('orders','accFillSz'), ('orders','px'), ('orders','avgPx'),
    ('fills','fillPx'), ('positions','avgPx'),
])
def test_nonrepresentable_consumed_fact_is_a_row_error(kind, field):
    good, bad = row(kind), row(kind, 2)
    if kind == 'positions':
        bad['instId']='TEST1-USDT-SWAP'  # Separate net identity, no unrelated conflict error.
    bad[field]='0.'+'0'*28+'1'
    reports,errors=asyncio.run(observe(kind,[good,bad]))
    assert reports, 'Valid sibling must survive'
    assert errors, f'Raw {field}=1e-29 cannot certify complete zero evidence'


@pytest.mark.parametrize('kind,field', [('orders','sz'), ('fills','fillPx'), ('positions','avgPx')])
def test_representable_dust_retains_native_rounding(kind,field):
    value=row(kind)
    value[field]='0.'+'0'*27+'1'
    reports,errors=asyncio.run(observe(kind,[value]))
    assert len(reports)==1 and not errors


@GAP
@pytest.mark.parametrize('kind,field', [
    ('orders', 'sz'), ('orders', 'accFillSz'), ('orders', 'px'), ('orders', 'avgPx'),
    ('fills', 'fillPx'), ('positions', 'avgPx'),
])
@pytest.mark.parametrize('raw', ['-0.' + '0'*28 + '1', '1.' + '0'*28 + '1'])
def test_nonexact_consumed_numeric_keeps_only_valid_sibling(kind, field, raw):
    good, bad = row(kind), row(kind, 2)
    if kind == 'positions':
        bad['instId'] = 'TEST1-USDT-SWAP'
    bad[field] = raw
    reports, errors = asyncio.run(observe(kind, [good, bad]))
    assert len(reports) == 1
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')


@pytest.mark.parametrize('kind,field', [
    ('orders', 'sz'), ('orders', 'accFillSz'), ('orders', 'px'), ('orders', 'avgPx'),
    ('fills', 'fillPx'), ('positions', 'avgPx'),
])
def test_legacy_numeric_underflow_remains_legacy(kind, field):
    value = row(kind)
    value[field] = '0.' + '0'*28 + '1'
    reports, errors = asyncio.run(observe(kind, [value], strict=False))
    assert len(reports) == 1 and not errors


@GAP
@pytest.mark.parametrize('field', ['sz', 'accFillSz', 'px', 'avgPx'])
def test_quote_order_raw_operands_must_be_exact(field):
    good, bad = row('orders'), row('orders', 2)
    bad.update(tgtCcy='quote_ccy', px='0.1', avgPx='0.1')
    bad[field] = '0.' + '0'*28 + '1'
    reports, errors = asyncio.run(observe('orders', [good, bad]))
    assert [str(report.client_order_id) for report in reports] == ['Synthetic1']
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')


@GAP
@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
@pytest.mark.parametrize('field', ['avgPx', 'markPx'])
def test_consumed_quote_position_price_requires_exact_decimal(kind, field):
    good = spot_position(kind=kind)
    bad = spot_position(2, kind=kind, currency='USDT')
    bad.update(avgPx='', markPx='1')
    bad[field] = '1.' + '0'*28 + '1'
    reports, errors = asyncio.run(observe_spot_positions([good, bad], kind=kind))
    assert len(reports) == 1 and str(reports[0].quantity) == '10'
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')


@pytest.mark.parametrize('kind', ['SPOT', 'MARGIN'])
def test_unused_mark_price_is_not_added_to_strict_domain(kind):
    value = spot_position(kind=kind, currency='USDT')
    value.update(avgPx='10', markPx='1.' + '0'*28 + '1')
    reports, errors = asyncio.run(observe_spot_positions([value], kind=kind))
    assert len(reports) == 1 and str(reports[0].quantity) == '1' and not errors


@pytest.mark.parametrize('kind,field', [
    ('orders', 'accFillSz'), ('orders', 'px'), ('orders', 'avgPx'), ('positions', 'avgPx'),
])
def test_remaining_representable_dust_keeps_existing_rounding(kind, field):
    value = row(kind)
    value[field] = '0.' + '0'*27 + '1'
    reports, errors = asyncio.run(observe(kind, [value]))
    assert len(reports) == 1 and not errors


@GAP
def test_order_quote_size_underflow_cannot_certify_zero_reservation():
    # Isolated extreme-operand check. Only the synthetic instrument precision changes.
    code = '''
import asyncio, json, resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from fork_tests import test_native_report_completeness as fixture
original_instrument = fixture.instrument
def precise_instrument(*args, **kwargs):
    inst = original_instrument(*args, **kwargs)
    inst.update(lotSz='0.00000001', minSz='0.00000001')
    return inst
fixture.instrument = precise_instrument
good, bad = fixture.row('orders'), fixture.row('orders', 2)
bad.update(tgtCcy='quote_ccy', sz='0.' + '0'*28 + '1', px='0.' + '0'*27 + '1')
reports, errors = asyncio.run(fixture.observe('orders', [good, bad]))
print(json.dumps(dict(quantities=[str(r.quantity) for r in reports], errors=errors)))
assert len(reports) == 1 and str(reports[0].client_order_id) == 'Synthetic1'
assert len(errors) == 1 and errors[0].endswith(':report:conversion')
'''
    result = subprocess.run([sys.executable, '-c', code], text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr[-1200:]


@GAP
def test_spot_market_quote_underflow_is_rejected_with_valid_controls():
    from pathlib import Path
    script = Path(__file__).with_name('g4s-round2-quote-probe.py')
    result = subprocess.run([sys.executable, str(script)], text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr[-1200:]
    observations = json.loads(result.stdout)
    # The subprocess asserts both representable controls before returning observations.
    affected = observations[0]
    assert affected['quantities'] == ['1.00000000'], affected
    assert len(affected['errors']) == 1, affected
