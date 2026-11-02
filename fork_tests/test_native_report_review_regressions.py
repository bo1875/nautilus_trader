"""Promoted architecture round-one oracles and native boundary controls.

Original review assertions are preserved. Run with --runxfail to qualify an
installed candidate; legacy wheels deliberately fail the strict contract.
"""
import asyncio
import pytest
from fork_tests.test_native_report_completeness import GAP, observe, row


@pytest.mark.parametrize('case', ['currency-empty', 'currency-blank', 'amount-empty'])
@GAP
def test_strict_fills_cannot_certify_guessed_fee_facts(case):
    good, bad = row('fills'), row('fills', 2)
    if case == 'amount-empty':
        bad['fee'] = ''
    else:
        bad['fee'] = '-0.25'
        bad['feeCcy'] = '' if case == 'currency-empty' else '   '
    reports, errors = asyncio.run(observe('fills', [good, bad]))
    assert any(str(r.trade_id) == '1' for r in reports)
    assert errors, f'Unknown fee certified complete: {[str(r.commission) for r in reports]}'


def test_zero_fee_without_currency_is_not_a_guessed_nonzero_economic_fact():
    good = row('fills')
    good['feeCcy'] = ''
    reports, errors = asyncio.run(observe('fills', [good]))
    assert len(reports) == 1 and not errors


@GAP
def test_order_creation_timestamp_overflow_is_a_row_error():
    good, bad = row('orders'), row('orders', 2)
    bad['cTime'] = str((2**64 - 1) // 1_000_000 + 1)
    reports, errors = asyncio.run(observe('orders', [good, bad]))
    assert any(str(r.client_order_id) == 'Synthetic1' for r in reports)
    assert errors, f'Overflowing cTime certified complete: {[r.ts_accepted for r in reports]}'


@GAP
def test_quote_conversion_overflow_retains_sibling_and_reports_row_error():
    # Current native Decimal division aborts the process. Keep the oracle isolated.
    import subprocess
    import sys
    code = """
import asyncio, resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from fork_tests.test_native_report_completeness import observe, row
good, bad = row('orders'), row('orders', 2)
bad.update(tgtCcy='quote_ccy', sz='79228162514264337593543950335', px='0.1')
reports, errors = asyncio.run(observe('orders', [good, bad]))
assert any(str(r.client_order_id) == 'Synthetic1' for r in reports)
assert errors
"""
    completed = subprocess.run([sys.executable, '-c', code], text=True,
                               capture_output=True, timeout=10)
    assert completed.returncode == 0, (
        f'Row conversion failed outside the error result: returncode={completed.returncode}; '
        f'stderr={completed.stderr[-1200:]}')


@pytest.mark.parametrize('field', ['cTime', 'uTime'])
@pytest.mark.parametrize('milliseconds', [0, (2**64 - 1) // 1_000_000])
def test_order_timestamp_representable_boundaries(field, milliseconds):
    value = row('orders')
    value[field] = str(milliseconds)
    reports, errors = asyncio.run(observe('orders', [value]))
    assert len(reports) == 1 and not errors
    timestamp = reports[0].ts_accepted if field == 'cTime' else reports[0].ts_last
    assert timestamp == milliseconds * 1_000_000


@GAP
@pytest.mark.parametrize('field', ['cTime', 'uTime'])
@pytest.mark.parametrize('bad_first', [False, True])
def test_order_timestamp_overflow_retains_only_valid_sibling(field, bad_first):
    good, bad = row('orders'), row('orders', 2)
    bad[field] = str((2**64 - 1) // 1_000_000 + 1)
    reports, errors = asyncio.run(observe('orders', [bad, good] if bad_first else [good, bad]))
    assert [str(r.client_order_id) for r in reports] == ['Synthetic1']
    assert len(errors) == 1 and errors[0].endswith(':timestamp:conversion')


@pytest.mark.parametrize('bound', ['start', 'end'])
def test_creation_overflow_outside_explicit_time_scope_is_filtered(bound):
    from datetime import datetime, timezone
    value = row('orders')
    value['cTime'] = str((2**64 - 1) // 1_000_000 + 1)
    # uTime remains a valid scope discriminator; cTime is not consumed after exclusion.
    year = 2024 if bound == 'start' else 2022
    reports, errors = asyncio.run(observe('orders', [value],
        **{bound: datetime(year, 1, 1, tzinfo=timezone.utc)}))
    assert reports == [] and not errors


@GAP
@pytest.mark.parametrize('field', ['cTime', 'uTime'])
def test_timestamp_overflow_outside_explicit_instrument_scope_is_filtered(field):
    from nautilus_trader.core import nautilus_pyo3 as native
    good, bad = row('orders'), row('orders', 2)
    bad.update(instId='TEST1-USDT-SWAP')
    bad[field] = str((2**64 - 1) // 1_000_000 + 1)
    reports, errors = asyncio.run(observe('orders', [bad, good],
        instrument_id=native.InstrumentId.from_str('TEST0-USDT-SWAP.OKX')))
    assert [str(r.client_order_id) for r in reports] == ['Synthetic1'] and not errors


@GAP
@pytest.mark.parametrize('missing', ['absent', 'empty'])
@pytest.mark.parametrize('bad_first', [False, True])
def test_missing_fee_amount_cannot_be_an_observed_zero(missing, bad_first):
    good, bad = row('fills'), row('fills', 2)
    good.update(fee='0.25', feeCcy='USDT')  # Valid rebate must survive.
    if missing == 'absent':
        bad.pop('fee')
    else:
        bad['fee'] = ''
    reports, errors = asyncio.run(observe('fills', [bad, good] if bad_first else [good, bad]))
    assert [str(r.trade_id) for r in reports] == ['1']
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')
    assert reports[0].commission.as_decimal() < 0


@GAP
@pytest.mark.parametrize('amount', ['-0.25', '0.25'])
@pytest.mark.parametrize('currency', ['', ' \t '])
def test_fee_and_rebate_need_observed_currency(amount, currency):
    good, bad = row('fills'), row('fills', 2)
    good.update(fee=amount, feeCcy='USDT')
    bad.update(fee=amount, feeCcy=currency)
    reports, errors = asyncio.run(observe('fills', [good, bad]))
    assert [str(r.trade_id) for r in reports] == ['1']
    assert len(errors) == 1 and errors[0].endswith(':report:conversion')


@pytest.mark.parametrize('amount', ['-0.25', '0.25', '0', '-0.000000001', '0.000000001'])
@pytest.mark.parametrize('currency', ['USDT', ' TEST0 '])
def test_observed_fee_facts_and_native_rounding_remain_accepted(amount, currency):
    from decimal import Decimal
    value = row('fills')
    value.update(fee=amount, feeCcy=currency)
    reports, errors = asyncio.run(observe('fills', [value]))
    assert len(reports) == 1 and not errors
    commission = reports[0].commission
    assert str(commission.currency) == currency.strip()
    # Native money rounding remains authoritative, including valid sub-quantum fees.
    if abs(Decimal(amount)) >= Decimal('0.00000001'):
        assert commission.as_decimal() == -Decimal(amount)


@pytest.mark.parametrize('currency', ['', ' \t '])
@pytest.mark.parametrize('amount', ['0', '-0', '0.0000000000000000000000000000'])
def test_explicit_zero_with_empty_currency_retains_legacy_zero_convention(amount, currency):
    value = row('fills')
    value.update(fee=amount, feeCcy=currency)
    reports, errors = asyncio.run(observe('fills', [value]))
    assert len(reports) == 1 and not errors
    assert reports[0].commission.as_decimal() == 0


@pytest.mark.parametrize('fee,currency', [(None, ''), ('', ''), ('-0.25', ''), ('0.25', '   ')])
def test_legacy_fee_fallbacks_remain_legacy_only(fee, currency):
    from decimal import Decimal
    value = row('fills')
    value['feeCcy'] = currency
    if fee is None:
        value.pop('fee')
    else:
        value['fee'] = fee
    reports, errors = asyncio.run(observe('fills', [value], strict=False))
    assert len(reports) == 1 and not errors
    assert reports[0].commission.as_decimal() == -Decimal(fee or '0')
    assert str(reports[0].commission.currency) == 'USDT'


def quote_subprocess(size, price, expected_quantity, *, bad_first=False):
    """All extreme-operand trials are isolated, even controls expected to be safe."""
    import json
    import subprocess
    import sys
    code = '''
import asyncio, json, resource, sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
from fork_tests.test_native_report_completeness import HAS_CAPABILITY, observe, row
size, price, expected, bad_first = json.loads(sys.argv[1])
good, candidate = row('orders'), row('orders', 2)
candidate.update(tgtCcy='quote_ccy', sz=size, px=price)
reports, errors = asyncio.run(observe('orders', [candidate, good] if bad_first else [good, candidate]))
assert any(str(r.client_order_id) == 'Synthetic1' for r in reports)
found = [r for r in reports if str(r.client_order_id) == 'Synthetic2']
if expected is None:
    assert not found
    if HAS_CAPABILITY:
        assert len(errors) == 1 and errors[0].endswith(':report:conversion')
else:
    assert len(found) == 1 and not errors
    assert str(found[0].quantity) == expected
'''
    return subprocess.run([sys.executable, '-c', code,
        json.dumps([size, price, expected_quantity, bad_first])],
        text=True, capture_output=True, timeout=10)


@pytest.mark.parametrize('size,price,quantity', [
    ('1', '0.1', '10'),
    ('0.0000000000000000000000000001', '0.0000000000000000000000000001', '1'),
    ('0', '0.1', '0'),
    # Decimal's exact maximum quotient is safe division but exceeds native Quantity.
    ('79228162514264337593543950335', '1', None),
    ('7922816251426433759354395033.5', '0.1', None),
])
def test_checked_quote_division_safe_boundaries(size, price, quantity):
    result = quote_subprocess(size, price, quantity)
    assert result.returncode == 0, result.stderr[-1200:]


@GAP
def test_quote_overflow_before_valid_sibling_stays_inside_row_errors():
    result = quote_subprocess('79228162514264337593543950335', '0.1', None, bad_first=True)
    assert result.returncode == 0, result.stderr[-1200:]
