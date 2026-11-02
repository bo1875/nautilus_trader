"""Synthetic contract for a separately built candidate, never G5's current wheel.

Without the candidate, changed contracts are strict expected failures. Run with
--runxfail after an explicitly authorized build. No test makes venue calls.
"""
from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
import asyncio
import socket
import subprocess
import sys
import time

import msgspec
import pytest
from nautilus_trader.accounting.accounts.cash import CashAccount
from nautilus_trader.accounting.error import AccountBalanceNegative
from nautilus_trader.accounting.factory import AccountFactory
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import MessageBus, TestClock as Clock
from nautilus_trader.config import CacheConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import AccountType, OrderSide
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.objects import AccountBalance, Currency, Money
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs as IdStubs

CANDIDATE = hasattr(CashAccount, 'allow_negative_balances')
proposed = pytest.mark.xfail(not CANDIDATE, strict=True, raises=(TypeError, AttributeError),
                            reason='requires separately authorized native candidate build')
ACCOUNT = AccountId('BINANCE-SYNTHETIC')


def balance(total, free=None, code='USDT'):
    c = Currency.from_str(code)
    total = Decimal(total); free = total if free is None else Decimal(free)
    return AccountBalance(Money(total, c), Money(total-free, c), Money(free, c))


def state(*balances, reported=True, account_id=ACCOUNT, ts=1):
    return AccountState(account_id, AccountType.CASH, None, reported, list(balances), [],
                        {'synthetic': 'raw facts unchanged'}, UUID4(), ts, ts)



def balances_by_value(account):
    # Account.to_dict serializes events, not the current balance projection.
    return {str(ccy): (value.total.as_decimal(), value.locked.as_decimal(), value.free.as_decimal())
            for ccy, value in account.balances().items()}


def history_by_value(account):
    return account.event_count, tuple(msgspec.json.encode(AccountState.to_dict(event))
                                      for event in account.events)


def assert_unchanged(account, balances, history):
    assert balances_by_value(account) == balances
    assert history_by_value(account) == history


def resting_order(cache, instrument, account_id=ACCOUNT):
    from nautilus_trader.test_kit.stubs.execution import TestExecStubs
    from nautilus_trader.test_kit.stubs.events import TestEventStubs
    order = TestExecStubs.limit_order(instrument=instrument, order_side=OrderSide.SELL,
        quantity=instrument.make_qty('1'), price=instrument.make_price('100'))
    cache.add_order(order)  # Persist OrderInitialized, not merely the last acceptance.
    for event in (TestEventStubs.order_submitted(order, account_id=account_id),
                  TestEventStubs.order_accepted(order, account_id=account_id)):
        order.apply(event)
        cache.update_order(order)  # Append lifecycle event and update the open index.
    assert order.account_id == account_id
    assert order.is_open and order.is_passive
    assert order in cache.orders_open(instrument_id=instrument.id)
    assert [type(event).__name__ for event in order.events] == [
        'OrderInitialized', 'OrderSubmitted', 'OrderAccepted']
    return order


def wait_for(predicate):
    # Only used for the task-local asynchronous native Redis writer.
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    assert predicate(), 'Private native Redis writer did not persist expected state'


@contextmanager
def redis_store(tmp_path):
    import redis
    from nautilus_trader.config import DatabaseConfig
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    process = subprocess.Popen(['redis-server', '--bind', '127.0.0.1', '--port', str(port),
        '--dir', str(tmp_path), '--save', '', '--appendonly', 'no'],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    connection = redis.Redis(host='127.0.0.1', port=port)
    try:
        def available():
            try:
                return connection.ping()
            except redis.ConnectionError:
                return False
        wait_for(available)
        yield connection, DatabaseConfig(host='127.0.0.1', port=port)
    finally:
        connection.close(); process.terminate(); process.wait(timeout=5)


@contextmanager
def native_runtime(database_config, encoding, selected=()):
    from nautilus_trader.cache.database import CacheDatabaseAdapter
    from nautilus_trader.live.execution_engine import LiveExecutionEngine
    from nautilus_trader.serialization.serializer import MsgSpecSerializer
    options = {'reported_negative_cash_accounts': selected} if selected else {}
    config = CacheConfig(database=database_config, encoding=encoding, buffer_interval_ms=1, **options)
    serializer = MsgSpecSerializer(getattr(msgspec, encoding), timestamps_as_str=True)
    database = CacheDatabaseAdapter(IdStubs.trader_id(), UUID4(), serializer, config)
    loop = asyncio.new_event_loop()
    cache = Cache(database, config); clock = Clock()
    bus = MessageBus(IdStubs.trader_id(), clock)
    portfolio = Portfolio(bus, cache, clock)
    engine = LiveExecutionEngine(loop, bus, cache, clock)
    try:
        yield SimpleNamespace(database=database, cache=cache, bus=bus, portfolio=portfolio,
                              engine=engine, serializer=serializer)
    finally:
        engine.dispose(); database.close(); loop.close()


def test_default_cash_remains_strict_without_borrowing():
    with pytest.raises(AccountBalanceNegative):
        CashAccount(state(balance('-1')))
    cash = CashAccount(state(balance('10')))
    assert not cash.allow_borrowing


@proposed
@pytest.mark.parametrize('total,free', [('-5','-7'), ('-5','0'), ('5','7'), ('5','-1'), ('0','0')])
def test_reported_signed_components_initial_and_apply(total, free):
    initial = state(balance(total, free), balance('2', code='ETH'))
    cash = CashAccount(initial, allow_negative_balances=True)
    assert not cash.allow_borrowing and not cash.calculate_account_state
    assert cash.balance(Currency.from_str('USDT')) == initial.balances[0]
    cash.apply(state(balance('8'), ts=2))
    assert cash.balance_total(Currency.from_str('ETH')).as_decimal() == 2
    cash.apply(state(balance(total, free), ts=3))
    assert cash.balance(Currency.from_str('USDT')) == initial.balances[0]
    assert cash.event_count == 3


@proposed
@pytest.mark.parametrize('total,free', [('-1','-1'), ('1','2'), ('1','-1')])
def test_calculated_and_direct_updates_reject_before_any_mutation(total, free):
    cash = CashAccount(state(balance('10'), balance('2', code='ETH')), allow_negative_balances=True)
    before, history = balances_by_value(cash), history_by_value(cash)
    bad = [balance('99', code='ETH'), balance(total, free)]
    with pytest.raises((ValueError, AccountBalanceNegative)):
        cash.update_balances(bad)
    assert_unchanged(cash, before, history)
    with pytest.raises((ValueError, AccountBalanceNegative)):
        cash.apply(state(*bad, reported=False, ts=2))
    assert_unchanged(cash, before, history)


@pytest.mark.xfail(not CANDIDATE, strict=True, raises=AssertionError,
                   reason='stock mutates the valid prefix before rejecting the negative tail')
@pytest.mark.parametrize('operation', ['direct', 'reported', 'unreported'])
def test_default_strict_rejection_is_atomic(operation):
    cash = CashAccount(state(balance('10'), balance('2', code='ETH')))
    before, history = balances_by_value(cash), history_by_value(cash)
    bad = [balance('99', code='ETH'), balance('-1')]
    with pytest.raises(AccountBalanceNegative):
        if operation == 'direct':
            cash.update_balances(bad)
        else:
            cash.apply(state(*bad, reported=operation == 'reported', ts=2))
    assert_unchanged(cash, before, history)


@proposed
@pytest.mark.parametrize('options', [{'allow_borrowing':True}, {'calculate_account_state':True}])
def test_contradictory_configuration_is_rejected(options):
    with pytest.raises(ValueError):
        CashAccount(state(balance('10')), allow_negative_balances=True, **options)


@proposed
@pytest.mark.parametrize('encoding', [msgspec.msgpack, msgspec.json])
def test_account_serialization_and_legacy_dictionary_defaults(encoding):
    account = CashAccount(state(balance('-1')), allow_negative_balances=True)
    data = CashAccount.to_dict(account)
    restored = CashAccount.from_dict(encoding.decode(encoding.encode(data)))
    assert restored.allow_negative_balances and not restored.allow_borrowing
    assert CashAccount.to_dict(restored) == data
    legacy = CashAccount.to_dict(CashAccount(state(balance('1'))))
    legacy.pop('allow_negative_balances')
    assert not CashAccount.from_dict(legacy).allow_negative_balances


@proposed
@pytest.mark.parametrize('bad', ['duplicate', 'wrong_identity', 'empty'])
def test_malformed_event_does_not_publish_prefix_or_append_event(bad):
    cache = Cache(config=CacheConfig(reported_negative_cash_accounts=(str(ACCOUNT),)))
    clock = Clock(); bus = MessageBus(IdStubs.trader_id(), clock)
    portfolio = Portfolio(bus, cache, clock)
    portfolio.update_account(state(balance('10'), balance('2', code='ETH')))
    account = cache.account(ACCOUNT)
    before, history = balances_by_value(account), history_by_value(account)
    received = []; bus.subscribe('events.account.*', received.append)
    if bad == 'duplicate':
        invalid = state(balance('99', code='ETH'), balance('1'), balance('-1'))
    elif bad == 'empty':
        with pytest.raises(ValueError): portfolio.update_account(state())
        assert received == []
        assert_unchanged(account, before, history)
        return
    else:
        invalid = state(balance('99'), account_id=AccountId('OTHER-SYNTHETIC'))
        # Direct apply checks identity; portfolio legitimately creates another account.
        with pytest.raises(ValueError): account.apply(invalid)
        assert_unchanged(account, before, history)
        return
    # Condition.not_in uses KeyError for a duplicate collection member.
    with pytest.raises(KeyError): portfolio.update_account(invalid)
    assert received == []
    assert_unchanged(account, before, history)


@proposed
def test_scope_is_per_cache_and_exact_account_not_process_or_issuer():
    enabled = Cache(config=CacheConfig(reported_negative_cash_accounts=(str(ACCOUNT),)))
    strict = Cache()
    signed = state(balance('-1'))
    assert enabled.create_account(signed).allow_negative_balances
    with pytest.raises(AccountBalanceNegative): strict.create_account(signed)
    with pytest.raises(AccountBalanceNegative): AccountFactory.create(signed)
    with pytest.raises(AccountBalanceNegative):
        enabled.create_account(state(balance('-1'), account_id=AccountId('BINANCE-OTHER')))


@proposed
def test_startup_with_resting_orders_keeps_reported_cash_and_subsequent_fill():
    from nautilus_trader.execution.engine import ExecutionEngine
    from nautilus_trader.test_kit.providers import TestInstrumentProvider
    from nautilus_trader.test_kit.stubs.events import TestEventStubs
    cache = Cache(config=CacheConfig(reported_negative_cash_accounts=(str(ACCOUNT),)))
    clock = Clock(); bus = MessageBus(IdStubs.trader_id(), clock)
    portfolio = Portfolio(bus, cache, clock)
    engine = ExecutionEngine(bus, cache, clock)
    try:
        inst = TestInstrumentProvider.ethusdt_binance(); cache.add_instrument(inst)
        portfolio.update_account(state(balance('-5', '0'), balance('2', code='ETH')))
        account = cache.account(ACCOUNT)
        before, history = balances_by_value(account), history_by_value(account)
        order = resting_order(cache, inst)
        portfolio.initialize_orders()
        assert portfolio.initialized
        assert_unchanged(account, before, history)
        fill = TestEventStubs.order_filled(order, inst, account_id=ACCOUNT,
            last_qty=inst.make_qty('.5'), last_px=inst.make_price('100'))
        engine.process(fill)
        assert order.filled_qty.as_decimal() == Decimal('.5')
        assert len(cache.positions()) == 1
        assert_unchanged(account, before, history)
    finally:
        engine.dispose()


@pytest.mark.parametrize('action', ['submit', 'modify'])
@pytest.mark.parametrize('selected', [False, pytest.param(True, marks=proposed)])
def test_existing_native_risk_scope_does_not_promise_cash_checks_on_amends(action, selected):
    from nautilus_trader.execution.messages import ModifyOrder, SubmitOrder
    from nautilus_trader.risk.engine import RiskEngine
    from nautilus_trader.test_kit.providers import TestInstrumentProvider
    from nautilus_trader.test_kit.stubs.execution import TestExecStubs
    options = {'reported_negative_cash_accounts': (str(ACCOUNT),)} if selected else {}
    cache = Cache(config=CacheConfig(**options)); clock = Clock()
    bus = MessageBus(IdStubs.trader_id(), clock)
    portfolio = Portfolio(bus, cache, clock)
    risk = RiskEngine(portfolio, bus, cache, clock)
    inst = TestInstrumentProvider.ethusdt_binance(); cache.add_instrument(inst)
    portfolio.update_account(state(balance('10'), balance('2', code='ETH')))
    assert not cache.account(ACCOUNT).allow_borrowing
    sent, events = [], []
    bus.register('ExecEngine.execute', sent.append)
    bus.register('ExecEngine.process', events.append)
    if action == 'submit':
        order = TestExecStubs.limit_order(instrument=inst, order_side=OrderSide.BUY,
            quantity=inst.make_qty('100'), price=inst.make_price('100'))
        command = SubmitOrder(order.trader_id, order.strategy_id, order, UUID4(), 1)
    else:
        order = TestExecStubs.make_accepted_order(instrument=inst, order_side=OrderSide.BUY,
            quantity=inst.make_qty('1'), price=inst.make_price('1'))
        cache.add_order(order)
        command = ModifyOrder(order.trader_id, order.strategy_id, inst.id,
            order.client_order_id, order.venue_order_id, inst.make_qty('100'),
            inst.make_price('100'), None, UUID4(), 1)
    risk.execute(command)
    # This is a characterization, not desired financial behavior. The PT
    # command owner must still enforce final cash admission on both paths.
    if action == 'submit':
        assert not sent and len(events) == 1
        assert 'NOTIONAL_EXCEEDS_FREE_BALANCE' in events[0].reason
    else:
        assert sent == [command] and not events
    risk.dispose()


@pytest.mark.parametrize('encoding', ['msgpack', 'json'])
@pytest.mark.parametrize('selected', [False, pytest.param(True, marks=proposed)])
def test_native_redis_restart_with_resting_order_then_ordinary_fill(tmp_path, encoding, selected):
    from nautilus_trader.test_kit.providers import TestInstrumentProvider
    from nautilus_trader.test_kit.stubs.events import TestEventStubs
    from nautilus_trader.serialization.serializer import MsgSpecSerializer
    if selected:
        # On stock, fail before starting a process. The false case qualifies
        # this very same persistence/lifecycle fixture with ordinary stock cash.
        CacheConfig(reported_negative_cash_accounts=(str(ACCOUNT),))
    allowlist = (str(ACCOUNT),) if selected else ()
    serializer = MsgSpecSerializer(getattr(msgspec, encoding), timestamps_as_str=True)
    key = f'trader-{IdStubs.trader_id()}:accounts:{ACCOUNT}'
    original = serializer.serialize(state(balance('10'), balance('2', code='ETH')))
    with redis_store(tmp_path) as (connection, database_config):
        connection.rpush(key, original)  # Legacy positive CASH event remains byte-identical.
        with native_runtime(database_config, encoding, allowlist) as first:
            first.engine.load_cache()
            account = first.cache.account(ACCOUNT)
            assert not account.allow_borrowing
            assert getattr(account, 'allow_negative_balances', False) == selected
            # A recovery and another negative observation are retained before restart.
            totals = ('-5', '20', '-5') if selected else ('5', '20', '5')
            for ts, total in enumerate(totals, 2):
                first.portfolio.update_account(state(balance(total, '0' if total == '-5' else total), ts=ts))
            before, history = balances_by_value(account), history_by_value(account)
            inst = TestInstrumentProvider.ethusdt_binance(); first.cache.add_instrument(inst)
            order = resting_order(first.cache, inst)
            order_key = f'trader-{IdStubs.trader_id()}:orders:{order.client_order_id}'
            wait_for(lambda: connection.llen(key) == 4 and connection.llen(order_key) == 3
                     and first.database.load_instrument(inst.id) is not None)
            persisted_events = [serializer.deserialize(raw) for raw in connection.lrange(order_key, 0, -1)]
            assert [type(event).__name__ for event in persisted_events] == [
                'OrderInitialized', 'OrderSubmitted', 'OrderAccepted']
            assert all(event.account_id == ACCOUNT for event in persisted_events[1:])
            account_bytes = connection.lrange(key, 0, -1)
            coid, iid = order.client_order_id, inst.id
        # New database adapter, native cache, portfolio, bus and live engine.
        with native_runtime(database_config, encoding, allowlist) as second:
            second.engine.load_cache()
            account = second.cache.account(ACCOUNT)
            assert account is not None and not account.allow_borrowing
            assert_unchanged(account, before, history)
            order = second.cache.order(coid); inst = second.cache.instrument(iid)
            assert order.account_id == ACCOUNT and order.is_open and order.is_passive
            assert second.cache.orders_open(instrument_id=iid) == [order]
            assert order.event_count == 3
            second.portfolio.initialize_orders()
            assert second.portfolio.initialized
            if selected:
                assert_unchanged(account, before, history)
            else:
                # Stock control proves this fixture actually reaches the lock
                # calculation that opted-in reported accounts must bypass.
                assert balances_by_value(account)['ETH'] == (Decimal(2), Decimal(1), Decimal(1))
                assert history_by_value(account) == history
            wallet_before_fill = balances_by_value(account)
            fill = TestEventStubs.order_filled(order, inst, account_id=ACCOUNT,
                last_qty=inst.make_qty('.5'), last_px=inst.make_price('100'))
            second.engine._handle_event_with_tracking(fill)  # The live event queue's actual receiver.
            assert order.filled_qty.as_decimal() == Decimal('.5')
            assert len(second.cache.positions()) == 1
            position = second.cache.positions()[0]
            wait_for(lambda: second.database.load_position(position.id) is not None
                     and connection.llen(order_key) == 4)
            restored_position = second.database.load_position(position.id)
            assert restored_position.signed_decimal_qty() == position.signed_decimal_qty()
            assert [event.trade_id for event in restored_position.events] == [fill.trade_id]
            assert_unchanged(account, wallet_before_fill, history)
            assert connection.lrange(key, 0, -1) == account_bytes
            assert account_bytes[0] == original
        if selected:
            with native_runtime(database_config, encoding) as strict:
                with pytest.raises(AccountBalanceNegative): strict.engine.load_cache()
                assert strict.cache.accounts() == []


@proposed
@pytest.mark.parametrize('encoding', ['msgpack', 'json'])
@pytest.mark.parametrize('key_selected,event_selected', [(True, True), (False, False), (True, False), (False, True)])
def test_redis_policy_selection_requires_key_event_identity(tmp_path, encoding, key_selected, event_selected):
    CacheConfig(reported_negative_cash_accounts=(str(ACCOUNT),))
    other = AccountId('BINANCE-OTHER')
    key_id = ACCOUNT if key_selected else other
    event_id = ACCOUNT if event_selected else other
    with redis_store(tmp_path) as (connection, database_config):
        with native_runtime(database_config, encoding, (str(ACCOUNT),)) as runtime:
            key = f'trader-{IdStubs.trader_id()}:accounts:{key_id}'
            raw = runtime.serializer.serialize(state(balance('10'), account_id=event_id))
            connection.rpush(key, raw)
            published = []; runtime.bus.subscribe('events.account.*', published.append)
            if key_id != event_id:
                with pytest.raises(ValueError): runtime.database.load_account(key_id)
                with pytest.raises(ValueError): runtime.engine.load_cache()
                assert runtime.cache.accounts() == [] and published == []
                assert connection.lrange(key, 0, -1) == [raw]
            else:
                restored = runtime.database.load_account(key_id)
                assert restored.id == event_id
                assert restored.allow_negative_balances == event_selected
                assert not restored.allow_borrowing
                runtime.engine.load_cache()
                installed = runtime.cache.account(event_id)
                assert installed.allow_negative_balances == event_selected
                if event_selected:
                    installed.apply(state(balance('-1'), account_id=event_id, ts=2))
                else:
                    with pytest.raises(AccountBalanceNegative):
                        installed.apply(state(balance('-1'), account_id=event_id, ts=2))
                assert published == []


@proposed
def test_failed_identity_keeps_existing_locks_and_event_history():
    from nautilus_trader.model.identifiers import InstrumentId
    cash = CashAccount(state(balance('10')), allow_negative_balances=True)
    quote = Currency.from_str('USDT')
    cash.update_balance_locked(InstrumentId.from_str('FIRST.SIM'), Money(1, quote))
    with pytest.raises(ValueError):
        cash.apply(state(balance('20'), account_id=AccountId('OTHER-SYNTHETIC')))
    assert cash.event_count == 1
    assert balances_by_value(cash)['USDT'] == (Decimal(10), Decimal(1), Decimal(9))
    cash.update_balance_locked(InstrumentId.from_str('SECOND.SIM'), Money(1, quote))
    assert cash.balance_free(quote).as_decimal() == 8


@pytest.mark.parametrize('selected', [False, pytest.param(True, marks=proposed)])
def test_lock_update_clear_and_rejection_preserve_balance_and_history(selected):
    from nautilus_trader.model.identifiers import InstrumentId
    options = {'allow_negative_balances': True} if selected else {}
    cash = CashAccount(state(balance('10')), **options)
    quote = Currency.from_str('USDT')
    first, second = InstrumentId.from_str('FIRST.SIM'), InstrumentId.from_str('SECOND.SIM')
    history = history_by_value(cash)
    cash.update_balance_locked(first, Money(2, quote))
    cash.update_balance_locked(second, Money(3, quote))
    assert balances_by_value(cash)['USDT'] == (Decimal(10), Decimal(5), Decimal(5))
    before = balances_by_value(cash)
    with pytest.raises(ValueError): cash.update_balance_locked(first, Money(-1, quote))
    assert_unchanged(cash, before, history)
    with pytest.raises(TypeError): cash.clear_balance_locked(None)
    assert_unchanged(cash, before, history)
    cash.clear_balance_locked(first)
    assert balances_by_value(cash)['USDT'] == (Decimal(10), Decimal(3), Decimal(7))
    cash.clear_balance_locked(second)
    assert balances_by_value(cash)['USDT'] == (Decimal(10), Decimal(0), Decimal(10))
    assert history_by_value(cash) == history


@proposed
def test_negative_report_rejects_lock_recalculation_without_leaking_a_lock():
    from nautilus_trader.model.identifiers import InstrumentId
    cash = CashAccount(state(balance('-5', '0')), allow_negative_balances=True)
    quote = Currency.from_str('USDT')
    first, second = InstrumentId.from_str('FIRST.SIM'), InstrumentId.from_str('SECOND.SIM')
    before, history = balances_by_value(cash), history_by_value(cash)
    with pytest.raises(AccountBalanceNegative): cash.update_balance_locked(first, Money(1, quote))
    assert_unchanged(cash, before, history)
    # With no valid lock for this negative currency, clearing is a harmless
    # no-op. The rejected update must not have left a lock to recalculate.
    cash.clear_balance_locked(first)
    assert_unchanged(cash, before, history)
    cash.update_balances([balance('10')])  # Direct update deliberately does not clear locks.
    cash.update_balance_locked(second, Money(1, quote))
    assert balances_by_value(cash)['USDT'] == (Decimal(10), Decimal(1), Decimal(9))
    assert history_by_value(cash) == history


@proposed
@pytest.mark.parametrize('kind', ['borrowing', 'calculated', 'custom'])
def test_selected_factory_rejects_existing_issuer_policy_conflicts(kind):
    # Stock stops here. Candidate registration tests run in a disposable Python
    # process because upstream has no deregistration for two of these registries.
    getattr(CashAccount, 'allow_negative_balances')
    script = '''
from nautilus_trader.accounting.accounts.cash import CashAccount
from nautilus_trader.accounting.factory import AccountFactory
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.objects import AccountBalance, Currency, Money
import sys
kind = sys.argv[1]
issuer = 'SYNTHETIC'
if kind == 'borrowing': AccountFactory.register_cash_borrowing(issuer)
elif kind == 'calculated': AccountFactory.register_calculated_account(issuer)
else: AccountFactory.register_account_type(issuer, CashAccount)
ccy = Currency.from_str('USDT')
balance = AccountBalance(Money(10, ccy), Money(0, ccy), Money(10, ccy))
event = AccountState(AccountId('SYNTHETIC-001'), AccountType.CASH, None, True,
                     [balance], [], {}, UUID4(), 1, 1)
legacy = AccountFactory.create(event)
assert legacy.allow_borrowing == (kind == 'borrowing')
assert legacy.calculate_account_state == (kind == 'calculated')
try: AccountFactory.create(event, allow_negative_balances=True)
except ValueError: pass
else: raise AssertionError('Conflicting factory selection accepted')
'''
    result = subprocess.run([sys.executable, '-c', script, kind], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('encoding', ['msgpack', 'json'])
def test_default_loader_refuses_retained_negative_cash_history(tmp_path, encoding):
    with redis_store(tmp_path) as (connection, database_config):
        with native_runtime(database_config, encoding) as runtime:
            key = f'trader-{IdStubs.trader_id()}:accounts:{ACCOUNT}'
            raw = [runtime.serializer.serialize(state(balance('10'))),
                   runtime.serializer.serialize(state(balance('-1'), ts=2))]
            connection.rpush(key, *raw)
            publications = []; runtime.bus.subscribe('events.account.*', publications.append)
            with pytest.raises(AccountBalanceNegative): runtime.engine.load_cache()
            assert runtime.cache.accounts() == []
            assert publications == []
            assert connection.lrange(key, 0, -1) == raw
