# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.okx.config import OKXExecClientConfig
from nautilus_trader.adapters.okx.constants import OKX_VENUE
from nautilus_trader.adapters.okx.execution import OKXExecutionClient
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.live.reconciliation import IncompleteOrderStatusReports
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.test_kit.functions import eventually
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.okx.conftest import _create_ws_mock


@pytest.fixture
def exec_client_builder(
    event_loop,
    mock_http_client,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    def builder(monkeypatch, *, config_kwargs: dict | None = None):
        private_ws = _create_ws_mock()
        business_ws = _create_ws_mock()
        ws_iter = iter([private_ws, business_ws])

        monkeypatch.setattr(
            "nautilus_trader.adapters.okx.execution.nautilus_pyo3.OKXWebSocketClient.with_credentials",
            lambda *args, **kwargs: next(ws_iter),
        )

        mock_http_client.reset_mock()
        mock_instrument_provider.initialize.reset_mock()
        mock_instrument_provider.instruments_pyo3.reset_mock()
        mock_instrument_provider.instruments_pyo3.return_value = [MagicMock(name="py_instrument")]

        config_kwargs = config_kwargs or {}
        instrument_types = config_kwargs.pop(
            "instrument_types",
            (nautilus_pyo3.OKXInstrumentType.SPOT,),
        )

        # Set the mock provider's instrument_types to match config
        mock_instrument_provider.instrument_types = instrument_types

        config = OKXExecClientConfig(
            api_key="test_api_key",
            api_secret="test_api_secret",
            api_passphrase="test_passphrase",
            instrument_types=instrument_types,
            **config_kwargs,
        )

        client = OKXExecutionClient(
            loop=event_loop,
            client=mock_http_client,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=config,
            name=None,
        )

        return client, private_ws, business_ws, mock_http_client, mock_instrument_provider

    return builder


@pytest.mark.asyncio
async def test_connect_success(exec_client_builder, monkeypatch):
    # Arrange
    client, private_ws, business_ws, http_client, instrument_provider = exec_client_builder(
        monkeypatch,
    )

    monkeypatch.setattr(client, "_await_account_registered", AsyncMock())

    # Act
    await client._connect()

    try:
        # Assert
        instrument_provider.initialize.assert_awaited_once()
        http_client.cache_instrument.assert_called_once_with(
            instrument_provider.instruments_pyo3.return_value[0],
        )
        http_client.request_account_state.assert_awaited_once()
        private_ws.connect.assert_awaited_once()
        private_ws.wait_until_active.assert_awaited_once_with(timeout_secs=30.0)
        business_ws.connect.assert_awaited_once()
        business_ws.wait_until_active.assert_awaited_once_with(timeout_secs=30.0)
        private_ws.subscribe_orders.assert_awaited_once_with(nautilus_pyo3.OKXInstrumentType.SPOT)
        business_ws.subscribe_orders_algo.assert_awaited_once_with(
            nautilus_pyo3.OKXInstrumentType.SPOT,
        )
        private_ws.subscribe_fills.assert_not_called()
        private_ws.subscribe_account.assert_awaited_once()
    finally:
        await client._disconnect()

    # Assert
    http_client.cancel_all_requests.assert_called_once()
    private_ws.close.assert_awaited_once()
    business_ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_generate_order_status_reports_converts_results(exec_client_builder, monkeypatch):
    # Arrange
    client, _, _, http_client, _ = exec_client_builder(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.OrderStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    pyo3_report = MagicMock()
    http_client.request_order_status_reports_with_errors.return_value = ([pyo3_report], [])

    command = GenerateOrderStatusReports(
        instrument_id=InstrumentId(Symbol("BTC-USD"), OKX_VENUE),
        start=None,
        end=None,
        open_only=True,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_order_status_reports(command)

    # Assert
    http_client.request_order_status_reports_with_errors.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_order_status_reports_handles_failure(exec_client_builder, monkeypatch):
    # Arrange
    client, _, _, http_client, _ = exec_client_builder(monkeypatch)
    http_client.request_order_status_reports.side_effect = TimeoutError("Synthetic read timeout")
    http_client.request_order_status_reports_with_errors = AsyncMock(
        side_effect=TimeoutError("Synthetic read timeout"),
    )

    command = GenerateOrderStatusReports(
        instrument_id=InstrumentId(Symbol("BTC-USD"), OKX_VENUE),
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    with pytest.raises(IncompleteOrderStatusReports) as exc:
        await client.generate_order_status_reports(command)
    assert exc.value.reports == []


@pytest.mark.asyncio
async def test_generate_fill_reports_converts_results(exec_client_builder, monkeypatch):
    # Arrange
    client, _, _, http_client, _ = exec_client_builder(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_fill_reports_with_errors.return_value = ([MagicMock()], [])

    command = GenerateFillReports(
        instrument_id=InstrumentId(Symbol("BTC-USD"), OKX_VENUE),
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_fill_reports(command)

    # Assert
    http_client.request_fill_reports_with_errors.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_position_status_reports_converts_results(exec_client_builder, monkeypatch):
    # Arrange
    # Use SWAP (derivatives) so positions are actually queried
    client, _, _, http_client, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={"instrument_types": (nautilus_pyo3.OKXInstrumentType.SWAP,)},
    )

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.PositionStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_position_status_reports_with_errors.return_value = ([MagicMock()], [])

    command = GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_position_status_reports(command)

    # Assert
    http_client.request_position_status_reports_with_errors.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_position_status_reports_spot_margin_uses_margin_inst_type(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange - spot margin mode with specific instrument query
    client, _, _, http_client, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.PositionStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_position_status_reports_with_errors.return_value = ([MagicMock()], [])

    command = GeneratePositionStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_position_status_reports(command)

    # Assert - verify MARGIN instType is used for spot margin positions
    http_client.request_position_status_reports_with_errors.assert_awaited_once()
    call_args = http_client.request_position_status_reports_with_errors.call_args
    assert call_args.kwargs["instrument_type"] == nautilus_pyo3.OKXInstrumentType.MARGIN
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_handle_fill_report_updates_venue_id_before_fill(exec_client_builder, monkeypatch):
    # Arrange
    client, _, _, http_client, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    order_list = TestExecStubs.limit_with_stop_market(instrument=instrument)
    stop_order = next(order for order in order_list.orders if isinstance(order, StopMarketOrder))

    submitted = TestEventStubs.order_submitted(order=stop_order)
    stop_order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=stop_order,
        venue_order_id=VenueOrderId("algo-venue-id"),
    )
    stop_order.apply(accepted)

    client._cache.add_order(stop_order, None, None)

    canonical_id = stop_order.client_order_id
    client._algo_order_ids[canonical_id] = "algo-venue-id"
    client._algo_order_instruments[canonical_id] = stop_order.instrument_id

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    new_venue_id = VenueOrderId("child-venue-id")
    fill_report = SimpleNamespace(
        client_order_id=stop_order.client_order_id,
        venue_order_id=new_venue_id,
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=stop_order.quantity,
        last_px=instrument.make_price(4018.5),
        commission=Money(0, instrument.quote_currency),
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456789,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report,
    )

    # Act
    client._handle_fill_report_pyo3(MagicMock())

    # Assert
    assert any(
        isinstance(event, OrderUpdated) and event.venue_order_id == new_venue_id
        for event in emitted_events
    )
    assert any(isinstance(event, OrderFilled) for event in emitted_events)
    assert client._cache.venue_order_id(stop_order.client_order_id) == new_venue_id
    assert canonical_id not in client._algo_order_ids

    http_client.request_fill_reports.assert_not_called()


@pytest.mark.asyncio
async def test_generate_position_status_reports_handles_failure(exec_client_builder, monkeypatch):
    # Arrange
    client, _, _, http_client, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={"instrument_types": (nautilus_pyo3.OKXInstrumentType.SWAP,)},
    )
    http_client.request_position_status_reports_with_errors.side_effect = Exception("boom")

    command = GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    with pytest.raises(Exception, match="boom"):
        await client.generate_position_status_reports(command)


@pytest.mark.asyncio
async def test_batch_cancel_orders_success(exec_client_builder, monkeypatch):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    # Create three limit orders
    order1 = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-batch-1"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(100),
        price=Price.from_str("1.0000"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    order2 = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-batch-2"),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(200),
        price=Price.from_str("1.0010"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    order3 = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-batch-3"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(150),
        price=Price.from_str("0.9990"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Apply submitted and accepted events
    for order in [order1, order2, order3]:
        submitted = TestEventStubs.order_submitted(order=order)
        order.apply(submitted)
        accepted = TestEventStubs.order_accepted(
            order=order,
            venue_order_id=VenueOrderId(f"venue-{order.client_order_id}"),
        )
        order.apply(accepted)
        client._cache.add_order(order, None, None)

    # Create batch cancel command
    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=[
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=order1.client_order_id,
                venue_order_id=order1.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=order2.client_order_id,
                venue_order_id=order2.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=order3.client_order_id,
                venue_order_id=order3.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
        ],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._batch_cancel_orders(command)

    # Assert
    private_ws.batch_cancel_orders.assert_awaited_once()
    call_args = private_ws.batch_cancel_orders.call_args[0][0]
    assert len(call_args) == 3
    # Verify all tuples have correct structure (instrument_id, client_order_id, venue_order_id)
    for item in call_args:
        assert len(item) == 3
        assert item[1] is not None  # client_order_id
        assert item[2] is not None  # venue_order_id


@pytest.mark.asyncio
async def test_batch_cancel_orders_filters_closed_orders(exec_client_builder, monkeypatch):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    # Create two orders - one open, one closed
    order_open = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-filter-open"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(100),
        price=Price.from_str("1.0000"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    order_closed = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-filter-closed"),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(200),
        price=Price.from_str("1.0010"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Apply events for open order
    submitted_open = TestEventStubs.order_submitted(order=order_open)
    order_open.apply(submitted_open)
    accepted_open = TestEventStubs.order_accepted(
        order=order_open,
        venue_order_id=VenueOrderId("venue-1"),
    )
    order_open.apply(accepted_open)

    # Apply events for closed order (canceled)
    submitted_closed = TestEventStubs.order_submitted(order=order_closed)
    order_closed.apply(submitted_closed)
    accepted_closed = TestEventStubs.order_accepted(
        order=order_closed,
        venue_order_id=VenueOrderId("venue-2"),
    )
    order_closed.apply(accepted_closed)
    canceled = TestEventStubs.order_canceled(order=order_closed)
    order_closed.apply(canceled)

    client._cache.add_order(order_open, None, None)
    client._cache.add_order(order_closed, None, None)

    # Create batch cancel command with both orders
    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=[
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=order_open.client_order_id,
                venue_order_id=order_open.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=order_closed.client_order_id,
                venue_order_id=order_closed.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
        ],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._batch_cancel_orders(command)

    # Assert - only one order should be sent (the open one)
    private_ws.batch_cancel_orders.assert_awaited_once()
    call_args = private_ws.batch_cancel_orders.call_args[0][0]
    assert len(call_args) == 1


@pytest.mark.asyncio
async def test_batch_cancel_orders_handles_order_not_in_cache(exec_client_builder, monkeypatch):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    # Create command for order that doesn't exist in cache
    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=[
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=TestIdStubs.client_order_id(),
                venue_order_id=VenueOrderId("venue-1"),
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
        ],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._batch_cancel_orders(command)

    # Assert - no orders should be sent
    private_ws.batch_cancel_orders.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_all_orders_uses_mass_cancel_when_configured(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={"use_mm_mass_cancel": True},
    )

    client._cache.add_instrument(instrument)

    command = CancelAllOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._cancel_all_orders(command)

    # Assert - should use mass cancel
    private_ws.mass_cancel_orders.assert_called_once()
    private_ws.batch_cancel_orders.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_all_orders_uses_batch_cancel_by_default(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={"use_mm_mass_cancel": False},
    )

    client._cache.add_instrument(instrument)

    # Create 5 open orders
    orders = []
    for i in range(5):
        order = LimitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId(f"O-cancel-all-{i}"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            price=Price.from_str(f"1.{i:04d}"),
            init_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        submitted = TestEventStubs.order_submitted(order=order)
        order.apply(submitted)
        accepted = TestEventStubs.order_accepted(
            order=order,
            venue_order_id=VenueOrderId(f"venue-{i}"),
        )
        order.apply(accepted)
        client._cache.add_order(order, None, None)
        orders.append(order)

    # Create batch cancel command to test batching logic
    cancels = [
        CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        for order in orders
    ]

    batch_command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=cancels,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act - test batch cancel with 5 orders (should be in one batch)
    await client._batch_cancel_orders(batch_command)

    # Assert - should use batch cancel with all 5 orders in one batch
    private_ws.batch_cancel_orders.assert_called_once()
    call_args = private_ws.batch_cancel_orders.call_args[0][0]
    assert len(call_args) == 5


@pytest.mark.asyncio
async def test_cancel_all_orders_batches_in_chunks_of_20(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    # Arrange
    client, private_ws, _, _, _ = exec_client_builder(monkeypatch)

    client._cache.add_instrument(instrument)

    # Create 45 open orders and add to cache
    orders = []
    for i in range(45):
        order = LimitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId(f"O-chunk-{i}"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            price=Price.from_str(f"1.{i:04d}"),
            init_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        submitted = TestEventStubs.order_submitted(order=order)
        order.apply(submitted)
        accepted = TestEventStubs.order_accepted(
            order=order,
            venue_order_id=VenueOrderId(f"venue-chunk-{i}"),
        )
        order.apply(accepted)
        client._cache.add_order(order, None, None)
        orders.append(order)

    # Create 45 cancel commands (should be split into 3 batches: 20 + 20 + 5)
    cancels = [
        CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        for order in orders
    ]

    # Test batching by processing cancels in chunks of 20
    batch_size = 20
    for i in range(0, len(cancels), batch_size):
        batch = cancels[i : i + batch_size]
        batch_command = BatchCancelOrders(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            cancels=batch,
            command_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        await client._batch_cancel_orders(batch_command)

    # Assert - should have 3 batch cancel calls
    assert private_ws.batch_cancel_orders.call_count == 3
    # First two batches should have 20 orders each
    first_batch = private_ws.batch_cancel_orders.call_args_list[0][0][0]
    second_batch = private_ws.batch_cancel_orders.call_args_list[1][0][0]
    third_batch = private_ws.batch_cancel_orders.call_args_list[2][0][0]
    assert len(first_batch) == 20
    assert len(second_batch) == 20
    assert len(third_batch) == 5


@pytest.mark.asyncio
async def test_cancel_all_orders_handles_mixed_regular_and_algo_orders(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    """
    Test that cancel_all separates regular orders (batch via WebSocket) from algo orders
    (individual via REST API).
    """
    # Arrange
    client, private_ws, _, http_client, _ = exec_client_builder(monkeypatch)

    client._cache.add_instrument(instrument)

    # Create 3 regular orders
    regular_orders = []
    for i in range(3):
        order = LimitOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=ClientOrderId(f"O-regular-{i}"),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100),
            price=Price.from_str(f"1.{i:04d}"),
            init_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        submitted = TestEventStubs.order_submitted(order=order)
        order.apply(submitted)
        accepted = TestEventStubs.order_accepted(
            order=order,
            venue_order_id=VenueOrderId(f"venue-regular-{i}"),
        )
        order.apply(accepted)
        client._cache.add_order(order, None, None)
        regular_orders.append(order)

    # Create 2 algo orders and register them in _algo_order_ids
    algo_client_ids = []
    for i in range(2):
        client_id = ClientOrderId(f"O-algo-{i}")
        algo_client_ids.append(client_id)
        # Register as algo order (simulating orders submitted via _submit_algo_order_http)
        client._algo_order_ids[client_id] = f"okx-algo-id-{i}"
        client._algo_order_instruments[client_id] = instrument.id

    # Mock the HTTP cancel_algo_order call
    http_client.cancel_algo_order = AsyncMock()

    # Act - Create batch with regular orders only (algo orders should be skipped)
    regular_cancels = [
        CancelOrder(
            trader_id=TestIdStubs.trader_id(),
            strategy_id=TestIdStubs.strategy_id(),
            instrument_id=instrument.id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=TestIdStubs.uuid(),
            ts_init=0,
        )
        for order in regular_orders
    ]

    batch_command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=regular_cancels,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    await client._batch_cancel_orders(batch_command)

    # Cancel algo orders via fallback
    for client_id in algo_client_ids:
        await client._cancel_algo_order_fallback(
            client_order_id=client_id,
            instrument_id=instrument.id,
            algo_id=client._algo_order_ids[client_id],
        )

    # Assert - regular orders should be batch cancelled via WebSocket
    private_ws.batch_cancel_orders.assert_called_once()
    call_args = private_ws.batch_cancel_orders.call_args[0][0]
    assert len(call_args) == 3  # Only the 3 regular orders

    # Assert - algo orders should be cancelled via REST API (2 calls)
    assert http_client.cancel_algo_order.call_count == 2


# ==============================================================================

# Quote Quantity Order Tests
# ==============================================================================



@pytest.mark.asyncio
async def test_spot_margin_market_buy_denies_order_without_quote_quantity(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    # Create market buy order without quote_quantity
    order = TestExecStubs.market_order(
        instrument=instrument,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("20.0"),  # Base quantity without quote_quantity=True
    )

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    from nautilus_trader.execution.messages import SubmitOrder

    command = SubmitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        order=order,
        position_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._submit_order(command)

    # Assert
    assert any(isinstance(event, OrderDenied) for event in emitted_events)
    denied_event = next(event for event in emitted_events if isinstance(event, OrderDenied))
    assert "quote-denominated quantities" in denied_event.reason


@pytest.mark.asyncio
async def test_spot_margin_market_buy_quote_quantity_converts_on_first_fill(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    # Create market buy order with quote_quantity=True
    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-quote-qty"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("20.0"),  # 20 USDT
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        quote_quantity=True,  # Key: quote quantity order
    )

    # Simulate order submission and acceptance
    submitted = TestEventStubs.order_submitted(order=order)
    order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=order,
        venue_order_id=VenueOrderId("venue-123"),
    )
    order.apply(accepted)

    client._cache.add_order(order, None, None)

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    # Create fill report: gross = 0.005230 ETH, commission = 0.00000523 ETH
    # Net = 0.005230 - 0.00000523 = 0.00522477 ETH
    fill_report = SimpleNamespace(
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=Quantity.from_str("0.005230"),  # Gross fill in base currency
        last_px=Price.from_str("3825.25"),
        commission=Money.from_str("0.00000523 ETH"),  # Fee in base currency
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456789,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report,
    )

    # Act
    client._handle_fill_report_pyo3(MagicMock())

    # Assert
    # Should generate OrderUpdated with net quantity
    assert any(isinstance(event, OrderUpdated) for event in emitted_events)
    updated_event = next(event for event in emitted_events if isinstance(event, OrderUpdated))
    # Net quantity = 0.005230 - 0.00000523 ≈ 0.005225 (rounded to precision 6)
    assert updated_event.quantity == Quantity.from_str("0.005225")

    # Should generate OrderFilled with net last_qty
    assert any(isinstance(event, OrderFilled) for event in emitted_events)
    filled_event = next(event for event in emitted_events if isinstance(event, OrderFilled))
    assert filled_event.last_qty == Quantity.from_str("0.005225")

    # Order should no longer be quote_quantity
    assert not order.is_quote_quantity


@pytest.mark.asyncio
async def test_spot_margin_market_buy_quote_quantity_handles_quote_currency_commission(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-quote-qty-2"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("20.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        quote_quantity=True,
    )

    submitted = TestEventStubs.order_submitted(order=order)
    order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=order,
        venue_order_id=VenueOrderId("venue-456"),
    )
    order.apply(accepted)

    client._cache.add_order(order, None, None)

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    # Create fill with commission in QUOTE currency (unusual but possible)
    fill_report = SimpleNamespace(
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=Quantity.from_str("0.005230"),
        last_px=Price.from_str("3825.25"),
        commission=Money.from_str("0.02 USDT"),  # Fee in QUOTE currency
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456789,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report,
    )

    # Act
    client._handle_fill_report_pyo3(MagicMock())

    # Assert
    # Should generate OrderUpdated but NOT subtract commission (different currency)
    assert any(isinstance(event, OrderUpdated) for event in emitted_events)
    updated_event = next(event for event in emitted_events if isinstance(event, OrderUpdated))
    # Quantity should be gross (not adjusted for commission)
    assert updated_event.quantity == Quantity.from_str("0.005230")

    # OrderFilled should also have gross last_qty
    assert any(isinstance(event, OrderFilled) for event in emitted_events)
    filled_event = next(event for event in emitted_events if isinstance(event, OrderFilled))
    assert filled_event.last_qty == Quantity.from_str("0.005230")


@pytest.mark.asyncio
async def test_regular_market_buy_not_affected_by_quote_quantity_logic(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange - without use_spot_margin, should not trigger quote quantity validation
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": False,  # Regular spot trading
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    # Market buy order WITHOUT quote_quantity should be allowed for regular spot
    order = TestExecStubs.market_order(
        instrument=instrument,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.1"),  # Base quantity
    )

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    from nautilus_trader.execution.messages import SubmitOrder

    command = SubmitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        order=order,
        position_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._submit_order(command)

    # Assert - should not be denied
    assert not any(isinstance(event, OrderDenied) for event in emitted_events)


@pytest.mark.asyncio
async def test_spot_margin_market_buy_quote_quantity_handles_partial_fills(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Arrange
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    # Create market buy order with quote_quantity=True for 20 USDT
    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-partial-fills"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("20.0"),  # 20 USDT
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        quote_quantity=True,
    )

    # Simulate order submission and acceptance
    submitted = TestEventStubs.order_submitted(order=order)
    order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=order,
        venue_order_id=VenueOrderId("venue-partial"),
    )
    order.apply(accepted)

    client._cache.add_order(order, None, None)

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    # First partial fill: 10 USDT worth = 0.002615 ETH @ 3825.25, commission = 0.00000261 ETH
    # Net = 0.002615 - 0.00000261 = 0.00261239 ETH
    fill_report_1 = SimpleNamespace(
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=Quantity.from_str("0.002615"),  # Gross fill in base currency
        last_px=Price.from_str("3825.25"),
        commission=Money.from_str("0.00000261 ETH"),  # Fee in base currency
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456789,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report_1,
    )

    # Act - First fill
    client._handle_fill_report_pyo3(MagicMock())

    # Assert - First fill should generate OrderUpdated with net quantity
    assert any(isinstance(event, OrderUpdated) for event in emitted_events)
    updated_event = next(event for event in emitted_events if isinstance(event, OrderUpdated))
    assert updated_event.quantity == Quantity.from_str("0.002612")  # Net rounded to precision 6

    # First fill should have net last_qty
    filled_events = [e for e in emitted_events if isinstance(e, OrderFilled)]
    assert len(filled_events) == 1
    assert filled_events[0].last_qty == Quantity.from_str("0.002612")

    # Clear events for second fill
    emitted_events.clear()

    # Second partial fill: Another 10 USDT worth = 0.002615 ETH @ 3825.25, commission = 0.00000261 ETH
    # Net should also be = 0.002615 - 0.00000261 = 0.00261239 ETH
    fill_report_2 = SimpleNamespace(
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=Quantity.from_str("0.002615"),  # Gross fill in base currency
        last_px=Price.from_str("3825.25"),
        commission=Money.from_str("0.00000261 ETH"),  # Fee in base currency
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456790,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report_2,
    )

    # Act - Second fill
    client._handle_fill_report_pyo3(MagicMock())

    # Assert - Second fill should NOT generate OrderUpdated (already converted)
    assert not any(isinstance(event, OrderUpdated) for event in emitted_events)

    # Second fill should also have net last_qty (adjusted for commission)
    filled_events = [e for e in emitted_events if isinstance(e, OrderFilled)]
    assert len(filled_events) == 1
    # This should be NET (0.002615 - 0.00000261 = 0.00261239 ≈ 0.002612)
    # NOT gross (0.002615)
    assert filled_events[0].last_qty == Quantity.from_str("0.002612")


@pytest.mark.asyncio
async def test_fill_with_changed_venue_order_id_generates_update(
    exec_client_builder,
    monkeypatch,
    eth_usdt_instrument,
):
    # Test that when a fill arrives with a different venue_order_id (e.g., algo order
    # transitions to live order), we correctly generate OrderUpdated with venue_order_id_modified=True
    # Arrange
    client, _, _, _, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": True,
            "margin_mode": nautilus_pyo3.OKXMarginMode.CROSS,
        },
    )

    instrument = eth_usdt_instrument
    client._cache.add_instrument(instrument)

    # Create and accept order with initial venue_order_id
    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-venue-change"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("20.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        quote_quantity=True,
    )

    submitted = TestEventStubs.order_submitted(order=order)
    order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=order,
        venue_order_id=VenueOrderId("algo-123"),  # Initial algo order ID
    )
    order.apply(accepted)

    client._cache.add_order(order, None, None)

    emitted_events: list = []

    def _capture(event):
        emitted_events.append(event)

    monkeypatch.setattr(client, "_send_order_event", _capture)

    # Fill arrives with DIFFERENT venue_order_id (algo transitioned to live)
    fill_report = SimpleNamespace(
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("live-456"),  # Different venue ID
        venue_position_id=None,
        trade_id=TestIdStubs.trade_id(),
        last_qty=Quantity.from_str("0.005230"),
        last_px=Price.from_str("3825.25"),
        commission=Money.from_str("0.00000523 ETH"),
        liquidity_side=LiquiditySide.TAKER,
        ts_event=123456789,
    )
    monkeypatch.setattr(
        "nautilus_trader.adapters.okx.execution.FillReport.from_pyo3",
        lambda _obj: fill_report,
    )

    # Act
    client._handle_fill_report_pyo3(MagicMock())

    # Assert
    # Should generate ONE OrderUpdated event that handles both:
    # 1. Quote quantity conversion (quantity updated)
    # 2. Venue order ID change (venue_order_id updated with venue_order_id_modified=True)
    updated_events = [e for e in emitted_events if isinstance(e, OrderUpdated)]
    assert len(updated_events) == 1

    # The single update should have the new venue_order_id
    assert updated_events[0].venue_order_id == VenueOrderId("live-456")
    # And the net quantity from the quote quantity conversion
    assert updated_events[0].quantity == Quantity.from_str("0.005225")

    # Should also generate OrderFilled with correct venue_order_id
    filled_events = [e for e in emitted_events if isinstance(e, OrderFilled)]
    assert len(filled_events) == 1
    assert filled_events[0].venue_order_id == VenueOrderId("live-456")
    # And net last_qty (commission adjusted)
    assert filled_events[0].last_qty == Quantity.from_str("0.005225")


@pytest.mark.asyncio
async def test_clear_order_state_removes_canonical_entry(exec_client_builder, monkeypatch):
    """
    Test that _clear_order_state removes the canonical client order ID from aliases.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical = ClientOrderId("PARENT-001")

    # Simulate alias registration (canonical→canonical)
    client._client_id_aliases[canonical] = canonical

    # Act
    client._clear_order_state(canonical)

    # Assert
    assert canonical not in client._client_id_aliases


@pytest.mark.asyncio
async def test_clear_order_state_removes_child_alias_pointing_to_canonical(
    exec_client_builder,
    monkeypatch,
):
    """
    Test that _clear_order_state removes child aliases that point to the canonical ID.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical = ClientOrderId("PARENT-001")
    child = ClientOrderId("CHILD-001")

    # Simulate alias registration
    client._client_id_aliases[canonical] = canonical  # parent→parent
    client._client_id_aliases[child] = canonical  # child→parent

    # Act
    client._clear_order_state(canonical)

    # Assert - both entries should be removed
    assert canonical not in client._client_id_aliases
    assert child not in client._client_id_aliases
    assert len(client._client_id_aliases) == 0


@pytest.mark.asyncio
async def test_clear_order_state_does_not_affect_unrelated_aliases(
    exec_client_builder,
    monkeypatch,
):
    """
    Test that _clear_order_state only removes aliases for the target order.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical1 = ClientOrderId("PARENT-001")
    child1 = ClientOrderId("CHILD-001")
    canonical2 = ClientOrderId("PARENT-002")
    child2 = ClientOrderId("CHILD-002")

    # Register two separate order alias sets
    client._client_id_aliases[canonical1] = canonical1
    client._client_id_aliases[child1] = canonical1
    client._client_id_aliases[canonical2] = canonical2
    client._client_id_aliases[child2] = canonical2

    # Act - only clear first order
    client._clear_order_state(canonical1)

    # Assert - first order fully removed, second order intact
    assert canonical1 not in client._client_id_aliases
    assert child1 not in client._client_id_aliases
    assert canonical2 in client._client_id_aliases
    assert child2 in client._client_id_aliases
    assert len(client._client_id_aliases) == 2


@pytest.mark.asyncio
async def test_clear_order_state_clears_children_map(exec_client_builder, monkeypatch):
    """
    Test that _clear_order_state removes entries from _client_id_children.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical = ClientOrderId("PARENT-001")
    child = ClientOrderId("CHILD-001")

    # Simulate registration
    client._client_id_aliases[canonical] = canonical
    client._client_id_aliases[child] = canonical
    client._client_id_children[canonical] = child

    # Act
    client._clear_order_state(canonical)

    # Assert
    assert canonical not in client._client_id_children


@pytest.mark.asyncio
async def test_clear_order_state_clears_algo_metadata(exec_client_builder, monkeypatch):
    """
    Test that _clear_order_state removes algo order metadata.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical = ClientOrderId("PARENT-001")
    algo_id = "algo-123"
    instrument_id = InstrumentId(Symbol("BTC-USDT-SWAP"), OKX_VENUE)

    # Simulate algo order state
    client._client_id_aliases[canonical] = canonical
    client._algo_order_ids[canonical] = algo_id
    client._algo_order_instruments[canonical] = instrument_id

    # Act
    client._clear_order_state(canonical)

    # Assert
    assert canonical not in client._algo_order_ids
    assert canonical not in client._algo_order_instruments


@pytest.mark.asyncio
async def test_clear_order_state_handles_child_id_input(exec_client_builder, monkeypatch):
    """
    Test that _clear_order_state resolves child ID to canonical before cleanup.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    canonical = ClientOrderId("PARENT-001")
    child = ClientOrderId("CHILD-001")

    # Simulate alias registration
    client._client_id_aliases[canonical] = canonical
    client._client_id_aliases[child] = canonical
    client._client_id_children[canonical] = child

    # Act - pass child ID, should resolve to canonical
    client._clear_order_state(child)

    # Assert - all entries for this order should be removed
    assert canonical not in client._client_id_aliases
    assert child not in client._client_id_aliases
    assert canonical not in client._client_id_children


@pytest.mark.asyncio
async def test_child_id_reuse_after_cleanup_routes_correctly(exec_client_builder, monkeypatch):
    """
    Integration test: After terminal order cleanup, reusing the same child ID
    should NOT route to the old canonical ID.
    """
    # Arrange
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    old_canonical = ClientOrderId("OLD-PARENT-001")
    child = ClientOrderId("CHILD-001")  # Same child ID to be reused
    new_canonical = ClientOrderId("NEW-PARENT-002")

    # First order: register old_canonical with child
    client._client_id_aliases[old_canonical] = old_canonical
    client._client_id_aliases[child] = old_canonical
    client._client_id_children[old_canonical] = child

    # Terminal event - cleanup old order
    client._clear_order_state(old_canonical)

    # New order: reuse same child ID with new canonical
    client._client_id_aliases[new_canonical] = new_canonical
    client._client_id_aliases[child] = new_canonical
    client._client_id_children[new_canonical] = child

    # Assert - child should now route to new canonical, not old
    resolved = client._canonical_client_order_id(child)
    assert resolved == new_canonical
    assert resolved != old_canonical


@pytest.mark.asyncio
async def test_batch_cancel_orders_separates_algo_orders(exec_client_builder, monkeypatch):
    """
    Test that batch cancel correctly separates algo orders from regular orders.

    Algo orders should be sent to HTTP cancel_algo_orders, regular to WebSocket.

    """
    # Arrange
    client, private_ws, _, http_client, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    # Create a regular limit order
    regular_order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-regular-001"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(100),
        price=Price.from_str("1.0000"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    submitted = TestEventStubs.order_submitted(order=regular_order)
    regular_order.apply(submitted)
    accepted = TestEventStubs.order_accepted(
        order=regular_order,
        venue_order_id=VenueOrderId("venue-regular-1"),
    )
    regular_order.apply(accepted)
    client._cache.add_order(regular_order, None, None)

    # Create a stop market order (algo order)
    algo_order = StopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-algo-001"),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(100),
        trigger_price=Price.from_str("0.9900"),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        expire_time_ns=0,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    submitted_algo = TestEventStubs.order_submitted(order=algo_order)
    algo_order.apply(submitted_algo)
    accepted_algo = TestEventStubs.order_accepted(
        order=algo_order,
        venue_order_id=VenueOrderId("algo-id-123"),
    )
    algo_order.apply(accepted_algo)
    client._cache.add_order(algo_order, None, None)

    # Register the algo order in _algo_order_ids (simulating what happens on order accept)
    client._algo_order_ids[algo_order.client_order_id] = "algo-id-123"
    client._algo_order_instruments[algo_order.client_order_id] = instrument.id

    # Mock the HTTP cancel_algo_orders method
    http_client.cancel_algo_orders = AsyncMock(return_value=[])

    # Create batch cancel command with both orders
    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=[
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=regular_order.client_order_id,
                venue_order_id=regular_order.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=algo_order.client_order_id,
                venue_order_id=algo_order.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
        ],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._batch_cancel_orders(command)

    # Assert - regular order sent to WebSocket, algo order sent to HTTP
    private_ws.batch_cancel_orders.assert_awaited_once()
    ws_call_args = private_ws.batch_cancel_orders.call_args[0][0]
    assert len(ws_call_args) == 1  # Only regular order

    http_client.cancel_algo_orders.assert_awaited_once()
    http_call_args = http_client.cancel_algo_orders.call_args[0][0]
    assert len(http_call_args) == 1  # Only algo order
    assert http_call_args[0][1] == "algo-id-123"  # algo_id

    # Verify algo order tracking was cleaned up
    assert algo_order.client_order_id not in client._algo_order_ids
    assert algo_order.client_order_id not in client._algo_order_instruments


@pytest.mark.asyncio
async def test_batch_cancel_orders_only_algo_orders(exec_client_builder, monkeypatch):
    """
    Test that batch cancel with only algo orders doesn't call WebSocket batch cancel.
    """
    # Arrange
    client, private_ws, _, http_client, _ = exec_client_builder(monkeypatch)

    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    client._cache.add_instrument(instrument)

    # Create two stop market orders (algo orders)
    algo_order1 = StopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-algo-001"),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(100),
        trigger_price=Price.from_str("0.9900"),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        expire_time_ns=0,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    submitted1 = TestEventStubs.order_submitted(order=algo_order1)
    algo_order1.apply(submitted1)
    accepted1 = TestEventStubs.order_accepted(
        order=algo_order1,
        venue_order_id=VenueOrderId("algo-id-1"),
    )
    algo_order1.apply(accepted1)
    client._cache.add_order(algo_order1, None, None)
    client._algo_order_ids[algo_order1.client_order_id] = "algo-id-1"
    client._algo_order_instruments[algo_order1.client_order_id] = instrument.id

    algo_order2 = StopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-algo-002"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(200),
        trigger_price=Price.from_str("1.0100"),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        expire_time_ns=0,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    submitted2 = TestEventStubs.order_submitted(order=algo_order2)
    algo_order2.apply(submitted2)
    accepted2 = TestEventStubs.order_accepted(
        order=algo_order2,
        venue_order_id=VenueOrderId("algo-id-2"),
    )
    algo_order2.apply(accepted2)
    client._cache.add_order(algo_order2, None, None)
    client._algo_order_ids[algo_order2.client_order_id] = "algo-id-2"
    client._algo_order_instruments[algo_order2.client_order_id] = instrument.id

    http_client.cancel_algo_orders = AsyncMock(return_value=[])

    command = BatchCancelOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        cancels=[
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=algo_order1.client_order_id,
                venue_order_id=algo_order1.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
            CancelOrder(
                trader_id=TestIdStubs.trader_id(),
                strategy_id=TestIdStubs.strategy_id(),
                instrument_id=instrument.id,
                client_order_id=algo_order2.client_order_id,
                venue_order_id=algo_order2.venue_order_id,
                command_id=TestIdStubs.uuid(),
                ts_init=0,
            ),
        ],
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    await client._batch_cancel_orders(command)

    # Assert - WebSocket batch cancel NOT called, HTTP batch cancel called once
    private_ws.batch_cancel_orders.assert_not_awaited()

    http_client.cancel_algo_orders.assert_awaited_once()
    http_call_args = http_client.cancel_algo_orders.call_args[0][0]
    assert len(http_call_args) == 2  # Both algo orders


def _order_report(order, account_id, status=OrderStatus.ACCEPTED):
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("1"),
        order_side=order.side,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        order_status=status,
        price=order.price,
        quantity=order.quantity,
        filled_qty=Quantity.zero(order.quantity.precision),
        report_id=TestIdStubs.uuid(),
        ts_accepted=0,
        ts_last=0,
        ts_init=0,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "row", "conversion", "cancel"])
async def test_targeted_query_failure_is_not_absence(
    exec_client_builder,
    monkeypatch,
    instrument,
    failure,
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    command = GenerateOrderStatusReport(
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-1"),
        venue_order_id=VenueOrderId("1"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    if failure in ("transport", "cancel"):
        error = TimeoutError() if failure == "transport" else asyncio.CancelledError()
        http.request_order_status_reports.side_effect = error
        http.request_order_status_reports_with_errors.side_effect = error
    elif failure == "row":
        http.request_order_status_reports_with_errors.return_value = ([], ["row:conversion"])
    else:
        http.request_order_status_reports.return_value = [object()]
        http.request_order_status_reports_with_errors.return_value = ([object()], [])

    expected = asyncio.CancelledError if failure == "cancel" else IncompleteOrderStatusReports
    with pytest.raises(expected):
        await client.generate_order_status_report(command)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "row", "conversion"])
async def test_periodic_okx_incomplete_query_keeps_positive_reports(
    exec_client_builder,
    monkeypatch,
    instrument,
    event_loop,
    msgbus,
    cache,
    live_clock,
    failure,
):
    client, _, _, http, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "instrument_types": (
                nautilus_pyo3.OKXInstrumentType.SPOT,
                nautilus_pyo3.OKXInstrumentType.SWAP,
            ),
        },
    )
    engine = LiveExecutionEngine(
        loop=event_loop,
        msgbus=MessageBus(trader_id=TestIdStubs.trader_id(), clock=live_clock),
        cache=cache,
        clock=live_clock,
        config=LiveExecEngineConfig(open_check_open_only=False, open_check_threshold_ms=0),
    )
    engine.register_client(client)
    cache.add_instrument(instrument)
    orders = [
        TestExecStubs.limit_order(instrument=instrument, client_order_id=ClientOrderId(f"O-{i}"))
        for i in (1, 2)
    ]
    for order in orders:
        cache.add_order(order, client_id=client.id)
        order.apply(TestEventStubs.order_submitted(order, account_id=client.account_id))
        order.apply(TestEventStubs.order_accepted(order, account_id=client.account_id))
        cache.update_order(order)
    positive = _order_report(orders[0], client.account_id, OrderStatus.CANCELED).to_pyo3()
    if failure == "transport":
        http.request_order_status_reports_with_errors.side_effect = [
            ([positive], []),
            TimeoutError(),
        ]
    elif failure == "row":
        http.request_order_status_reports_with_errors.side_effect = [
            ([positive], ["row:conversion"]),
            ([], []),
        ]
    else:
        http.request_order_status_reports_with_errors.side_effect = [
            ([object(), positive], []),
            ([], []),
        ]

    try:
        await engine._check_orders_consistency()
        assert orders[0].status == OrderStatus.CANCELED
        assert orders[1].status == OrderStatus.ACCEPTED
        assert orders[1].client_order_id not in engine._recon_check_retries
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("matches", [False, True])
async def test_targeted_incomplete_query_can_return_a_positive_match(
    exec_client_builder,
    monkeypatch,
    instrument,
    matches,
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    order = TestExecStubs.limit_order(instrument=instrument, client_order_id=ClientOrderId("O-1"))
    report = _order_report(order, client.account_id).to_pyo3()
    http.request_order_status_reports_with_errors.return_value = ([report], ["row:conversion"])
    command = GenerateOrderStatusReport(
        instrument_id=instrument.id,
        client_order_id=order.client_order_id if matches else ClientOrderId("O-2"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    if matches:
        result = await client.generate_order_status_report(command)
        assert result.client_order_id == order.client_order_id
    else:
        with pytest.raises(IncompleteOrderStatusReports) as exc:
            await client.generate_order_status_report(command)
        assert [r.client_order_id for r in exc.value.reports] == [order.client_order_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_targeted_algo_fallback_preserves_failure(
    exec_client_builder,
    monkeypatch,
    instrument,
    failure,
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    other = TestExecStubs.limit_order(instrument=instrument, client_order_id=ClientOrderId("O-2"))
    http.request_order_status_reports_with_errors.return_value = (
        [_order_report(other, client.account_id).to_pyo3()],
        [],
    )
    http.request_algo_order_status_reports_with_errors = AsyncMock(return_value=([], []))
    if failure:
        http.request_algo_order_status_reports_with_errors.side_effect = TimeoutError(
            "Synthetic read timeout"
        )
    command = GenerateOrderStatusReport(
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-1"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    if failure:
        with pytest.raises(TimeoutError):
            await client.generate_order_status_report(command)
    else:
        assert await client.generate_order_status_report(command) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["orders", "fills", "positions", "margin", "wallet"])
@pytest.mark.parametrize("failure", ["transport", "conversion", "cancel", "row", "empty"])
async def test_mass_status_requires_every_reader_to_complete(
    exec_client_builder,
    monkeypatch,
    instrument,
    reader,
    failure,
):
    is_wallet = reader == "wallet"
    client, _, _, http, provider = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "instrument_types": (
                nautilus_pyo3.OKXInstrumentType.SPOT
                if reader in ("wallet", "margin")
                else nautilus_pyo3.OKXInstrumentType.SWAP,
            ),
            "use_spot_margin": reader == "margin",
            "use_spot_cash_position_reports": is_wallet,
        },
    )
    provider.get_all.return_value = {instrument.id: instrument}
    for name in ("order_status", "fill", "position_status"):
        setattr(http, f"request_{name}_reports_with_errors", AsyncMock(return_value=([], [])))
    http.get_balance = AsyncMock(return_value=[])
    method = {
        "orders": "request_order_status_reports",
        "fills": "request_fill_reports",
        "positions": "request_position_status_reports",
        "margin": "request_position_status_reports",
        "wallet": "get_balance",
    }[reader]
    legacy = getattr(http, method)
    strict = legacy if is_wallet else getattr(http, method + "_with_errors")
    if failure in ("transport", "cancel"):
        error = (
            TimeoutError("Synthetic read timeout")
            if failure == "transport"
            else asyncio.CancelledError()
        )
        legacy.side_effect = strict.side_effect = error
    elif failure == "conversion":
        legacy.return_value = [object()]
        strict.return_value = [object()] if is_wallet else ([object()], [])
    elif failure == "row":
        if is_wallet:
            http.get_balance.return_value = [SimpleNamespace(ccy="BTC", cash_bal="NaN", liab="0")]
        else:
            strict.return_value = ([], ["row:conversion"])

    if failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await client.generate_mass_status()
    elif failure == "empty":
        assert await client.generate_mass_status() is not None
    else:
        assert await client.generate_mass_status() is None


@pytest.mark.asyncio
async def test_mass_status_wallet_quantity_failure_is_not_flat(
    exec_client_builder,
    monkeypatch,
    instrument,
):
    client, _, _, http, provider = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": False,
            "use_spot_cash_position_reports": True,
        },
    )
    provider.get_all.return_value = {instrument.id: instrument}
    http.request_fill_reports_with_errors = AsyncMock(return_value=([], []))
    http.get_balance = AsyncMock(
        return_value=[SimpleNamespace(ccy="BTC", cash_bal="1e100", liab="0")]
    )
    assert await client.generate_mass_status() is None


@pytest.mark.asyncio
async def test_overlapping_mass_status_success_cannot_certify_failure(
    exec_client_builder, monkeypatch
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fills(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            raise TimeoutError("Synthetic read timeout")
        return [], []

    http.request_fill_reports_with_errors.side_effect = fills
    failed = asyncio.create_task(client.generate_mass_status())
    await started.wait()
    try:
        complete = await client.generate_mass_status()
        assert complete is not None
        assert not failed.done()
    finally:
        release.set()
    assert await failed is None
    assert await client.generate_mass_status() is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("balance", ["0", "0.00000001"])
async def test_wallet_qualified_zero_remains_flat(
    exec_client_builder, monkeypatch, instrument, balance
):
    client, _, _, http, _ = exec_client_builder(
        monkeypatch,
        config_kwargs={
            "use_spot_margin": False,
            "use_spot_cash_position_reports": True,
        },
    )
    http.get_balance = AsyncMock(
        return_value=[SimpleNamespace(ccy="BTC", cash_bal=balance, liab="0")]
    )
    command = GeneratePositionStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    reports = await client.generate_position_status_reports(command)
    assert len(reports) == 1
    assert reports[0].quantity == Quantity.zero(instrument.size_precision)


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ["periodic", "inflight"])
@pytest.mark.parametrize("regular_rows", ["empty", "unrelated"])
@pytest.mark.parametrize("outcome", ["client_match", "algo_match", "error", "absent"])
async def test_known_algo_resolution_does_not_depend_on_regular_orders(
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
    live_clock,
    caller,
    regular_rows,
    outcome,
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    order = StopMarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-ALGO"),
        order_side=OrderSide.SELL,
        quantity=instrument.make_qty(1),
        trigger_price=instrument.make_price(100),
        trigger_type=TriggerType.LAST_PRICE,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, client_id=client.id)
    order.apply(TestEventStubs.order_submitted(order, account_id=client.account_id, ts_event=0))
    if caller == "periodic":
        order.apply(TestEventStubs.order_accepted(order, account_id=client.account_id, ts_event=0))
    cache.update_order(order)
    before = list(order.events)
    client._algo_order_ids[order.client_order_id] = "9001"
    report = OrderStatusReport(
        account_id=client.account_id,
        instrument_id=instrument.id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("9001"),
        order_side=order.side,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        order_status=OrderStatus.ACCEPTED,
        quantity=order.quantity,
        filled_qty=Quantity.zero(instrument.size_precision),
        trigger_price=order.trigger_price,
        trigger_type=TriggerType.LAST_PRICE,
        report_id=TestIdStubs.uuid(),
        ts_accepted=0,
        ts_last=0,
        ts_init=0,
    )
    rows = []
    if regular_rows == "unrelated":
        other = TestExecStubs.limit_order(
            instrument=instrument, client_order_id=ClientOrderId("O-OTHER")
        )
        other_report = _order_report(other, client.account_id)
        other_report.venue_order_id = VenueOrderId("9002")
        rows = [other_report.to_pyo3()]
    http.request_order_status_reports_with_errors.return_value = (rows, [])

    async def algo_read(**kwargs):
        if outcome == "error":
            raise TimeoutError("Synthetic algo read failure")
        matched = (outcome == "client_match" and kwargs["algo_client_order_id"] is not None) or (
            outcome == "algo_match" and kwargs["algo_id"] == "9001"
        )
        return ([report.to_pyo3()] if matched else [], [])

    http.request_algo_order_status_reports_with_errors = AsyncMock(side_effect=algo_read)
    engine = LiveExecutionEngine(
        loop=asyncio.get_running_loop(),
        msgbus=MessageBus(trader_id=TestIdStubs.trader_id(), clock=live_clock),
        cache=cache,
        clock=live_clock,
        config=LiveExecEngineConfig(
            open_check_open_only=False, open_check_threshold_ms=0, single_order_query_delay_ms=0
        ),
    )
    engine.register_client(client)
    engine._recon_check_retries[order.client_order_id] = (
        engine.open_check_missing_retries
        if caller == "periodic"
        else engine.inflight_check_max_retries
    )
    try:
        if caller == "periodic":
            await engine._check_orders_consistency()
        else:
            await engine._check_inflight_orders()
        if outcome == "absent":
            assert order.status == OrderStatus.REJECTED
        elif outcome == "error":
            assert order.events == before
        else:
            assert order.status == OrderStatus.ACCEPTED
        http.request_algo_order_status_reports_with_errors.assert_awaited()
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ["periodic", "inflight"])
@pytest.mark.parametrize("positive_route", ["targeted_incomplete", "bulk_incomplete"])
async def test_retained_positive_on_other_targeted_query(
    exec_client_builder, monkeypatch, instrument, cache, live_clock, caller, positive_route
):
    client, _, _, http, _ = exec_client_builder(monkeypatch)
    cache.add_instrument(instrument)
    engine = LiveExecutionEngine(
        loop=asyncio.get_running_loop(),
        msgbus=MessageBus(trader_id=TestIdStubs.trader_id(), clock=live_clock),
        cache=cache,
        clock=live_clock,
        config=LiveExecEngineConfig(
            open_check_open_only=False, open_check_threshold_ms=0, single_order_query_delay_ms=0
        ),
    )
    engine.register_client(client)
    b = TestExecStubs.limit_order(instrument=instrument, client_order_id=ClientOrderId("O-B"))
    cache.add_order(b, client_id=client.id)
    b.apply(TestEventStubs.order_submitted(b, account_id=client.account_id, ts_event=0))
    if caller == "periodic":
        b.apply(
            TestEventStubs.order_accepted(
                b, account_id=client.account_id, venue_order_id=VenueOrderId("B"), ts_event=0
            )
        )
    cache.update_order(b)
    engine._recon_check_retries[b.client_order_id] = (
        engine.open_check_missing_retries
        if caller == "periodic"
        else engine.inflight_check_max_retries
    )
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def read(**kwargs):
        nonlocal calls
        calls += 1
        # Initial periodic bulk pass has no positive observation.
        if caller == "periodic" and calls == 1:
            return [], []
        entered.set()
        await release.wait()
        return [], []

    http.request_order_status_reports_with_errors.side_effect = read
    http.request_algo_order_status_reports_with_errors = AsyncMock(return_value=([], []))
    task = asyncio.create_task(
        engine._check_orders_consistency()
        if caller == "periodic"
        else engine._check_inflight_orders()
    )
    await asyncio.wait_for(entered.wait(), 1)
    a = TestExecStubs.limit_order(instrument=instrument, client_order_id=ClientOrderId("O-A"))
    cache.add_order(a, client_id=client.id)
    a.apply(TestEventStubs.order_submitted(a, account_id=client.account_id, ts_event=0))
    a.apply(
        TestEventStubs.order_accepted(
            a, account_id=client.account_id, venue_order_id=VenueOrderId("A"), ts_event=0
        )
    )
    cache.update_order(a)
    positive = _order_report(b, client.account_id)
    positive.venue_order_id = VenueOrderId("B")
    http.request_order_status_reports_with_errors.side_effect = None
    http.request_order_status_reports_with_errors.return_value = (
        [positive.to_pyo3()],
        ["synthetic unrelated row conversion failure"],
    )
    try:
        if positive_route == "targeted_incomplete":
            # Actual engine caller receives IncompleteOrderStatusReports([positive B])
            # from actual OKX targeted lookup of A.
            await engine._resolve_order_not_found_at_venue(a)
        else:
            await engine._check_orders_consistency()
        assert not a.is_closed
    finally:
        release.set()
        await task
        engine.dispose()
    assert not engine._pending_absence_queries
    assert not b.is_closed, (
        "Retained positive B received during its older query must invalidate that absence decision"
    )


@pytest.mark.asyncio
async def test_reconnect_schedules_full_recovery_without_pending_entries(
    exec_client_builder,
    monkeypatch,
    msgbus,
):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    report = object()
    recover = AsyncMock(return_value=report)
    monkeypatch.setattr(client, "generate_mass_status", recover)
    received = []
    msgbus.register("ExecEngine.reconcile_execution_mass_status", received.append)
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client._handle_ws_reconnect(event)
    await client._ws_recovery_task
    recover.assert_awaited_once_with()
    assert received == [report]


@pytest.mark.asyncio
async def test_reconnect_failed_recovery_retains_uncertainty(exec_client_builder, monkeypatch):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    recover = AsyncMock(side_effect=RuntimeError("synthetic read failure"))
    monkeypatch.setattr(client, "generate_mass_status", recover)
    event = SimpleNamespace(pending_count=200, omitted_count=72)
    client._handle_ws_reconnect(event)
    await client._ws_recovery_task
    recover.assert_awaited_once_with()
    assert client._ws_recovery_required


@pytest.mark.asyncio
async def test_reconnect_during_readback_schedules_followup(exec_client_builder, monkeypatch):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def readback():
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
        return object()

    monkeypatch.setattr(client, "generate_mass_status", readback)
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", lambda report: None)
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client._handle_ws_reconnect(event)
    await started.wait()
    task = client._ws_recovery_task
    client._handle_ws_reconnect(event)
    assert client._ws_recovery_task is task
    release.set()
    await task
    assert calls == 2


@pytest.mark.asyncio
async def test_reconnect_actual_mass_read_failure_sends_no_report(exec_client_builder, monkeypatch):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    monkeypatch.setattr(
        client,
        "generate_order_status_reports",
        AsyncMock(side_effect=RuntimeError("synthetic failure")),
    )
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))
    received = []
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", received.append)
    client._handle_ws_reconnect(SimpleNamespace(pending_count=0, omitted_count=0))
    await client._ws_recovery_task
    assert client._ws_recovery_required
    assert not received


@pytest.mark.asyncio
async def test_reconnect_without_strict_collector_keeps_recovery_required(
    exec_client_builder, monkeypatch
):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    recover = AsyncMock()
    monkeypatch.setattr(client, "generate_mass_status", recover)
    client._handle_ws_reconnect(SimpleNamespace(pending_count=0, omitted_count=0))
    assert client._ws_recovery_required
    assert client._ws_recovery_task is None
    recover.assert_not_awaited()


async def _connect_recovery_client(client, monkeypatch):
    monkeypatch.setattr(client, "_await_account_registered", AsyncMock())
    client.connect()
    await eventually(lambda: client.is_connected)


@pytest.mark.asyncio
async def test_reconnect_during_and_after_public_disconnect_starts_no_read(
    exec_client_builder,
    monkeypatch,
):
    client, private_ws, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    close_started = asyncio.Event()
    close_release = asyncio.Event()

    async def close():
        close_started.set()
        await close_release.wait()

    private_ws.close.side_effect = close
    reads = AsyncMock(return_value=[])
    monkeypatch.setattr(client, "generate_order_status_reports", reads)
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))
    reports = []
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", reports.append)
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client.disconnect()
    try:
        # Includes the turn before the scheduled disconnect coroutine runs.
        client._handle_ws_reconnect(event)
        await close_started.wait()
        client._handle_ws_reconnect(event)
        await asyncio.sleep(0)
        reads.assert_not_awaited()
        assert not reports
        assert client._ws_recovery_required
    finally:
        close_release.set()
        await eventually(lambda: not client.is_connected)
    client._handle_ws_reconnect(event)
    await asyncio.sleep(0)
    reads.assert_not_awaited()
    assert not reports
    assert client._ws_recovery_required


@pytest.mark.asyncio
async def test_reconnect_read_ignoring_cancellation_cannot_submit_after_disconnect(
    exec_client_builder,
    monkeypatch,
):
    client, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    client._mass_status_reports_complete = True
    started = asyncio.Event()
    collect = client.generate_mass_status

    async def read():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return await collect()

    monkeypatch.setattr(client, "generate_mass_status", read)
    monkeypatch.setattr(client, "generate_order_status_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))
    reports = []
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", reports.append)
    client._handle_ws_reconnect(SimpleNamespace(pending_count=0, omitted_count=0))
    await asyncio.wait_for(started.wait(), 2)
    client.disconnect()
    await eventually(lambda: not client.is_connected)
    assert not reports
    assert client._ws_recovery_required


@pytest.mark.asyncio
async def test_reconnect_new_lifecycle_rejects_old_callback_and_recovers(
    exec_client_builder,
    monkeypatch,
):
    client, private_ws, *_ = exec_client_builder(monkeypatch)
    await _connect_recovery_client(client, monkeypatch)
    old_callback = private_ws.connect.call_args.kwargs["callback"]
    client._mass_status_reports_complete = True
    reads = AsyncMock(return_value=[])
    monkeypatch.setattr(client, "generate_order_status_reports", reads)
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(nautilus_pyo3, "OKXWebSocketReconnect", SimpleNamespace)
    reports = []
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", reports.append)
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client.disconnect()
    await eventually(lambda: not client.is_connected)
    old_callback(event)
    await asyncio.sleep(0)
    reads.assert_not_awaited()
    assert client._ws_recovery_required
    await _connect_recovery_client(client, monkeypatch)
    new_callback = private_ws.connect.call_args.kwargs["callback"]
    try:
        old_callback(event)
        await asyncio.sleep(0)
        reads.assert_not_awaited()
        new_callback(event)
        await client._ws_recovery_task
        reads.assert_awaited_once()
        assert len(reports) == 1
        assert not client._ws_recovery_required
    finally:
        client.disconnect()
        await eventually(lambda: not client.is_connected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("synthetic connect failure"), asyncio.CancelledError()]
)
async def test_reconnect_failed_connect_keeps_admission_closed(
    exec_client_builder,
    monkeypatch,
    failure,
):
    client, private_ws, *_ = exec_client_builder(monkeypatch)
    client._mass_status_reports_complete = True
    monkeypatch.setattr(client, "_await_account_registered", AsyncMock())
    private_ws.wait_until_active.side_effect = failure
    recover = AsyncMock()
    monkeypatch.setattr(client, "generate_mass_status", recover)
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client._handle_ws_reconnect(event)
    recover.assert_not_awaited()
    with pytest.raises(type(failure)):
        await client._connect()
    client._handle_ws_reconnect(event)
    await asyncio.sleep(0)
    recover.assert_not_awaited()
    assert client._ws_recovery_required


def _track_delayed_collection(client, release_read, canceled, collected, suppress_cancellation):
    collect = client.generate_mass_status

    async def tracked_collection():
        lifecycle = client._ws_recovery_lifecycle
        try:
            report = await collect()
        except asyncio.CancelledError:
            canceled.set()
            if not suppress_cancellation:
                raise
            while not release_read.is_set():
                with suppress(asyncio.CancelledError):
                    await release_read.wait()
            report = await collect()
        collected.append((lifecycle, report))
        return report

    return tracked_collection


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_failure", ["error", "cancel"])
@pytest.mark.parametrize(
    "retire",
    ["before_retry", "after_retry", "cancel_suppressed", "disconnect_retry", "no_new_reconnect"],
)
async def test_reconnect_retry_after_failed_startup_has_one_current_recovery(
    exec_client_builder,
    monkeypatch,
    startup_failure,
    retire,
):
    client, private_ws, business_ws, *_ = exec_client_builder(monkeypatch)
    client._mass_status_reports_complete = True
    monkeypatch.setattr(client, "_await_account_registered", AsyncMock())
    monkeypatch.setattr(nautilus_pyo3, "OKXWebSocketReconnect", SimpleNamespace)
    startup = asyncio.Event()
    fail_startup = asyncio.Event()
    read_started = asyncio.Event()
    release_read = asyncio.Event()
    canceled = asyncio.Event()
    reads = []
    collected = []
    reports = []
    client._msgbus.register("ExecEngine.reconcile_execution_mass_status", reports.append)

    async def wait_for_business(**kwargs):
        startup.set()
        await fail_startup.wait()
        raise RuntimeError("synthetic business startup failure")

    async def orders(command):
        reads.append(client._ws_recovery_lifecycle)
        if len(reads) == 1:
            read_started.set()
            await release_read.wait()
        return []

    tracked_collection = _track_delayed_collection(
        client,
        release_read,
        canceled,
        collected,
        suppress_cancellation=retire in ("cancel_suppressed", "disconnect_retry"),
    )

    business_ws.wait_until_active.side_effect = wait_for_business
    monkeypatch.setattr(client, "generate_mass_status", tracked_collection)
    monkeypatch.setattr(client, "generate_order_status_reports", orders)
    monkeypatch.setattr(client, "generate_fill_reports", AsyncMock(return_value=[]))
    monkeypatch.setattr(client, "generate_position_status_reports", AsyncMock(return_value=[]))
    event = SimpleNamespace(pending_count=0, omitted_count=0)
    client.connect()
    await asyncio.wait_for(startup.wait(), 2)
    old_callback = private_ws.connect.call_args.kwargs["callback"]
    old_callback(event)
    await asyncio.wait_for(read_started.wait(), 2)
    old_task = client._ws_recovery_task
    old_lifecycle = client._ws_recovery_lifecycle
    if startup_failure == "cancel":
        next(t for t in client._tasks if t.get_name() == "_connect" and not t.done()).cancel()
    else:
        fail_startup.set()
    await eventually(lambda: client._ws_recovery_lifecycle is None)
    assert not client.is_connected
    if retire == "before_retry":
        release_read.set()
        await old_task
    business_ws.wait_until_active.side_effect = None
    await _connect_recovery_client(client, monkeypatch)
    current_lifecycle = client._ws_recovery_lifecycle
    current_callback = private_ws.connect.call_args.kwargs["callback"]
    if retire != "no_new_reconnect":
        current_callback(event)
    if retire == "cancel_suppressed":
        old_task.cancel()
        await asyncio.wait_for(canceled.wait(), 2)
    if retire == "disconnect_retry":
        client.disconnect()
        await asyncio.wait_for(canceled.wait(), 2)
    release_read.set()
    await eventually(lambda: client._ws_recovery_task.done())
    if retire == "no_new_reconnect":
        assert not reports
        assert client._ws_recovery_required
        current_callback(event)
        await client._ws_recovery_task
    try:
        assert all(report is not None for _, report in collected)
        old_reports = [report for owner, report in collected if owner is old_lifecycle]
        current_reports = [report for owner, report in collected if owner is current_lifecycle]
        assert len(old_reports) == 1
        assert all(report not in reports for report in old_reports)
        assert len(current_reports) == int(retire != "disconnect_retry")
        assert reports == current_reports
        assert client._ws_recovery_required == (retire == "disconnect_retry")
    finally:
        client.disconnect()
        await eventually(lambda: not client.is_connected)


@pytest.mark.asyncio
async def test_capture_status_client_lifecycle_coalesces_and_contains_failure(
    exec_client_builder, monkeypatch
):
    from nautilus_trader.live.execution_engine import ExecutionCaptureStatus

    client, private_ws, business_ws, *_ = exec_client_builder(monkeypatch)
    rows = []
    sequence = [0]

    def status(scope, closed=False):
        sequence[0] += 1
        return ExecutionCaptureStatus(
            1,
            "okx_transport",
            scope,
            0,
            100 + sequence[0],
            100,
            60_000_000_000,
            4096,
            4096,
            4096,
            2,
            sequence[0],
            "closed" if closed else "exhausted",
        )

    private_ws.capture_status = lambda: status("private")
    business_ws.capture_status = lambda: status("business")
    client._handle_ws_capture_status = rows.append
    client._start_ws_capture_status()
    assert len(rows) == 2
    old = client._ws_capture_status_handle
    client._start_ws_capture_status()
    assert old.cancelled()
    assert len(rows) == 4

    def fail(_):
        raise RuntimeError("synthetic diagnostic consumer failure")

    client._handle_ws_capture_status = fail
    client._ws_capture_status_handle.cancel()
    client._ws_capture_status_tick()
    assert client._ws_capture_status_failures == 2
    assert client._ws_capture_status_handle is not None
    client._handle_ws_capture_status = rows.append
    final_private = status("private", True)
    final_business = status("business", True)
    private_ws.capture_status = lambda: final_private
    business_ws.capture_status = lambda: final_business
    old = client._ws_capture_status_handle
    await client._disconnect()
    assert old.cancelled()
    assert client._ws_capture_status_handle is None
    assert rows[-2:] == [final_private, final_business]
    count = len(rows)
    client._offer_ws_capture_status(closed_only=True)
    client._ws_capture_status_tick()
    assert len(rows) == count
    assert client._ws_capture_status_handle is None


@pytest.mark.asyncio
async def test_capture_status_legacy_extension_and_disconnect_failure(
    exec_client_builder, monkeypatch
):
    client, private_ws, business_ws, *_ = exec_client_builder(monkeypatch)
    private_ws.capture_status = None
    business_ws.capture_status = None
    client._start_ws_capture_status()
    assert client._ws_capture_status_failures == 0
    handle = client._ws_capture_status_handle
    private_ws.is_closed.return_value = False
    private_ws.close.side_effect = RuntimeError("synthetic close failure")
    with pytest.raises(RuntimeError, match="synthetic close failure"):
        await client._disconnect()
    assert handle.cancelled()
    assert client._ws_capture_status_handle is None
