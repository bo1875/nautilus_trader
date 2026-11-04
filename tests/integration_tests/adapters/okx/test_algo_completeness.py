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
import json
import threading
from contextlib import suppress
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs
from urllib.parse import urlsplit

import pytest

from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.okx import test_execution


exec_client_builder = test_execution.exec_client_builder


def _algo_row(**changes):
    row = {
        "algoId": "9001",
        "algoClOrdId": "O-ALGO",
        "clOrdId": "",
        "ordId": "",
        "instId": "BTC-USD",
        "instType": "SPOT",
        "ordType": "trigger",
        "state": "live",
        "side": "sell",
        "posSide": "net",
        "sz": "1",
        "triggerPx": "100",
        "triggerPxType": "last",
        "ordPx": "-1",
        "tdMode": "cash",
        "cTime": "1",
        "uTime": "1",
    }
    return row | changes


@pytest.fixture
def algo_http_server():
    paths = []
    entered = threading.Event()
    release = threading.Event()
    config = {"rows": [], "history_status": 200, "pending_status": 200, "block": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            url = urlsplit(self.path)
            query = parse_qs(url.query)
            paths.append((url.path, query))
            status, rows = 200, []
            if url.path.endswith("/order-algo-pending"):
                entered.set()
                if config["block"]:
                    release.wait(5)
                status = config["pending_status"]
                rows = config["rows"]
                if config.get("lookup") == "algo" and "algoId" not in query:
                    rows = []
            elif url.path.endswith("/order-algo-history"):
                status = config["history_status"]
            data = json.dumps({"code": "0", "msg": "", "data": rows}).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            # Cancellation may close the synthetic client connection.
            with suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield config, paths, entered, release, f"http://127.0.0.1:{server.server_port}"
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(2)


def _http_client(url, instrument):
    client = nautilus_pyo3.OKXHttpClient(
        api_key="synthetic",
        api_secret="synthetic",
        api_passphrase="synthetic",
        base_url=url,
        max_retries=0,
        timeout_secs=2,
    )
    client.cache_instrument(nautilus_pyo3.CurrencyPair.from_dict(instrument.to_dict(instrument)))
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ["periodic", "inflight"])
@pytest.mark.parametrize("lookup", ["client", "algo"])
@pytest.mark.parametrize(
    "outcome",
    [
        "valid",
        "empty",
        "invalid",
        "unrelated_invalid",
        "mixed",
        "history_error_positive",
        "error",
        "not_found",
        "cancel",
    ],
)
async def test_algo_http_completeness_at_absence_threshold(  # noqa: C901
    exec_client_builder,
    monkeypatch,
    instrument,
    cache,
    live_clock,
    algo_http_server,
    caller,
    lookup,
    outcome,
):
    config, paths, entered, release, url = algo_http_server
    config["lookup"] = lookup
    row = _algo_row()
    if outcome in {"valid", "mixed", "history_error_positive"}:
        config["rows"] = [row]
    if outcome in {"invalid", "mixed"}:
        config["rows"].insert(0, row | {"sz": "invalid"})
    if outcome == "unrelated_invalid":
        config["rows"] = [_algo_row(algoId="9002", algoClOrdId="O-OTHER", sz="invalid")]
    if outcome in {"error", "history_error_positive"}:
        config["history_status"] = 503
    if outcome == "not_found":
        config["pending_status"] = 404
    if outcome == "cancel":
        config["block"] = True
    client, _, _, _, _ = exec_client_builder(monkeypatch)
    client._http_client = _http_client(url, instrument)
    cache.add_instrument(instrument)
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
        order.apply(
            TestEventStubs.order_accepted(
                order,
                account_id=client.account_id,
                venue_order_id=VenueOrderId("9001"),
                ts_event=0,
            )
        )
    cache.update_order(order)
    before = list(order.events)
    client._algo_order_ids[order.client_order_id] = "9001"
    engine = LiveExecutionEngine(
        loop=asyncio.get_running_loop(),
        msgbus=MessageBus(trader_id=TestIdStubs.trader_id(), clock=live_clock),
        cache=cache,
        clock=live_clock,
        config=LiveExecEngineConfig(
            open_check_open_only=False,
            open_check_threshold_ms=0,
            single_order_query_delay_ms=0,
        ),
    )
    engine.register_client(client)
    engine._recon_check_retries[order.client_order_id] = (
        engine.open_check_missing_retries
        if caller == "periodic"
        else engine.inflight_check_max_retries
    )
    task = asyncio.create_task(
        engine._check_orders_consistency()
        if caller == "periodic"
        else engine._check_inflight_orders(),
    )
    try:
        if outcome == "cancel":
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await asyncio.wait_for(task, 5)
        if outcome in {"empty", "unrelated_invalid"}:
            assert order.status == OrderStatus.REJECTED
        elif outcome in {"valid", "mixed", "history_error_positive"}:
            assert order.status == OrderStatus.ACCEPTED
        else:
            assert order.events == before
        assert not engine._pending_absence_queries
        queries = [query for path, query in paths if path.endswith("/order-algo-pending")]
        assert queries
        assert queries[0]["algoClOrdId"] == ["O-ALGO"]
        if lookup == "algo" and outcome != "cancel":
            assert any(query.get("algoId") == ["9001"] for query in queries)
    finally:
        release.set()
        task.cancel()
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["conversion", "conversion_after_positive", "cache", "transport", "page"])
async def test_strict_algo_reader_retains_positives_and_legacy_raises(
    algo_http_server,
    instrument,
    failure,
):
    config, _, _, _, url = algo_http_server
    config["rows"] = [_algo_row()]
    if failure == "conversion":
        config["rows"].insert(0, _algo_row(sz="invalid"))
    if failure == "conversion_after_positive":
        config["rows"].append(_algo_row(sz="invalid"))
    if failure == "cache":
        config["rows"].append(_algo_row(algoId="9002", instId="UNKNOWN-USD"))
    if failure == "transport":
        config["history_status"] = 503
    client = _http_client(url, instrument)
    kwargs = {
        "account_id": nautilus_pyo3.AccountId("OKX-123"),
        "instrument_type": nautilus_pyo3.OKXInstrumentType.SPOT,
        "limit": 1 if failure == "page" else 100,
    }
    reports, errors = await client.request_algo_order_status_reports_with_errors(**kwargs)
    assert len(reports) == 1
    assert reports[0].venue_order_id.value == "9001"
    assert errors
    with pytest.raises(ValueError, match="Incomplete algo order reports"):
        await client.request_algo_order_status_reports(**kwargs)


@pytest.mark.asyncio
async def test_legacy_singular_algo_conversion_failure_raises(algo_http_server, instrument):
    config, _, _, _, url = algo_http_server
    config["rows"] = [_algo_row(sz="invalid")]
    client = _http_client(url, instrument)
    with pytest.raises(ValueError, match="Incomplete algo order reports"):
        await client.request_algo_order_status_report(
            account_id=nautilus_pyo3.AccountId("OKX-123"),
            instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
            client_order_id=nautilus_pyo3.ClientOrderId("O-ALGO"),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["empty", "instrument", "type", "client", "algo"])
async def test_strict_algo_reader_qualified_empty(algo_http_server, instrument, scope):
    config, _, _, _, url = algo_http_server
    if scope != "empty":
        changes = {
            "instrument": {"instId": "UNKNOWN-USD"},
            "type": {"instType": "SWAP"},
            "client": {"algoClOrdId": "O-OTHER"},
            "algo": {"algoId": "9002"},
        }[scope]
        config["rows"] = [_algo_row(sz="invalid", **changes)]
    client = _http_client(url, instrument)
    kwargs = {
        "account_id": nautilus_pyo3.AccountId("OKX-123"),
        "instrument_id": nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        "algo_client_order_id": nautilus_pyo3.ClientOrderId("O-ALGO"),
        "algo_id": "9001",
    }
    assert await client.request_algo_order_status_reports_with_errors(**kwargs) == ([], [])
    assert await client.request_algo_order_status_reports(**kwargs) == []


@pytest.mark.asyncio
async def test_targeted_algo_requires_strict_binary_contract(
    exec_client_builder, monkeypatch, instrument
):
    from nautilus_trader.execution.messages import GenerateOrderStatusReport

    client, _, _, http, _ = exec_client_builder(monkeypatch)
    del http.request_algo_order_status_reports_with_errors
    command = GenerateOrderStatusReport(
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-ALGO"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    with pytest.raises(AttributeError):
        await client.generate_order_status_report(command)
