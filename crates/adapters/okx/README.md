# nautilus-okx

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-okx)](https://docs.rs/nautilus-okx/latest/nautilus-okx/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-okx.svg)](https://crates.io/crates/nautilus-okx)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

[NautilusTrader](http://nautilustrader.io) adapter for the [OKX](https://www.okx.com/) cryptocurrency exchange.

The `nautilus-okx` crate provides client bindings (HTTP & WebSocket), data
models and helper utilities that wrap the official **OKX v5 API**.

The official OKX API reference can be found at <https://www.okx.com/docs-v5/en/>.

## Platform

[NautilusTrader](http://nautilustrader.io) is an open-source, high-performance, production-grade
algorithmic trading platform, providing quantitative traders with the ability to backtest
portfolios of automated trading strategies on historical data with an event-driven engine,
and also deploy those same strategies live, with no code changes.

NautilusTrader's design, architecture, and implementation philosophy prioritizes software correctness and safety at the
highest level, with the aim of supporting mission-critical, trading system backtesting and live deployment workloads.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `extension-module`: Builds as a Python extension module.

[High-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation#precision-mode) (128-bit value types) is enabled by default.

## Documentation

See [the docs](https://docs.rs/nautilus-okx) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="300" height="auto"/>

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.

## Reconnect recovery and request observations

The Python websocket callback receives `OKXWebSocketReconnect` on every transport
reconnect, including when no request correlations remain. This announces a need
for recovery; authentication, subscription replay, and reconciliation may still
be incomplete. The execution adapter dispatches it to `_handle_ws_reconnect`.
Integrations with an existing recovery owner should override that hook and request
full recovery regardless of the event's pending count. Reconnect must not reject,
resubmit, or release exposure for an uncertain request.

The default consumer coalesces reconnects into mass readbacks only when
`_mass_status_reports_complete` is explicitly true. This requires all configured
report readers to fail on incomplete results. This change does not enable that
capability. Without it, recovery remains required and the adapter logs the missing
integration. A failed read sends no mass report; a reconnect during a read requires
another pass. Report submission is not a settlement acknowledgement.

Default recovery is admitted only within a connection lifecycle. Public disconnect
closes admission before scheduling socket shutdown, preserves unresolved recovery,
and cancels its current task. Socket callbacks retain their lifecycle identity;
late reconnects cannot start recovery after close or enter a later lifecycle.
Readbacks recheck that identity before submitting a report, even if a reader
suppresses cancellation. A fresh connection opens a new lifecycle. Ordinary order
events and observation hooks retain their existing dispatch during shutdown.

Schema version 1 uses a random handler-lifetime `connection_id` and a generation
incremented at reconnect. Up to 128 pending amend/cancel observations carry the
native request ID, operation, order/instrument identities, requested venue-unit
terms, and original timing. `pending_count` counts all discarded correlations;
`omitted_count` includes details beyond the cap and place/mass-cancel records,
whose detailed metadata is not captured. Counts are not lost acknowledgements.

`OKXRequestObservation` is also delivered to `_handle_ws_request_observation`, a
no-op extension hook. Outcomes distinguish `handler_received`, `send_completed`,
`acknowledged`, `rejected`, and `unresolved_on_reconnect`. `response_code` is the
validated effective per-item code, or a whole-request refusal code when no items
are supplied. All timestamps are local UNIX nanoseconds: `ts_handler_ns` is
correlation registration before serialization/retries, `ts_send_completed_ns` is
the transport send return, and `ts_observed_ns` is the local observation boundary
(response processing or reconnect for those outcomes). These are neither caller
intent time nor venue receipt time, and wall-clock differences are not monotonic
latency measurements. Success retires response correlation only; orders-channel
messages remain responsible for confirmed order state.

Diagnostic delivery has a 4,096-observation budget per connection generation.
`observations_dropped` on reconnect is the cumulative number of refused diagnostic
enqueue attempts, including a closed receiver. It does not measure callback loss
or missing operations. Reconnect signals bypass this budget. Existing output
channels remain unbounded; no whole-process memory or complete-history guarantee
is implied. Python conversion or callback scheduling failures remain logged by
the existing bridge. These records are ephemeral diagnostics, not a durable ledger.

The typed callbacks require a newly built native extension. Older extensions
continue to work with the Python adapter but cannot provide these notifications.
Rust consumers must handle the reconnect payload and request-observation variant.
