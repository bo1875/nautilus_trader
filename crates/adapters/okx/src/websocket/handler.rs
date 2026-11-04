// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! WebSocket message handler for OKX.
//!
//! The handler runs in a dedicated Tokio task as the I/O boundary between the client
//! orchestrator and the network layer. It exclusively owns the `WebSocketClient` and
//! processes commands from the client via an unbounded channel, serializing them to JSON
//! and sending via the WebSocket. Raw messages are received from the network, deserialized,
//! and transformed into `NautilusWsMessage` events which are emitted back to the client.
//!
//! Responsibilities:
//! - Command processing: Receives `HandlerCommand` from client, executes WebSocket operations.
//! - Message transformation: Parses raw venue messages into Nautilus domain events.
//! - Pending state tracking: Owns `AHashMap` for matching requests/responses (single-threaded).
//! - Retry logic: Retries transient WebSocket send failures using `RetryManager`.
//! - Error event emission: Emits `OrderRejected`, `OrderCancelRejected` when retries exhausted.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use super::capture::{
    CaptureSlot, CaptureSnapshot, LIMIT, OKXCaptureStatus, WINDOW, close_capture,
};
use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use nautilus_common::cache::fifo::FifoCache;
use nautilus_core::{AtomicTime, UUID4, nanos::UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::InstrumentStatus,
    enums::{OrderStatus, OrderType},
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderModifyRejected, OrderRejected,
    },
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, Symbol, TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    types::{Money, Quantity},
};
use nautilus_network::{
    RECONNECTED,
    retry::{RetryManager, create_websocket_retry_manager},
    websocket::{AuthTracker, SubscriptionState, TEXT_PING, TEXT_PONG, WebSocketClient},
};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use super::{
    enums::{OKXSubscriptionEvent, OKXWsChannel, OKXWsOperation},
    error::OKXWsError,
    messages::{
        ExecutionReport, NautilusWsMessage, OKXAlgoOrderMsg, OKXBookMsg, OKXOrderMsg,
        OKXRequestObservation, OKXSubscription, OKXSubscriptionArg, OKXWebSocketArg,
        OKXWebSocketError, OKXWebSocketReconnect, OKXWsMessage, OKXWsRequest, WsAmendOrderParams,
        WsCancelAlgoOrderParamsBuilder, WsCancelOrderParamsBuilder, WsMassCancelParams,
        WsPostAlgoOrderParams, WsPostOrderParams,
    },
    parse::{
        OrderStateSnapshot, ParsedOrderEvent, extract_fees_from_cached_instrument,
        parse_algo_order_msg, parse_book_msg_vec, parse_order_event, parse_order_msg,
        parse_order_updated, parse_ws_message_data,
    },
    subscription::topic_from_websocket_arg,
};
use crate::{
    common::{
        consts::{
            OKX_POST_ONLY_CANCEL_REASON, OKX_POST_ONLY_CANCEL_SOURCE, OKX_POST_ONLY_ERROR_CODE,
            OKX_VENUE, should_retry_error_code,
        },
        enums::{
            OKXBookAction, OKXInstrumentStatus, OKXInstrumentType, OKXOrderStatus, OKXSide,
            OKXTargetCurrency, OKXTradeMode,
        },
        models::OKXInstrument,
        parse::{
            determine_order_type, is_market_price, okx_instrument_type,
            okx_status_to_market_action, parse_account_state, parse_client_order_id,
            parse_instrument_any, parse_millisecond_timestamp, parse_position_status_report,
            parse_price, parse_quantity,
        },
    },
    http::models::{OKXAccount, OKXPosition},
    websocket::client::{
        OKX_RATE_LIMIT_KEY_AMEND, OKX_RATE_LIMIT_KEY_CANCEL, OKX_RATE_LIMIT_KEY_ORDER,
        OKX_RATE_LIMIT_KEY_SUBSCRIPTION,
    },
};

/// Data cached for pending place requests to correlate with responses.
type PlaceRequestData = (
    PendingOrderParams,
    ClientOrderId,
    TraderId,
    StrategyId,
    InstrumentId,
);

/// Data cached for pending cancel requests to correlate with responses.
type CancelRequestData = (
    ClientOrderId,
    TraderId,
    StrategyId,
    InstrumentId,
    Option<VenueOrderId>,
    OKXRequestObservation,
);

/// Data cached for pending amend requests to correlate with responses.
type AmendRequestData = CancelRequestData;

#[derive(Debug)]
pub enum PendingOrderParams {
    Regular(WsPostOrderParams),
    Algo(WsPostAlgoOrderParams),
}

/// Commands sent from the outer client to the inner message handler.
#[allow(
    clippy::large_enum_variant,
    reason = "Commands are ephemeral and immediately consumed"
)]
#[allow(missing_debug_implementations)]
pub enum HandlerCommand {
    SetClient(WebSocketClient),
    Disconnect,
    Authenticate {
        payload: String,
    },
    InitializeInstruments(Vec<InstrumentAny>),
    UpdateInstrument(InstrumentAny),
    Subscribe {
        args: Vec<OKXSubscriptionArg>,
    },
    Unsubscribe {
        args: Vec<OKXSubscriptionArg>,
    },
    PlaceOrder {
        params: WsPostOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
    },
    PlaceAlgoOrder {
        params: WsPostAlgoOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
    },
    AmendOrder {
        params: WsAmendOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
    },
    CancelOrder {
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        instrument_id: InstrumentId,
        trader_id: TraderId,
        strategy_id: StrategyId,
    },
    CancelAlgoOrder {
        client_order_id: Option<ClientOrderId>,
        algo_order_id: Option<VenueOrderId>,
        instrument_id: InstrumentId,
        trader_id: TraderId,
        strategy_id: StrategyId,
    },
    MassCancel {
        instrument_id: InstrumentId,
    },
    BatchPlaceOrders {
        args: Vec<Value>,
        request_id: String,
    },
    BatchAmendOrders {
        args: Vec<Value>,
        request_id: String,
    },
    BatchCancelOrders {
        args: Vec<Value>,
        request_id: String,
    },
    MapIndexTicker {
        base_pair: Ustr,
        original: Ustr,
    },
    UnmapIndexTicker {
        base_pair: Ustr,
        original: Ustr,
    },
}

pub(super) struct OKXWsFeedHandler {
    clock: &'static AtomicTime,
    account_id: AccountId,
    signal: Arc<AtomicBool>,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    out_tx: tokio::sync::mpsc::UnboundedSender<NautilusWsMessage>,
    auth_tracker: AuthTracker,
    subscriptions_state: SubscriptionState,
    retry_manager: RetryManager<OKXWsError>,
    pending_place_requests: AHashMap<String, PlaceRequestData>,
    pending_cancel_requests: AHashMap<String, CancelRequestData>,
    pending_amend_requests: AHashMap<String, AmendRequestData>,
    pending_mass_cancel_requests: AHashMap<String, InstrumentId>,
    pending_messages: VecDeque<NautilusWsMessage>,
    connection_id: String,
    connection_generation: u64,
    observations_emitted: usize,
    observations_dropped: u64,
    observations_total: u64,
    observation_window_start: Instant,
    observation_window_date: u64,
    capture_sequence: Arc<AtomicU64>,
    pub(super) capture_state: CaptureSlot,
    active_client_orders: Arc<DashMap<ClientOrderId, (TraderId, StrategyId, InstrumentId)>>,
    client_id_aliases: Arc<DashMap<ClientOrderId, ClientOrderId>>,
    inst_id_code_cache: Arc<DashMap<Ustr, u64>>,
    emitted_accepted: FifoCache<VenueOrderId, 10_000>,
    instruments_cache: AHashMap<Ustr, InstrumentAny>,
    index_ticker_map: AHashMap<Ustr, AHashSet<Ustr>>,
    fee_cache: AHashMap<Ustr, Money>,
    filled_qty_cache: AHashMap<Ustr, Quantity>,
    order_state_cache: AHashMap<ClientOrderId, OrderStateSnapshot>,
    funding_rate_cache: AHashMap<Ustr, (Ustr, u64)>, // Cache (funding_rate, funding_time) by inst_id
    last_account_state: Option<AccountState>,
    request_id_counter: Arc<AtomicU64>,
}

impl Drop for OKXWsFeedHandler {
    fn drop(&mut self) {
        self.publish_capture_state();
        close_capture(&self.capture_state, Some(&self.connection_id));
    }
}

impl OKXWsFeedHandler {
    /// Creates a new [`OKXWsFeedHandler`] instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: AccountId,
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<NautilusWsMessage>,
        active_client_orders: Arc<DashMap<ClientOrderId, (TraderId, StrategyId, InstrumentId)>>,
        client_id_aliases: Arc<DashMap<ClientOrderId, ClientOrderId>>,
        inst_id_code_cache: Arc<DashMap<Ustr, u64>>,
        auth_tracker: AuthTracker,
        subscriptions_state: SubscriptionState,
        request_id_counter: Arc<AtomicU64>,
    ) -> Self {
        Self {
            clock: get_atomic_clock_realtime(),
            account_id,
            signal,
            inner: None,
            cmd_rx,
            raw_rx,
            out_tx,
            auth_tracker,
            subscriptions_state,
            retry_manager: create_websocket_retry_manager(),
            pending_place_requests: AHashMap::new(),
            pending_cancel_requests: AHashMap::new(),
            pending_amend_requests: AHashMap::new(),
            pending_mass_cancel_requests: AHashMap::new(),
            pending_messages: VecDeque::new(),
            connection_id: UUID4::new().to_string(),
            connection_generation: 0,
            observations_emitted: 0,
            observations_dropped: 0,
            observations_total: 0,
            observation_window_start: Instant::now(),
            observation_window_date: get_atomic_clock_realtime().get_time_ns().as_u64(),
            capture_sequence: Arc::new(AtomicU64::new(0)),
            capture_state: Arc::new(ArcSwapOption::empty()),
            active_client_orders,
            client_id_aliases,
            inst_id_code_cache,
            emitted_accepted: FifoCache::new(),
            instruments_cache: AHashMap::new(),
            index_ticker_map: AHashMap::new(),
            fee_cache: AHashMap::new(),
            filled_qty_cache: AHashMap::new(),
            order_state_cache: AHashMap::new(),
            funding_rate_cache: AHashMap::new(),
            last_account_state: None,
            request_id_counter,
        }
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn send(&self, msg: NautilusWsMessage) -> Result<(), ()> {
        self.out_tx.send(msg).map_err(|_| ())
    }

    async fn send_with_retry(
        &self,
        payload: String,
        rate_limit_keys: Option<&[Ustr]>,
    ) -> Result<(), OKXWsError> {
        if let Some(client) = &self.inner {
            let keys_owned: Option<Vec<Ustr>> = rate_limit_keys.map(|k| k.to_vec());
            self.retry_manager
                .execute_with_retry(
                    "websocket_send",
                    || {
                        let payload = payload.clone();
                        let keys = keys_owned.clone();
                        async move {
                            client
                                .send_text(payload, keys.as_deref())
                                .await
                                .map_err(|e| OKXWsError::ClientError(format!("Send failed: {e}")))
                        }
                    },
                    should_retry_okx_error,
                    create_okx_timeout_error,
                )
                .await
        } else {
            Err(OKXWsError::ClientError(
                "No active WebSocket client".to_string(),
            ))
        }
    }

    pub(super) async fn send_pong(&self) -> anyhow::Result<()> {
        match self.send_with_retry(TEXT_PONG.to_string(), None).await {
            Ok(()) => {
                log::trace!("Sent pong response to OKX text ping");
                Ok(())
            }
            Err(e) => {
                log::warn!("Failed to send pong after retries: error={e}");
                Err(anyhow::anyhow!("Failed to send pong: {e}"))
            }
        }
    }

    pub(super) async fn next(&mut self) -> Option<NautilusWsMessage> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Some(message);
        }

        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            log::debug!("Handler received WebSocket client");
                            self.inner = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            log::debug!("Handler disconnecting WebSocket client");
                            self.inner = None;
                            return None;
                        }
                        HandlerCommand::Authenticate { payload } => {
                            if let Err(e) = self.send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice())).await {
                                log::error!("Failed to send authentication message after retries: error={e}");
                            }
                        }
                        HandlerCommand::InitializeInstruments(instruments) => {
                            for inst in instruments {
                                self.instruments_cache.insert(inst.symbol().inner(), inst);
                            }
                        }
                        HandlerCommand::UpdateInstrument(inst) => {
                            self.instruments_cache.insert(inst.symbol().inner(), inst);
                        }
                        HandlerCommand::Subscribe { args } => {
                            if let Err(e) = self.handle_subscribe(args).await {
                                log::error!("Failed to handle subscribe command: error={e}");
                            }
                        }
                        HandlerCommand::Unsubscribe { args } => {
                            if let Err(e) = self.handle_unsubscribe(args).await {
                                log::error!("Failed to handle unsubscribe command: error={e}");
                            }
                        }
                        HandlerCommand::CancelOrder {
                            client_order_id,
                            venue_order_id,
                            instrument_id,
                            trader_id,
                            strategy_id,
                        } => {
                            if let Err(e) = self
                                .handle_cancel_order(
                                    client_order_id,
                                    venue_order_id,
                                    instrument_id,
                                    trader_id,
                                    strategy_id,
                                )
                                .await
                            {
                                log::error!("Failed to handle cancel order command: error={e}");
                            }
                        }
                        HandlerCommand::CancelAlgoOrder {
                            client_order_id,
                            algo_order_id,
                            instrument_id,
                            trader_id,
                            strategy_id,
                        } => {
                            if let Err(e) = self
                                .handle_cancel_algo_order(
                                    client_order_id,
                                    algo_order_id,
                                    instrument_id,
                                    trader_id,
                                    strategy_id,
                                )
                                .await
                            {
                                log::error!("Failed to handle cancel algo order command: error={e}");
                            }
                        }
                        HandlerCommand::PlaceOrder {
                            params,
                            client_order_id,
                            trader_id,
                            strategy_id,
                            instrument_id,
                        } => {
                            if let Err(e) = self
                                .handle_place_order(
                                    params,
                                    client_order_id,
                                    trader_id,
                                    strategy_id,
                                    instrument_id,
                                )
                                .await
                            {
                                log::error!("Failed to handle place order command: error={e}");
                            }
                        }
                        HandlerCommand::PlaceAlgoOrder {
                            params,
                            client_order_id,
                            trader_id,
                            strategy_id,
                            instrument_id,
                        } => {
                            if let Err(e) = self
                                .handle_place_algo_order(
                                    params,
                                    client_order_id,
                                    trader_id,
                                    strategy_id,
                                    instrument_id,
                                )
                                .await
                            {
                                log::error!("Failed to handle place algo order command: error={e}");
                            }
                        }
                        HandlerCommand::AmendOrder {
                            params,
                            client_order_id,
                            trader_id,
                            strategy_id,
                            instrument_id,
                            venue_order_id,
                        } => {
                            if let Err(e) = self
                                .handle_amend_order(
                                    params,
                                    client_order_id,
                                    trader_id,
                                    strategy_id,
                                    instrument_id,
                                    venue_order_id,
                                )
                                .await
                            {
                                log::error!("Failed to handle amend order command: error={e}");
                            }
                        }
                        HandlerCommand::MassCancel { instrument_id } => {
                            if let Err(e) = self.handle_mass_cancel(instrument_id).await {
                                log::error!("Failed to handle mass cancel command: error={e}");
                            }
                        }
                        HandlerCommand::BatchCancelOrders { args, request_id } => {
                            if let Err(e) = self.handle_batch_cancel_orders(args, request_id).await {
                                log::error!("Failed to handle batch cancel orders command: error={e}");
                            }
                        }
                        HandlerCommand::BatchPlaceOrders { args, request_id } => {
                            if let Err(e) = self.handle_batch_place_orders(args, request_id).await {
                                log::error!("Failed to handle batch place orders command: error={e}");
                            }
                        }
                        HandlerCommand::BatchAmendOrders { args, request_id } => {
                            if let Err(e) = self.handle_batch_amend_orders(args, request_id).await {
                                log::error!("Failed to handle batch amend orders command: error={e}");
                            }
                        }
                        HandlerCommand::MapIndexTicker { base_pair, original } => {
                            self.index_ticker_map
                                .entry(base_pair)
                                .or_default()
                                .insert(original);
                        }
                        HandlerCommand::UnmapIndexTicker { base_pair, original } => {
                            if let Some(set) = self.index_ticker_map.get_mut(&base_pair) {
                                set.remove(&original);
                                if set.is_empty() {
                                    self.index_ticker_map.remove(&base_pair);
                                }
                            }
                        }
                    }
                }

                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    if self.signal.load(std::sync::atomic::Ordering::Acquire) {
                        log::debug!("Stop signal received during idle period");
                        return None;
                    }
                }

                msg = self.raw_rx.recv() => {
                    let event = match msg {
                        Some(msg) => match Self::parse_raw_message(msg) {
                            Some(event) => event,
                            None => continue,
                        },
                        None => {
                            log::debug!("WebSocket stream closed");
                            return None;
                        }
                    };

                    let ts_init = self.clock.get_time_ns();

            match event {
                OKXWsMessage::Ping => {
                    if let Err(e) = self.send_pong().await {
                        log::warn!("Failed to send pong response: error={e}");
                    }
                }
                OKXWsMessage::Login {
                    code, msg, conn_id, ..
                } => {
                    if code == "0" {
                        self.auth_tracker.succeed();

                        // Must return immediately to deliver Authenticated message.
                        // Using push_back() + continue blocks the select! loop and prevents
                        // the spawn block from receiving this event, breaking reconnection flow.
                        return Some(NautilusWsMessage::Authenticated);
                    }

                    log::error!("WebSocket authentication failed: error={msg}");
                    self.auth_tracker.fail(msg.clone());

                    let error = OKXWebSocketError {
                        code,
                        message: msg,
                        conn_id: Some(conn_id),
                        timestamp: self.clock.get_time_ns().as_u64(),
                    };
                    self.pending_messages
                        .push_back(NautilusWsMessage::Error(error));
                }
                OKXWsMessage::BookData { arg, action, data } => {
                    if let Some(msg) = self.handle_book_data(arg, action, data, ts_init) {
                        return Some(msg);
                    }
                }
                OKXWsMessage::OrderResponse {
                    id,
                    op,
                    code,
                    msg,
                    data,
                } => {
                    if let Some(msg) = self.handle_order_response(id, op, code, msg, data, ts_init) {
                        return Some(msg);
                    }
                }
                OKXWsMessage::Data { arg, data } => {
                    let OKXWebSocketArg {
                        channel, inst_id, ..
                    } = arg;

                    match channel {
                        OKXWsChannel::Account => {
                            if let Some(msg) = self.handle_account_data(data, ts_init) {
                                return Some(msg);
                            }
                        }
                        OKXWsChannel::Positions => {
                            self.handle_positions_data(data, ts_init);
                        }
                        OKXWsChannel::Orders => {
                            if let Some(msg) = self.handle_orders_data(data, ts_init) {
                                return Some(msg);
                            }
                        }
                        OKXWsChannel::OrdersAlgo | OKXWsChannel::AlgoAdvance => {
                            if let Some(msg) = self.handle_algo_orders_data(data, ts_init) {
                                return Some(msg);
                            }
                        }

                        OKXWsChannel::Instruments => {
                            if let Some(msg) = self.handle_instruments_data(data, ts_init) {
                                return Some(msg);
                            }
                        }
                        _ => {
                            if let Some(msg) =
                                self.handle_other_channel_data(channel, inst_id, data, ts_init)
                            {
                                return Some(msg);
                            }
                        }
                    }
                }
                OKXWsMessage::Error { code, msg } => {
                    let error = OKXWebSocketError {
                        code,
                        message: msg,
                        conn_id: None,
                        timestamp: self.clock.get_time_ns().as_u64(),
                    };
                    return Some(NautilusWsMessage::Error(error));
                }
                OKXWsMessage::Reconnected => {
                    return Some(NautilusWsMessage::Reconnected(
                        self.drain_pending_requests_on_reconnect(),
                    ));
                }
                OKXWsMessage::Subscription {
                    event,
                    arg,
                    code,
                    msg,
                    ..
                } => {
                    let topic = topic_from_websocket_arg(&arg);
                    let success = code.as_deref().is_none_or(|c| c == "0");

                    match event {
                        OKXSubscriptionEvent::Subscribe => {
                            if success {
                                self.subscriptions_state.confirm_subscribe(&topic);
                            } else {
                                log::warn!("Subscription failed: topic={topic:?}, error={msg:?}, code={code:?}");
                                self.subscriptions_state.mark_failure(&topic);
                            }
                        }
                        OKXSubscriptionEvent::Unsubscribe => {
                            if success {
                                self.subscriptions_state.confirm_unsubscribe(&topic);
                            } else {
                                log::warn!("Unsubscription failed - restoring subscription: topic={topic:?}, error={msg:?}, code={code:?}");
                                // Venue rejected unsubscribe, so we're still subscribed. Restore state:
                                self.subscriptions_state.confirm_unsubscribe(&topic); // Clear pending_unsubscribe
                                self.subscriptions_state.mark_subscribe(&topic);      // Mark as subscribing
                                self.subscriptions_state.confirm_subscribe(&topic);   // Confirm subscription
                            }
                        }
                    }
                }
                OKXWsMessage::ChannelConnCount { .. } => {}
            }
                }

                // Handle shutdown - either channel closed or stream ended
                else => {
                    log::debug!("Handler shutting down: stream ended or command channel closed");
                    return None;
                }
            }
        }
    }

    pub(super) fn is_post_only_auto_cancel(msg: &OKXOrderMsg) -> bool {
        if msg.state != OKXOrderStatus::Canceled {
            return false;
        }

        let cancel_source_matches = matches!(
            msg.cancel_source.as_deref(),
            Some(source) if source == OKX_POST_ONLY_CANCEL_SOURCE
        );

        let reason_matches = matches!(
            msg.cancel_source_reason.as_deref(),
            Some(reason) if reason.contains("POST_ONLY")
        );

        if !(cancel_source_matches || reason_matches) {
            return false;
        }

        msg.acc_fill_sz
            .as_ref()
            .is_none_or(|filled| filled == "0" || filled.is_empty())
    }

    fn try_handle_post_only_auto_cancel(
        &mut self,
        msg: &OKXOrderMsg,
        ts_init: UnixNanos,
        exec_reports: &mut Vec<ExecutionReport>,
    ) -> bool {
        if !Self::is_post_only_auto_cancel(msg) {
            return false;
        }

        let Some(client_order_id) = parse_client_order_id(&msg.cl_ord_id) else {
            return false;
        };

        let Some((_, (trader_id, strategy_id, instrument_id))) =
            self.active_client_orders.remove(&client_order_id)
        else {
            return false;
        };

        self.client_id_aliases.remove(&client_order_id);

        if !exec_reports.is_empty() {
            let reports = std::mem::take(exec_reports);
            self.pending_messages
                .push_back(NautilusWsMessage::ExecutionReports(reports));
        }

        let reason = msg
            .cancel_source_reason
            .as_ref()
            .filter(|reason| !reason.is_empty())
            .map_or_else(
                || Ustr::from(OKX_POST_ONLY_CANCEL_REASON),
                |reason| Ustr::from(reason.as_str()),
            );

        let ts_event = parse_millisecond_timestamp(msg.u_time);
        let rejected = OrderRejected::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            self.account_id,
            reason,
            UUID4::new(),
            ts_event,
            ts_init,
            false,
            true,
        );

        self.pending_messages
            .push_back(NautilusWsMessage::OrderRejected(rejected));

        true
    }

    fn drain_pending_requests_on_reconnect(&mut self) -> OKXWebSocketReconnect {
        const DETAIL_LIMIT: usize = 128;
        let pending_count = self.pending_place_requests.len()
            + self.pending_cancel_requests.len()
            + self.pending_amend_requests.len()
            + self.pending_mass_cancel_requests.len();
        let ts_observed_ns = self.clock.get_time_ns().as_u64();
        let mut pending = Vec::with_capacity(pending_count.min(DETAIL_LIMIT));
        for (_, (_, _, _, _, _, mut observation)) in self
            .pending_cancel_requests
            .drain()
            .chain(self.pending_amend_requests.drain())
        {
            if pending.len() < DETAIL_LIMIT {
                observation.ts_observed_ns = ts_observed_ns;
                observation.outcome = "unresolved_on_reconnect".into();
                pending.push(observation);
            }
        }
        self.pending_place_requests.clear();
        self.pending_mass_cancel_requests.clear();
        self.connection_generation += 1;
        self.observations_emitted = 0;
        self.observation_window_start = Instant::now();
        self.observation_window_date = ts_observed_ns;
        self.publish_capture_state();
        let event = OKXWebSocketReconnect {
            schema_version: 1,
            connection_id: self.connection_id.clone(),
            connection_generation: self.connection_generation,
            ts_observed_ns,
            pending_count,
            observations_dropped: self.observations_dropped,
            omitted_count: pending_count - pending.len(),
            pending,
        };
        log::warn!(
            "Websocket reconnected: pending={}, omitted={}; outcomes unresolved",
            event.pending_count,
            event.omitted_count
        );
        event
    }

    #[allow(clippy::too_many_arguments)]
    fn request_observation(
        &self,
        request_id: &str,
        op: OKXWsOperation,
        client_order_id: ClientOrderId,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
        requested_price: Option<String>,
        requested_size: Option<String>,
    ) -> OKXRequestObservation {
        let now = self.clock.get_time_ns().as_u64();
        OKXRequestObservation {
            schema_version: 1,
            connection_id: self.connection_id.clone(),
            connection_generation: self.connection_generation,
            request_id: request_id.into(),
            operation: op.to_string(),
            client_order_id: client_order_id.to_string(),
            venue_order_id: venue_order_id.map(|id| id.to_string()),
            instrument_id: instrument_id.to_string(),
            requested_price,
            requested_size,
            ts_handler_ns: now,
            ts_send_completed_ns: None,
            ts_observed_ns: now,
            outcome: "handler_received".into(),
            response_code: None,
            event_id: None,
            ts_received_ns: None,
            ts_venue_ns: None,
            observed_price: None,
            observed_size: None,
        }
    }

    fn receive_observation(&self, operation: &str, ts_init: UnixNanos) -> OKXRequestObservation {
        OKXRequestObservation {
            schema_version: 2,
            connection_id: self.connection_id.clone(),
            connection_generation: self.connection_generation,
            request_id: String::new(),
            operation: operation.into(),
            client_order_id: String::new(),
            venue_order_id: None,
            instrument_id: String::new(),
            requested_price: None,
            requested_size: None,
            ts_handler_ns: 0, // Unknown: no matched operation, never a duration anchor.
            ts_send_completed_ns: None,
            ts_observed_ns: ts_init.as_u64(),
            outcome: "received".into(),
            response_code: None,
            event_id: None,
            ts_received_ns: Some(ts_init.as_u64()),
            ts_venue_ns: None,
            observed_price: None,
            observed_size: None,
        }
    }

    fn refresh_observation_window(&mut self) {
        let windows = self.observation_window_start.elapsed().as_secs() / WINDOW.as_secs();
        if windows > 0 {
            let advance = std::time::Duration::from_secs(windows * WINDOW.as_secs());
            self.observation_window_start += advance;
            self.observation_window_date = self
                .observation_window_date
                .saturating_add(advance.as_nanos() as u64);
            self.observations_emitted = 0;
        }
    }

    pub(super) fn publish_capture_state(&self) {
        let snapshot = Arc::new(CaptureSnapshot {
            status: OKXCaptureStatus {
                schema_version: 1,
                scope: "okx_transport".into(),
                scope_id: self.connection_id.clone(),
                connection_generation: Some(self.connection_generation),
                ts_observed_ns: 0,
                window_start_ns: self.observation_window_date,
                window_duration_ns: WINDOW.as_nanos() as u64,
                window_limit: LIMIT as u64,
                window_attempts: self.observations_emitted.min(LIMIT) as u64,
                total_attempts: self.observations_total,
                observations_dropped: self.observations_dropped,
                status_sequence: 0,
                state: "active".into(),
            },
            window_start: self.observation_window_start,
            sequence: self.capture_sequence.clone(),
        });
        self.capture_state.rcu(|current| {
            if current.as_ref().is_some_and(|current| {
                current.status.scope_id != self.connection_id || current.status.state == "closed"
            }) {
                current.clone()
            } else {
                Some(snapshot.clone())
            }
        });
    }

    fn emit_request_observation(&mut self, observation: OKXRequestObservation) {
        if self
            .capture_state
            .load()
            .as_ref()
            .is_some_and(|snapshot| snapshot.status.state == "closed")
        {
            return;
        }
        self.refresh_observation_window();
        if self.observations_emitted >= LIMIT {
            self.observations_dropped = self.observations_dropped.saturating_add(1);
        } else {
            self.observations_emitted += 1;
            self.observations_total = self.observations_total.saturating_add(1);
            if self
                .send(NautilusWsMessage::RequestObservation(observation))
                .is_err()
            {
                self.observations_dropped = self.observations_dropped.saturating_add(1);
            }
        }
        self.publish_capture_state();
    }

    fn observe_send_completed(&mut self, request_id: &str) {
        let pending = self
            .pending_amend_requests
            .get_mut(request_id)
            .or_else(|| self.pending_cancel_requests.get_mut(request_id));
        if let Some((_, _, _, _, _, observation)) = pending {
            let now = self.clock.get_time_ns().as_u64();
            observation.ts_send_completed_ns = Some(now);
            observation.ts_observed_ns = now;
            observation.outcome = "send_completed".into();
            let observation = observation.clone();
            self.emit_request_observation(observation);
        }
    }

    fn register_client_order_aliases(
        &self,
        raw_child: &Option<ClientOrderId>,
        parent_from_msg: &Option<ClientOrderId>,
    ) -> Option<ClientOrderId> {
        if let Some(parent) = parent_from_msg {
            self.client_id_aliases.insert(*parent, *parent);
            if let Some(child) = raw_child.as_ref().filter(|child| **child != *parent) {
                self.client_id_aliases.insert(*child, *parent);
            }
            Some(*parent)
        } else if let Some(child) = raw_child.as_ref() {
            if let Some(mapped) = self.client_id_aliases.get(child) {
                Some(*mapped.value())
            } else {
                self.client_id_aliases.insert(*child, *child);
                Some(*child)
            }
        } else {
            None
        }
    }

    fn adjust_execution_report(
        &self,
        report: ExecutionReport,
        effective_client_id: &Option<ClientOrderId>,
        raw_child: &Option<ClientOrderId>,
    ) -> ExecutionReport {
        match report {
            ExecutionReport::Order(status_report) => {
                let mut adjusted = status_report;
                let mut final_id = *effective_client_id;

                if final_id.is_none() {
                    final_id = adjusted.client_order_id;
                }

                if final_id.is_none()
                    && let Some(child) = raw_child.as_ref()
                    && let Some(mapped) = self.client_id_aliases.get(child)
                {
                    final_id = Some(*mapped.value());
                }

                if let Some(final_id_value) = final_id {
                    if adjusted.client_order_id != Some(final_id_value) {
                        adjusted = adjusted.with_client_order_id(final_id_value);
                    }
                    self.client_id_aliases
                        .insert(final_id_value, final_id_value);

                    if let Some(child) =
                        raw_child.as_ref().filter(|child| **child != final_id_value)
                    {
                        adjusted = adjusted.with_linked_order_ids(vec![*child]);
                    }
                }

                ExecutionReport::Order(adjusted)
            }
            ExecutionReport::Fill(mut fill_report) => {
                let mut final_id = *effective_client_id;
                if final_id.is_none() {
                    final_id = fill_report.client_order_id;
                }

                if final_id.is_none()
                    && let Some(child) = raw_child.as_ref()
                    && let Some(mapped) = self.client_id_aliases.get(child)
                {
                    final_id = Some(*mapped.value());
                }

                if let Some(final_id_value) = final_id {
                    fill_report.client_order_id = Some(final_id_value);
                    self.client_id_aliases
                        .insert(final_id_value, final_id_value);
                }

                ExecutionReport::Fill(fill_report)
            }
        }
    }

    fn update_caches_with_report(&mut self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Fill(fill_report) => {
                let order_id = fill_report.venue_order_id.inner();
                let current_fee = self
                    .fee_cache
                    .get(&order_id)
                    .copied()
                    .unwrap_or_else(|| Money::new(0.0, fill_report.commission.currency));
                let total_fee = if current_fee.currency == fill_report.commission.currency {
                    current_fee + fill_report.commission
                } else {
                    log::warn!(
                        "Fee cache currency mismatch for {order_id}: cached={}, fill={}, resetting cache",
                        current_fee.currency.code,
                        fill_report.commission.currency.code,
                    );
                    fill_report.commission
                };
                self.fee_cache.insert(order_id, total_fee);

                let current_filled_qty = self
                    .filled_qty_cache
                    .get(&order_id)
                    .copied()
                    .unwrap_or_else(|| Quantity::zero(fill_report.last_qty.precision));
                let total_filled_qty = current_filled_qty + fill_report.last_qty;
                self.filled_qty_cache.insert(order_id, total_filled_qty);
            }
            ExecutionReport::Order(status_report) => {
                if matches!(status_report.order_status, OrderStatus::Filled) {
                    self.fee_cache.remove(&status_report.venue_order_id.inner());
                    self.filled_qty_cache
                        .remove(&status_report.venue_order_id.inner());
                }

                if matches!(
                    status_report.order_status,
                    OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Filled
                        | OrderStatus::Rejected,
                ) {
                    self.emitted_accepted.remove(&status_report.venue_order_id);
                    if let Some(client_order_id) = status_report.client_order_id {
                        self.order_state_cache.remove(&client_order_id);
                        self.active_client_orders.remove(&client_order_id);
                        self.client_id_aliases.remove(&client_order_id);
                    }

                    if let Some(linked) = &status_report.linked_order_ids {
                        for child in linked {
                            self.client_id_aliases.remove(child);
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn handle_order_response(
        &mut self,
        id: Option<String>,
        op: OKXWsOperation,
        code: String,
        msg: String,
        data: Vec<Value>,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let matched = id
            .as_deref()
            .and_then(|id| {
                self.pending_amend_requests
                    .get(id)
                    .or_else(|| self.pending_cancel_requests.get(id))
            })
            .filter(|pending| pending.5.operation == op.to_string());
        let mut observation = matched
            .map(|pending| pending.5.clone())
            .unwrap_or_else(|| self.receive_observation(&op.to_string(), ts_init));
        // The frame request id is observable even when no retained operation matches.
        observation.request_id = id.clone().unwrap_or_default();
        observation.ts_received_ns = Some(ts_init.as_u64());
        observation.ts_observed_ns = ts_init.as_u64();
        observation.outcome = "response_received".into();
        observation.response_code = Some(code.clone());
        let was_matched = matched.is_some();
        self.emit_request_observation(observation.clone());
        let result = self.handle_order_response_inner(id.clone(), op, code, msg, data, ts_init);
        let still_pending = id.as_deref().is_some_and(|id| {
            self.pending_amend_requests.contains_key(id)
                || self.pending_cancel_requests.contains_key(id)
        });
        if !was_matched || still_pending {
            observation.outcome = if was_matched {
                "response_unresolved"
            } else {
                "response_unmatched"
            }
            .into();
            observation.ts_observed_ns = self.clock.get_time_ns().as_u64();
            self.emit_request_observation(observation);
        }
        result
    }

    fn handle_order_response_inner(
        &mut self,
        id: Option<String>,
        op: OKXWsOperation,
        code: String,
        msg: String,
        data: Vec<Value>,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        if matches!(
            op,
            OKXWsOperation::AmendOrder | OKXWsOperation::CancelOrder | OKXWsOperation::CancelAlgos
        ) {
            let request_id = id.as_deref()?;
            let pending = if op == OKXWsOperation::AmendOrder {
                self.pending_amend_requests.get(request_id)
            } else {
                self.pending_cancel_requests.get(request_id)
            }?;
            let observation = &pending.5;
            if observation.operation != op.to_string() {
                return None;
            }
            // An operation id alone cannot make a malformed or wrong-item reply definitive.
            let valid_code = |value: &str| {
                !value.is_empty() && value.len() <= 16 && value.bytes().all(|b| b.is_ascii_digit())
            };
            if !valid_code(&code) {
                return None;
            }
            let effective_code = if data.is_empty() && code != "0" {
                code.as_str() // Explicit whole-request refusal, no contradictory item.
            } else {
                if data.len() != 1 {
                    return None;
                }
                let item = &data[0];
                let (client_key, venue_key) = if op == OKXWsOperation::CancelAlgos {
                    ("algoClOrdId", "algoId")
                } else {
                    ("clOrdId", "ordId")
                };
                let client_id = item
                    .get(client_key)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty());
                let venue_id = item
                    .get(venue_key)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty());
                let client_matches = client_id == Some(observation.client_order_id.as_str());
                let venue_matches =
                    venue_id.is_some() && venue_id == observation.venue_order_id.as_deref();
                if (!client_matches && !venue_matches)
                    || client_id.is_some_and(|id| id != observation.client_order_id)
                    || (venue_id.is_some()
                        && observation.venue_order_id.is_some()
                        && !venue_matches)
                {
                    return None;
                }
                let item_code = item.get("sCode").and_then(Value::as_str)?;
                if !valid_code(item_code) || (code != "0" && item_code == "0") {
                    return None;
                }
                item_code
            };
            let (
                client_order_id,
                trader_id,
                strategy_id,
                instrument_id,
                venue_order_id,
                mut observation,
            ) = if op == OKXWsOperation::AmendOrder {
                self.pending_amend_requests.remove(request_id)?
            } else {
                self.pending_cancel_requests.remove(request_id)?
            };
            observation.ts_observed_ns = ts_init.as_u64();
            observation.response_code = Some(effective_code.into());
            observation.outcome = if effective_code == "0" {
                "acknowledged"
            } else {
                "rejected"
            }
            .into();
            self.emit_request_observation(observation);
            if effective_code == "0" {
                return None;
            }
            let reason = data
                .first()
                .and_then(|item| item.get("sMsg"))
                .and_then(Value::as_str)
                .unwrap_or(&msg);
            let ts_event = self.clock.get_time_ns();
            return Some(if op == OKXWsOperation::AmendOrder {
                NautilusWsMessage::OrderModifyRejected(OrderModifyRejected::new(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Ustr::from(reason),
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    venue_order_id,
                    Some(self.account_id),
                ))
            } else {
                NautilusWsMessage::OrderCancelRejected(OrderCancelRejected::new(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Ustr::from(reason),
                    UUID4::new(),
                    ts_event,
                    ts_init,
                    false,
                    venue_order_id,
                    Some(self.account_id),
                ))
            });
        }

        if code == "0" {
            log::debug!("Order operation successful: id={id:?} op={op} code={code}");

            if op == OKXWsOperation::BatchCancelOrders {
                log::debug!(
                    "Batch cancel operation successful: id={id:?} cancel_count={}",
                    data.len()
                );

                // Check for per-order errors even when top-level code is "0"
                for (idx, entry) in data.iter().enumerate() {
                    if let Some(entry_code) = entry.get("sCode").and_then(|v| v.as_str())
                        && entry_code != "0"
                    {
                        let entry_msg = entry
                            .get("sMsg")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown error");

                        if let Some(cl_ord_id_str) = entry
                            .get("clOrdId")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                        {
                            log::error!(
                                "Batch cancel partial failure for order {cl_ord_id_str}: sCode={entry_code} sMsg={entry_msg}"
                            );
                            // TODO: Emit OrderCancelRejected for this specific order
                        } else {
                            log::error!(
                                "Batch cancel entry[{idx}] failed: sCode={entry_code} sMsg={entry_msg} data={entry:?}"
                            );
                        }
                    }
                }

                return None;
            } else if op == OKXWsOperation::MassCancel
                && let Some(request_id) = &id
                && let Some(instrument_id) = self.pending_mass_cancel_requests.remove(request_id)
            {
                log::debug!("Mass cancel operation successful for instrument: {instrument_id}");
            } else if op == OKXWsOperation::Order
                && let Some(request_id) = &id
                && let Some((params, client_order_id, trader_id, strategy_id, instrument_id)) =
                    self.pending_place_requests.remove(request_id)
            {
                let (venue_order_id, ts_accepted) = if let Some(first) = data.first() {
                    let ord_id = first
                        .get("ordId")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(VenueOrderId::new);

                    let ts = first
                        .get("ts")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map_or_else(
                            || self.clock.get_time_ns(),
                            |ms| UnixNanos::from(ms * 1_000_000),
                        );

                    (ord_id, ts)
                } else {
                    (None, self.clock.get_time_ns())
                };

                if let Some(instrument) = self.instruments_cache.get(&instrument_id.symbol.inner())
                {
                    match params {
                        PendingOrderParams::Regular(order_params) => {
                            let order_type = determine_order_type(
                                order_params.ord_type,
                                order_params.px.as_deref().unwrap_or(""),
                            );

                            let is_explicit_quote_sized = order_params
                                .tgt_ccy
                                .is_some_and(|tgt| tgt == OKXTargetCurrency::QuoteCcy);

                            // SPOT market BUY in cash mode with no tgt_ccy defaults to quote-sizing
                            let is_implicit_quote_sized = order_params.tgt_ccy.is_none()
                                && order_params.side == OKXSide::Buy
                                && order_type == OrderType::Market
                                && order_params.td_mode == OKXTradeMode::Cash
                                && instrument.instrument_class().as_ref() == "SPOT";

                            if is_explicit_quote_sized || is_implicit_quote_sized {
                                // For quote-sized orders, sz is in quote currency (USDT),
                                // not base currency (ETH). We can't accurately parse the
                                // base quantity without the fill price, so we skip the
                                // synthetic OrderAccepted and rely on the orders channel
                                log::debug!(
                                    "Skipping synthetic OrderAccepted for {} quote-sized order: client_order_id={client_order_id}, venue_order_id={venue_order_id:?}",
                                    if is_explicit_quote_sized {
                                        "explicit"
                                    } else {
                                        "implicit"
                                    },
                                );
                                return None;
                            }

                            let Some(v_order_id) = venue_order_id else {
                                log::error!(
                                    "No venue_order_id for accepted order: client_order_id={client_order_id}"
                                );
                                return None;
                            };

                            // Check if already emitted from orders channel push
                            if self.emitted_accepted.contains(&v_order_id) {
                                log::debug!(
                                    "Skipping duplicate OrderAccepted from operation response for venue_order_id={v_order_id}"
                                );
                                return None;
                            }
                            self.emitted_accepted.add(v_order_id);

                            let accepted = OrderAccepted::new(
                                trader_id,
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                v_order_id,
                                self.account_id,
                                UUID4::new(),
                                ts_accepted,
                                ts_init,
                                false, // Not from reconciliation
                            );

                            log::debug!(
                                "Order accepted: client_order_id={client_order_id}, venue_order_id={v_order_id}"
                            );

                            return Some(NautilusWsMessage::OrderAccepted(accepted));
                        }
                        PendingOrderParams::Algo(_) => {
                            log::debug!(
                                "Algo order placement confirmed: client_order_id={client_order_id}, venue_order_id={venue_order_id:?}"
                            );
                        }
                    }
                } else {
                    log::error!("Instrument not found for accepted order: {instrument_id}");
                }
            }

            if let Some(first) = data.first()
                && let Some(success_msg) = first.get("sMsg").and_then(|value| value.as_str())
            {
                log::debug!("Order details: {success_msg}");
            }

            return None;
        }

        let error_msg = data
            .first()
            .and_then(|d| d.get("sMsg"))
            .and_then(|s| s.as_str())
            .unwrap_or(&msg)
            .to_string();

        if let Some(first) = data.first() {
            log::debug!(
                "Error data fields: {}",
                serde_json::to_string_pretty(first)
                    .unwrap_or_else(|_| "unable to serialize".to_string())
            );
        }

        log::warn!("Order operation failed: id={id:?} op={op} code={code} msg={error_msg}");

        let ts_event = self.clock.get_time_ns();

        if let Some(request_id) = &id {
            match op {
                OKXWsOperation::Order => {
                    if let Some((_params, client_order_id, trader_id, strategy_id, instrument_id)) =
                        self.pending_place_requests.remove(request_id)
                    {
                        let due_post_only = is_post_only_rejection(code.as_str(), &data);
                        let rejected = OrderRejected::new(
                            trader_id,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            self.account_id,
                            Ustr::from(error_msg.as_str()),
                            UUID4::new(),
                            ts_event,
                            ts_init,
                            false, // Not from reconciliation
                            due_post_only,
                        );

                        return Some(NautilusWsMessage::OrderRejected(rejected));
                    }
                }
                OKXWsOperation::OrderAlgo => {
                    if let Some((_params, client_order_id, trader_id, strategy_id, instrument_id)) =
                        self.pending_place_requests.remove(request_id)
                    {
                        let due_post_only = is_post_only_rejection(code.as_str(), &data);
                        let rejected = OrderRejected::new(
                            trader_id,
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            self.account_id,
                            Ustr::from(error_msg.as_str()),
                            UUID4::new(),
                            ts_event,
                            ts_init,
                            false, // Not from reconciliation
                            due_post_only,
                        );

                        return Some(NautilusWsMessage::OrderRejected(rejected));
                    }
                }
                OKXWsOperation::MassCancel => {
                    if let Some(instrument_id) =
                        self.pending_mass_cancel_requests.remove(request_id)
                    {
                        log::error!(
                            "Mass cancel operation failed for {instrument_id}: code={code} msg={error_msg}"
                        );
                        let error = OKXWebSocketError {
                            code,
                            message: format!("Mass cancel failed for {instrument_id}: {error_msg}"),
                            conn_id: None,
                            timestamp: ts_event.as_u64(),
                        };
                        return Some(NautilusWsMessage::Error(error));
                    } else {
                        log::error!("Mass cancel operation failed: code={code} msg={error_msg}");
                    }
                }
                OKXWsOperation::BatchCancelOrders => {
                    log::warn!(
                        "Batch cancel operation failed: id={id:?} code={code} msg={error_msg} data_count={}",
                        data.len()
                    );

                    // Iterate through data array to check per-order errors
                    for (idx, entry) in data.iter().enumerate() {
                        let entry_code =
                            entry.get("sCode").and_then(|v| v.as_str()).unwrap_or(&code);
                        let entry_msg = entry
                            .get("sMsg")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&error_msg);

                        if entry_code != "0" {
                            // Try to extract client order ID for targeted error events
                            if let Some(cl_ord_id_str) = entry
                                .get("clOrdId")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                            {
                                log::error!(
                                    "Batch cancel failed for order {cl_ord_id_str}: sCode={entry_code} sMsg={entry_msg}"
                                );
                                // TODO: Emit OrderCancelRejected event once we track
                                // batch cancel metadata (client_order_id, trader_id, etc.)
                            } else {
                                log::error!(
                                    "Batch cancel entry[{idx}] failed: sCode={entry_code} sMsg={entry_msg} data={entry:?}"
                                );
                            }
                        }
                    }

                    // Emit generic error for the batch operation
                    let error = OKXWebSocketError {
                        code,
                        message: format!("Batch cancel failed: {error_msg}"),
                        conn_id: None,
                        timestamp: ts_event.as_u64(),
                    };
                    return Some(NautilusWsMessage::Error(error));
                }
                _ => log::warn!("Unhandled operation type for rejection: {op}"),
            }
        }

        let error = OKXWebSocketError {
            code,
            message: error_msg,
            conn_id: None,
            timestamp: ts_event.as_u64(),
        };
        Some(NautilusWsMessage::Error(error))
    }

    fn handle_book_data(
        &self,
        arg: OKXWebSocketArg,
        action: OKXBookAction,
        data: Vec<OKXBookMsg>,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let Some(inst_id) = arg.inst_id else {
            log::error!("Instrument ID missing for book data event");
            return None;
        };

        let inst = match self.instruments_cache.get(&inst_id) {
            Some(inst) => inst,
            None => {
                log::warn!("Instrument not in cache for book data: {inst_id}");
                return None;
            }
        };

        let instrument_id = inst.id();
        let price_precision = inst.price_precision();
        let size_precision = inst.size_precision();

        match parse_book_msg_vec(
            data,
            &instrument_id,
            price_precision,
            size_precision,
            action,
            ts_init,
        ) {
            Ok(payloads) => Some(NautilusWsMessage::Data(payloads)),
            Err(e) => {
                log::error!("Failed to parse book message: {e}");
                None
            }
        }
    }

    fn handle_account_data(
        &mut self,
        data: Value,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let Value::Array(arr) = data else {
            log::error!("Account data is not an array");
            return None;
        };

        let first = arr.into_iter().next()?;

        let account: OKXAccount = match serde_json::from_value(first) {
            Ok(acc) => acc,
            Err(e) => {
                log::error!("Failed to parse account data: {e}");
                return None;
            }
        };

        match parse_account_state(&account, self.account_id, ts_init) {
            Ok(account_state) => {
                if let Some(last_account_state) = &self.last_account_state
                    && account_state.has_same_balances_and_margins(last_account_state)
                {
                    return None;
                }
                self.last_account_state = Some(account_state.clone());
                Some(NautilusWsMessage::AccountUpdate(account_state))
            }
            Err(e) => {
                log::error!("Failed to parse account state: {e}");
                None
            }
        }
    }

    fn handle_positions_data(&mut self, data: Value, ts_init: UnixNanos) {
        match serde_json::from_value::<Vec<OKXPosition>>(data) {
            Ok(positions) => {
                log::debug!("Received {} position update(s)", positions.len());

                for position in positions {
                    let instrument = match self.instruments_cache.get(&position.inst_id) {
                        Some(inst) => inst,
                        None => {
                            log::warn!(
                                "Received position update for unknown instrument {}, skipping",
                                position.inst_id
                            );
                            continue;
                        }
                    };

                    let instrument_id = instrument.id();
                    let size_precision = instrument.size_precision();

                    match parse_position_status_report(
                        position,
                        self.account_id,
                        instrument_id,
                        size_precision,
                        ts_init,
                    ) {
                        Ok(position_report) => {
                            self.pending_messages
                                .push_back(NautilusWsMessage::PositionUpdate(position_report));
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse position status report for {instrument_id}: {e}"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to parse positions data: {e}");
            }
        }
    }

    fn handle_orders_data(&mut self, data: Value, ts_init: UnixNanos) -> Option<NautilusWsMessage> {
        let orders: Vec<OKXOrderMsg> = match serde_json::from_value(data) {
            Ok(orders) => orders,
            Err(e) => {
                log::error!("Failed to deserialize orders channel payload: {e}");
                let mut observation = self.receive_observation("orders", ts_init);
                observation.outcome = "malformed_orders".into();
                self.emit_request_observation(observation);
                return None;
            }
        };

        log::debug!(
            "Received {} order message(s) from orders channel",
            orders.len()
        );

        let mut exec_reports: Vec<ExecutionReport> = Vec::with_capacity(orders.len());

        for msg in orders {
            let mut observation = self.receive_observation("orders", ts_init);
            observation.client_order_id = msg.cl_ord_id.to_string();
            observation.venue_order_id = Some(msg.ord_id.to_string());
            observation.instrument_id = self.instruments_cache.get(&msg.inst_id).map_or_else(
                || msg.inst_id.to_string(),
                |instrument| instrument.id().to_string(),
            );
            observation.ts_venue_ns = Some(parse_millisecond_timestamp(msg.u_time).as_u64());
            observation.observed_price = Some(msg.px.clone());
            observation.observed_size = Some(msg.sz.clone());
            self.emit_request_observation(observation.clone());
            log::debug!(
                "Processing order message: inst_id={}, cl_ord_id={}, state={:?}, exec_type={:?}",
                msg.inst_id,
                msg.cl_ord_id,
                msg.state,
                msg.exec_type
            );

            if self.try_handle_post_only_auto_cancel(&msg, ts_init, &mut exec_reports) {
                observation.outcome = "post_only_cancel_handled".into();
                self.emit_request_observation(observation);
                continue;
            }

            let raw_child = parse_client_order_id(&msg.cl_ord_id);
            let parent_from_msg = msg
                .algo_cl_ord_id
                .as_ref()
                .filter(|value| !value.is_empty())
                .map(ClientOrderId::new);
            let effective_client_id =
                self.register_client_order_aliases(&raw_child, &parent_from_msg);

            let Some(instrument) = self.instruments_cache.get(&msg.inst_id) else {
                log::error!(
                    "No instrument found for inst_id: {inst_id}",
                    inst_id = msg.inst_id
                );
                observation.outcome = "missing_instrument".into();
                self.emit_request_observation(observation);
                continue;
            };
            let price_precision = instrument.price_precision();
            let size_precision = instrument.size_precision();

            let order_metadata = effective_client_id
                .and_then(|cid| self.active_client_orders.get(&cid).map(|e| *e.value()));

            let previous_fee = self.fee_cache.get(&msg.ord_id).copied();
            let previous_filled_qty = self.filled_qty_cache.get(&msg.ord_id).copied();
            let previous_state =
                effective_client_id.and_then(|cid| self.order_state_cache.get(&cid).cloned());
            // Do not regress confirmed live terms on delayed pushes. Fills still
            // pass through their existing trade-id/cumulative-quantity safeguards.
            if msg.state == OKXOrderStatus::Live
                && previous_state.as_ref().is_some_and(|previous| {
                    parse_millisecond_timestamp(msg.u_time) < previous.ts_event
                })
            {
                observation.outcome = "stale_or_duplicate".into();
                self.emit_request_observation(observation);
                continue;
            }

            // order_metadata being Some implies effective_client_id is Some
            if let (Some((trader_id, strategy_id, _instrument_id)), Some(canonical_client_id)) =
                (order_metadata, effective_client_id)
            {
                match parse_order_event(
                    &msg,
                    canonical_client_id,
                    self.account_id,
                    trader_id,
                    strategy_id,
                    instrument,
                    previous_fee,
                    previous_filled_qty,
                    previous_state.as_ref(),
                    ts_init,
                ) {
                    Ok(event) => {
                        // The first authoritative limit snapshot must reach the native
                        // cache even if an ACK already emitted acceptance. Submitted or
                        // acknowledged terms never seed this confirmed-state cache.
                        let first_terms = previous_state.is_none()
                            && !is_market_price(&msg.px)
                            && matches!(
                                &event,
                                ParsedOrderEvent::Accepted(_) | ParsedOrderEvent::Fill(_)
                            );
                        let terms = if first_terms {
                            parse_order_updated(
                                &msg,
                                canonical_client_id,
                                self.account_id,
                                trader_id,
                                strategy_id,
                                instrument,
                                ts_init,
                            )
                            .ok()
                            .map(ParsedOrderEvent::Updated)
                        } else if matches!(&event, ParsedOrderEvent::Fill(_))
                            && previous_state.as_ref().is_some_and(|previous| {
                                parse_millisecond_timestamp(msg.u_time) >= previous.ts_event
                            })
                        {
                            // A fill row may also confirm changed terms at the same
                            // millisecond. Compare terms; time equality is not identity.
                            let mut terms_msg = msg.clone();
                            terms_msg.state = OKXOrderStatus::Live;
                            terms_msg.fill_sz = "0".into();
                            terms_msg.trade_id.clear();
                            terms_msg.acc_fill_sz = None;
                            match parse_order_event(
                                &terms_msg,
                                canonical_client_id,
                                self.account_id,
                                trader_id,
                                strategy_id,
                                instrument,
                                previous_fee,
                                previous_filled_qty,
                                previous_state.as_ref(),
                                ts_init,
                            ) {
                                Ok(updated @ ParsedOrderEvent::Updated(_)) => Some(updated),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        let mut events = Vec::with_capacity(2);
                        if matches!(&event, ParsedOrderEvent::Accepted(_)) {
                            events.push(event);
                            events.extend(terms);
                        } else {
                            // Queue confirmed terms before the associated fill report.
                            events.extend(terms);
                            events.push(event);
                        }
                        for event in events {
                            let mut event_observation = observation.clone();
                            if let ParsedOrderEvent::Updated(updated) = &event {
                                let filled = previous_filled_qty
                                    .unwrap_or_else(|| Quantity::zero(size_precision));
                                let reported_filled = parse_quantity(
                                    msg.acc_fill_sz.as_deref().unwrap_or("0"),
                                    size_precision,
                                )
                                .ok();
                                if updated.quantity < filled
                                    || reported_filled.is_some_and(|qty| updated.quantity < qty)
                                {
                                    event_observation.outcome = "below_filled".into();
                                    self.emit_request_observation(event_observation);
                                    continue;
                                }
                                if previous_state
                                    .as_ref()
                                    .is_some_and(|previous| updated.ts_event < previous.ts_event)
                                {
                                    event_observation.outcome = "stale_or_duplicate".into();
                                    self.emit_request_observation(event_observation);
                                    continue;
                                }
                            }
                            let (outcome, event_id) = match &event {
                                ParsedOrderEvent::Accepted(e)
                                    if !self.emitted_accepted.contains(&e.venue_order_id) =>
                                {
                                    ("accepted_forwarded", Some(e.event_id.to_string()))
                                }
                                ParsedOrderEvent::Accepted(_) => {
                                    ("duplicate_accepted_seeded", None)
                                }
                                ParsedOrderEvent::Updated(e) => {
                                    ("updated_forwarded", Some(e.event_id.to_string()))
                                }
                                ParsedOrderEvent::Canceled(e) => {
                                    ("canceled_forwarded", Some(e.event_id.to_string()))
                                }
                                ParsedOrderEvent::Expired(e) => {
                                    ("expired_forwarded", Some(e.event_id.to_string()))
                                }
                                ParsedOrderEvent::Triggered(e) => {
                                    ("triggered_forwarded", Some(e.event_id.to_string()))
                                }
                                ParsedOrderEvent::Fill(_) => ("fill_report_forwarded", None),
                                ParsedOrderEvent::StatusOnly(_) => {
                                    ("status_report_forwarded", None)
                                }
                            };
                            event_observation.outcome = outcome.into();
                            event_observation.event_id = event_id;
                            self.process_parsed_order_event(
                                event,
                                &msg,
                                price_precision,
                                size_precision,
                                canonical_client_id,
                                &raw_child,
                                &mut exec_reports,
                            );
                            event_observation.ts_observed_ns = self.clock.get_time_ns().as_u64();
                            self.emit_request_observation(event_observation);
                        }
                        continue;
                    }
                    Err(e) => {
                        observation.outcome = "parse_error".into();
                        log::error!("Failed to parse order event: {e}");
                    }
                }
            } else {
                // External order or not tracked - use old parse_order_msg for backward compatibility
                match parse_order_msg(
                    &msg,
                    self.account_id,
                    &self.instruments_cache,
                    &self.fee_cache,
                    &self.filled_qty_cache,
                    ts_init,
                ) {
                    Ok(report) => {
                        log::debug!("Parsed external order as execution report: {report:?}");
                        let adjusted =
                            self.adjust_execution_report(report, &effective_client_id, &raw_child);
                        self.update_caches_with_report(&adjusted);
                        exec_reports.push(adjusted);
                        observation.outcome = "external_report_forwarded".into();
                    }
                    Err(e) => {
                        observation.outcome = "parse_error".into();
                        log::error!("Failed to parse order message: {e}");
                    }
                }
            }
            observation.ts_observed_ns = self.clock.get_time_ns().as_u64();
            self.emit_request_observation(observation);
        }

        if !exec_reports.is_empty() {
            log::debug!(
                "Pushing {count} execution report(s) to message queue",
                count = exec_reports.len()
            );
            self.pending_messages
                .push_back(NautilusWsMessage::ExecutionReports(exec_reports));
        }

        self.pending_messages.pop_front()
    }

    /// Processes a parsed order event and emits the appropriate message.
    #[allow(clippy::too_many_arguments)]
    fn process_parsed_order_event(
        &mut self,
        event: ParsedOrderEvent,
        msg: &OKXOrderMsg,
        price_precision: u8,
        size_precision: u8,
        canonical_client_id: ClientOrderId,
        raw_child: &Option<ClientOrderId>,
        exec_reports: &mut Vec<ExecutionReport>,
    ) {
        let venue_order_id = VenueOrderId::new(msg.ord_id);

        match event {
            ParsedOrderEvent::Accepted(accepted) => {
                // Only an orders-channel observation establishes confirmed terms.
                // Seed even when the placement response already emitted acceptance.
                self.update_order_state_cache(
                    &canonical_client_id,
                    msg,
                    price_precision,
                    size_precision,
                );
                if self.emitted_accepted.contains(&venue_order_id) {
                    log::debug!(
                        "Skipping duplicate OrderAccepted for venue_order_id={venue_order_id}"
                    );
                    return;
                }
                self.emitted_accepted.add(venue_order_id);

                self.pending_messages
                    .push_back(NautilusWsMessage::OrderAccepted(accepted));
            }
            ParsedOrderEvent::Canceled(canceled) => {
                self.cleanup_terminal_order(&canonical_client_id, &venue_order_id);
                self.pending_messages
                    .push_back(NautilusWsMessage::OrderCanceled(canceled));
            }
            ParsedOrderEvent::Expired(expired) => {
                self.cleanup_terminal_order(&canonical_client_id, &venue_order_id);
                self.pending_messages
                    .push_back(NautilusWsMessage::OrderExpired(expired));
            }
            ParsedOrderEvent::Triggered(triggered) => {
                self.update_order_state_cache(
                    &canonical_client_id,
                    msg,
                    price_precision,
                    size_precision,
                );
                self.pending_messages
                    .push_back(NautilusWsMessage::OrderTriggered(triggered));
            }
            ParsedOrderEvent::Updated(updated) => {
                self.update_order_state_cache(
                    &canonical_client_id,
                    msg,
                    price_precision,
                    size_precision,
                );
                self.pending_messages
                    .push_back(NautilusWsMessage::OrderUpdated(updated));
            }
            ParsedOrderEvent::Fill(fill_report) => {
                let effective_client_id = Some(canonical_client_id);
                let adjusted = self.adjust_execution_report(
                    ExecutionReport::Fill(fill_report),
                    &effective_client_id,
                    raw_child,
                );
                self.update_caches_with_report(&adjusted);

                if msg.state == OKXOrderStatus::Filled {
                    self.cleanup_terminal_order(&canonical_client_id, &venue_order_id);
                }

                exec_reports.push(adjusted);
            }
            ParsedOrderEvent::StatusOnly(status_report) => {
                let effective_client_id = Some(canonical_client_id);
                let adjusted = self.adjust_execution_report(
                    ExecutionReport::Order(*status_report),
                    &effective_client_id,
                    raw_child,
                );
                self.update_caches_with_report(&adjusted);
                exec_reports.push(adjusted);
            }
        }
    }

    /// Updates the order state cache for detecting future updates.
    fn update_order_state_cache(
        &mut self,
        client_order_id: &ClientOrderId,
        msg: &OKXOrderMsg,
        price_precision: u8,
        size_precision: u8,
    ) {
        let venue_order_id = VenueOrderId::new(msg.ord_id);
        let quantity = parse_quantity(&msg.sz, size_precision).ok();
        let price = if is_market_price(&msg.px) {
            None
        } else {
            parse_price(&msg.px, price_precision).ok()
        };

        if let Some(qty) = quantity {
            self.order_state_cache.insert(
                *client_order_id,
                OrderStateSnapshot {
                    venue_order_id,
                    ts_event: parse_millisecond_timestamp(msg.u_time),
                    quantity: qty,
                    price,
                },
            );
        }
    }

    /// Cleans up tracking state for terminal orders.
    fn cleanup_terminal_order(
        &mut self,
        client_order_id: &ClientOrderId,
        venue_order_id: &VenueOrderId,
    ) {
        self.emitted_accepted.remove(venue_order_id);
        self.order_state_cache.remove(client_order_id);
        self.active_client_orders.remove(client_order_id);
        self.client_id_aliases.remove(client_order_id);
        let stale_keys: Vec<ClientOrderId> = self
            .client_id_aliases
            .iter()
            .filter(|entry| entry.value() == client_order_id)
            .map(|entry| *entry.key())
            .collect();
        for key in stale_keys {
            self.client_id_aliases.remove(&key);
        }

        self.fee_cache.remove(&venue_order_id.inner());
        self.filled_qty_cache.remove(&venue_order_id.inner());
    }

    fn handle_algo_orders_data(
        &mut self,
        data: Value,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let orders: Vec<OKXAlgoOrderMsg> = match serde_json::from_value(data) {
            Ok(orders) => orders,
            Err(e) => {
                log::error!("Failed to deserialize algo orders payload: {e}");
                return None;
            }
        };

        let mut exec_reports: Vec<ExecutionReport> = Vec::with_capacity(orders.len());

        for msg in orders {
            let raw_child = parse_client_order_id(&msg.cl_ord_id);
            let parent_from_msg = parse_client_order_id(&msg.algo_cl_ord_id);
            let effective_client_id =
                self.register_client_order_aliases(&raw_child, &parent_from_msg);

            match parse_algo_order_msg(msg, self.account_id, &self.instruments_cache, ts_init) {
                Ok(Some(report)) => {
                    let adjusted =
                        self.adjust_execution_report(report, &effective_client_id, &raw_child);
                    self.update_caches_with_report(&adjusted);
                    exec_reports.push(adjusted);
                }
                Ok(None) => {}
                Err(e) => {
                    log::error!("Failed to parse algo order message: {e}");
                }
            }
        }

        if exec_reports.is_empty() {
            None
        } else {
            Some(NautilusWsMessage::ExecutionReports(exec_reports))
        }
    }

    fn handle_instruments_data(
        &mut self,
        data: Value,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let instruments: Vec<OKXInstrument> = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to deserialize instruments array: {e}");
                return None;
            }
        };

        let mut messages: Vec<NautilusWsMessage> = Vec::new();

        for msg in instruments {
            let inst_id_ustr = Ustr::from(&msg.inst_id);
            let status_action = okx_status_to_market_action(msg.state);
            let cached = self.instruments_cache.get(&inst_id_ustr);
            let (margin_init, margin_maint, maker_fee, taker_fee) = cached.map_or(
                (None, None, None, None),
                extract_fees_from_cached_instrument,
            );

            let instrument_id = cached.map_or_else(
                || InstrumentId::new(Symbol::new(inst_id_ustr), *OKX_VENUE),
                |inst| inst.id(),
            );

            let status = InstrumentStatus::new(
                instrument_id,
                status_action,
                ts_init,
                ts_init,
                None,
                None,
                Some(matches!(msg.state, OKXInstrumentStatus::Live)),
                None,
                None,
            );

            match parse_instrument_any(
                &msg,
                margin_init,
                margin_maint,
                maker_fee,
                taker_fee,
                ts_init,
            ) {
                Ok(Some(inst_any)) => {
                    self.instruments_cache
                        .insert(inst_any.symbol().inner(), inst_any.clone());
                    messages.push(NautilusWsMessage::Instrument(
                        Box::new(inst_any),
                        Some(status),
                    ));
                }
                Ok(None) => {
                    log::warn!("Could not parse instrument {inst_id_ustr}, emitting status only");
                    messages.push(NautilusWsMessage::InstrumentStatus(status));
                }
                Err(e) => {
                    log::error!("Failed to parse instrument {inst_id_ustr}: {e}");
                    messages.push(NautilusWsMessage::InstrumentStatus(status));
                }
            }
        }

        let mut iter = messages.into_iter();
        let first = iter.next();

        for msg in iter {
            self.pending_messages.push_back(msg);
        }

        first
    }

    fn handle_other_channel_data(
        &mut self,
        channel: OKXWsChannel,
        inst_id: Option<Ustr>,
        data: Value,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let Some(inst_id) = inst_id else {
            log::error!("No instrument for channel {channel:?}");
            return None;
        };

        // Index-tickers use base-pair (e.g., BTC-USDT) which may map to multiple
        // instruments (e.g., BTC-USDT-SWAP and BTC-USDT-250627). Emit one message
        // per mapped instrument, buffering extras via pending_messages.
        if channel == OKXWsChannel::IndexTickers {
            return if let Some(originals) = self.index_ticker_map.get(&inst_id) {
                let symbols: Vec<Ustr> = originals.iter().copied().collect();
                let mut first_msg = None;

                for symbol in symbols {
                    if let Some(msg) =
                        self.parse_channel_data(&channel, symbol, data.clone(), ts_init)
                    {
                        if first_msg.is_none() {
                            first_msg = Some(msg);
                        } else {
                            self.pending_messages.push_back(msg);
                        }
                    }
                }

                first_msg
            } else {
                None
            };
        }

        self.parse_channel_data(&channel, inst_id, data, ts_init)
    }

    fn parse_channel_data(
        &mut self,
        channel: &OKXWsChannel,
        inst_id: Ustr,
        data: Value,
        ts_init: UnixNanos,
    ) -> Option<NautilusWsMessage> {
        let Some(instrument) = self.instruments_cache.get(&inst_id) else {
            log::error!("No instrument for channel {channel:?}, inst_id {inst_id:?}");
            return None;
        };

        let instrument_id = instrument.id();
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();

        match parse_ws_message_data(
            channel,
            data,
            &instrument_id,
            price_precision,
            size_precision,
            ts_init,
            &mut self.funding_rate_cache,
            &self.instruments_cache,
        ) {
            Ok(Some(msg)) => {
                if let NautilusWsMessage::Instrument(ref inst, _) = msg {
                    self.instruments_cache
                        .insert(inst.symbol().inner(), inst.as_ref().clone());
                }
                Some(msg)
            }
            Ok(None) => None,
            Err(e) => {
                log::error!("Error parsing message for channel {channel:?}: {e}");
                None
            }
        }
    }

    pub(crate) fn parse_raw_message(
        msg: tokio_tungstenite::tungstenite::Message,
    ) -> Option<OKXWsMessage> {
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                if text == TEXT_PONG {
                    log::trace!("Received pong from OKX");
                    return None;
                }

                if text == TEXT_PING {
                    log::trace!("Received ping from OKX (text)");
                    return Some(OKXWsMessage::Ping);
                }

                if text == RECONNECTED {
                    log::debug!("Received WebSocket reconnection signal");
                    return Some(OKXWsMessage::Reconnected);
                }
                log::trace!("Received WebSocket message: {text}");

                match serde_json::from_str(&text) {
                    Ok(ws_event) => match &ws_event {
                        OKXWsMessage::Error { code, msg } => {
                            log::error!("WebSocket error: {code} - {msg}");
                            Some(ws_event)
                        }
                        OKXWsMessage::Login {
                            event,
                            code,
                            msg,
                            conn_id,
                        } => {
                            if code == "0" {
                                log::info!("WebSocket authenticated: conn_id={conn_id}");
                            } else {
                                log::error!(
                                    "WebSocket authentication failed: event={event}, code={code}, error={msg}"
                                );
                            }
                            Some(ws_event)
                        }
                        OKXWsMessage::Subscription {
                            event,
                            arg,
                            conn_id,
                            ..
                        } => {
                            let channel_str = serde_json::to_string(&arg.channel)
                                .expect("Invalid OKX websocket channel")
                                .trim_matches('"')
                                .to_string();
                            log::debug!("{event}d: channel={channel_str}, conn_id={conn_id}");
                            Some(ws_event)
                        }
                        OKXWsMessage::ChannelConnCount {
                            event: _,
                            channel,
                            conn_count,
                            conn_id,
                        } => {
                            let channel_str = serde_json::to_string(&channel)
                                .expect("Invalid OKX websocket channel")
                                .trim_matches('"')
                                .to_string();
                            log::debug!(
                                "Channel connection status: channel={channel_str}, connections={conn_count}, conn_id={conn_id}",
                            );
                            None
                        }
                        OKXWsMessage::Ping => {
                            log::trace!("Ignoring ping event parsed from text payload");
                            None
                        }
                        OKXWsMessage::Data { .. } => Some(ws_event),
                        OKXWsMessage::BookData { .. } => Some(ws_event),
                        OKXWsMessage::OrderResponse {
                            id,
                            op,
                            code,
                            msg: _,
                            data,
                        } => {
                            if code == "0" {
                                log::debug!(
                                    "Order operation successful: id={id:?}, op={op}, code={code}"
                                );

                                if let Some(order_data) = data.first() {
                                    let success_msg = order_data
                                        .get("sMsg")
                                        .and_then(|s| s.as_str())
                                        .unwrap_or("Order operation successful");
                                    log::debug!("Order success details: {success_msg}");
                                }
                            }
                            Some(ws_event)
                        }
                        OKXWsMessage::Reconnected => {
                            // This shouldn't happen as we handle RECONNECTED string directly
                            log::warn!("Unexpected Reconnected event from deserialization");
                            None
                        }
                    },
                    Err(e) => {
                        log::error!("Failed to parse message: {e}: {text}");
                        None
                    }
                }
            }
            Message::Ping(_payload) => {
                log::trace!("Received binary ping frame from OKX");
                Some(OKXWsMessage::Ping)
            }
            Message::Pong(payload) => {
                log::trace!("Received pong frame from OKX ({} bytes)", payload.len());
                None
            }
            Message::Binary(msg) => {
                log::debug!("Raw binary: {msg:?}");
                None
            }
            Message::Close(_) => {
                log::debug!("Received close message");
                None
            }
            msg => {
                log::warn!("Unexpected message: {msg}");
                None
            }
        }
    }

    fn generate_unique_request_id(&self) -> String {
        self.request_id_counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
    }

    fn get_instrument_type_and_family_from_instrument(
        instrument: &InstrumentAny,
    ) -> anyhow::Result<(OKXInstrumentType, String)> {
        let inst_type = okx_instrument_type(instrument)?;
        let symbol = instrument.symbol().inner();

        // Determine instrument family based on instrument type
        let inst_family = match instrument {
            InstrumentAny::CurrencyPair(_) => symbol.as_str().to_string(),
            InstrumentAny::CryptoPerpetual(_) => {
                // For SWAP: "BTC-USDT-SWAP" -> "BTC-USDT"
                symbol
                    .as_str()
                    .strip_suffix("-SWAP")
                    .unwrap_or(symbol.as_str())
                    .to_string()
            }
            InstrumentAny::CryptoFuture(_) => {
                // For FUTURES: "BTC-USDT-250328" -> "BTC-USDT"
                // Extract the base pair by removing date suffix
                let s = symbol.as_str();
                if let Some(idx) = s.rfind('-') {
                    s[..idx].to_string()
                } else {
                    s.to_string()
                }
            }
            _ => {
                anyhow::bail!("Unsupported instrument type for OKX");
            }
        };

        Ok((inst_type, inst_family))
    }

    async fn handle_mass_cancel(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let instrument = self
            .instruments_cache
            .get(&instrument_id.symbol.inner())
            .ok_or_else(|| anyhow::anyhow!("Unknown instrument {instrument_id}"))?;

        let (inst_type, inst_family) =
            Self::get_instrument_type_and_family_from_instrument(instrument)?;

        let params = WsMassCancelParams {
            inst_type,
            inst_family: Ustr::from(&inst_family),
        };

        let args =
            vec![serde_json::to_value(params).map_err(|e| anyhow::anyhow!("JSON error: {e}"))?];

        let request_id = self.generate_unique_request_id();

        self.pending_mass_cancel_requests
            .insert(request_id.clone(), instrument_id);

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::MassCancel,
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize mass cancel request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_CANCEL.as_slice()))
            .await
        {
            Ok(()) => {
                log::debug!("Sent mass cancel for {instrument_id}");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send mass cancel after retries: error={e}");

                self.pending_mass_cancel_requests.remove(&request_id);

                let error = OKXWebSocketError {
                    code: "CLIENT_ERROR".to_string(),
                    message: format!("Mass cancel failed for {instrument_id}: {e}"),
                    conn_id: None,
                    timestamp: self.clock.get_time_ns().as_u64(),
                };
                let _ = self.send(NautilusWsMessage::Error(error));

                Err(anyhow::anyhow!("Failed to send mass cancel: {e}"))
            }
        }
    }

    async fn handle_batch_cancel_orders(
        &self,
        args: Vec<Value>,
        request_id: String,
    ) -> anyhow::Result<()> {
        let request = OKXWsRequest {
            id: Some(request_id),
            op: OKXWsOperation::BatchCancelOrders,
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize batch cancel request: {e}"))?;

        if let Some(client) = &self.inner {
            client
                .send_text(payload, Some(OKX_RATE_LIMIT_KEY_CANCEL.as_slice()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send batch cancel: {e}"))?;
            log::debug!("Sent batch cancel orders");
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active WebSocket client"))
        }
    }

    async fn handle_batch_place_orders(
        &self,
        args: Vec<Value>,
        request_id: String,
    ) -> anyhow::Result<()> {
        let request = OKXWsRequest {
            id: Some(request_id),
            op: OKXWsOperation::BatchOrders,
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize batch place request: {e}"))?;

        if let Some(client) = &self.inner {
            client
                .send_text(payload, Some(OKX_RATE_LIMIT_KEY_ORDER.as_slice()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send batch place: {e}"))?;
            log::debug!("Sent batch place orders");
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active WebSocket client"))
        }
    }

    async fn handle_batch_amend_orders(
        &self,
        args: Vec<Value>,
        request_id: String,
    ) -> anyhow::Result<()> {
        let request = OKXWsRequest {
            id: Some(request_id),
            op: OKXWsOperation::BatchAmendOrders,
            exp_time: None,
            args,
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize batch amend request: {e}"))?;

        if let Some(client) = &self.inner {
            client
                .send_text(payload, Some(OKX_RATE_LIMIT_KEY_AMEND.as_slice()))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send batch amend: {e}"))?;
            log::debug!("Sent batch amend orders");
            Ok(())
        } else {
            Err(anyhow::anyhow!("No active WebSocket client"))
        }
    }

    async fn handle_subscribe(&self, args: Vec<OKXSubscriptionArg>) -> anyhow::Result<()> {
        for arg in &args {
            log::debug!(
                "Subscribing to channel: channel={:?}, inst_id={:?}",
                arg.channel,
                arg.inst_id
            );
        }

        let message = OKXSubscription {
            op: OKXWsOperation::Subscribe,
            args,
        };

        let json_txt = serde_json::to_string(&message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize subscription: {e}"))?;

        self.send_with_retry(json_txt, Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send subscription after retries: {e}"))?;
        Ok(())
    }

    async fn handle_unsubscribe(&self, args: Vec<OKXSubscriptionArg>) -> anyhow::Result<()> {
        for arg in &args {
            log::debug!(
                "Unsubscribing from channel: channel={:?}, inst_id={:?}",
                arg.channel,
                arg.inst_id
            );
        }

        let message = OKXSubscription {
            op: OKXWsOperation::Unsubscribe,
            args,
        };

        let json_txt = serde_json::to_string(&message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize unsubscription: {e}"))?;

        self.send_with_retry(json_txt, Some(OKX_RATE_LIMIT_KEY_SUBSCRIPTION.as_slice()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send unsubscription after retries: {e}"))?;
        Ok(())
    }

    async fn handle_place_order(
        &mut self,
        params: WsPostOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        let request_id = self.generate_unique_request_id();

        self.pending_place_requests.insert(
            request_id.clone(),
            (
                PendingOrderParams::Regular(params.clone()),
                client_order_id,
                trader_id,
                strategy_id,
                instrument_id,
            ),
        );

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::Order,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize place order request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_ORDER.as_slice()))
            .await
        {
            Ok(()) => {
                log::debug!("Sent place order request");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send place order after retries: error={e}");

                self.pending_place_requests.remove(&request_id);

                let ts_now = self.clock.get_time_ns();
                let rejected = OrderRejected::new(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    self.account_id,
                    Ustr::from(&format!("WebSocket send failed: {e}")),
                    UUID4::new(),
                    ts_now, // ts_event
                    ts_now, // ts_init
                    false,  // Not from reconciliation
                    false,  // Not due to post-only
                );
                let _ = self.send(NautilusWsMessage::OrderRejected(rejected));

                Err(anyhow::anyhow!("Failed to send place order: {e}"))
            }
        }
    }

    async fn handle_place_algo_order(
        &mut self,
        params: WsPostAlgoOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        let request_id = self.generate_unique_request_id();

        self.pending_place_requests.insert(
            request_id.clone(),
            (
                PendingOrderParams::Algo(params.clone()),
                client_order_id,
                trader_id,
                strategy_id,
                instrument_id,
            ),
        );

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::OrderAlgo,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize place algo order request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_ORDER.as_slice()))
            .await
        {
            Ok(()) => {
                log::debug!("Sent place algo order request");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send place algo order after retries: error={e}");

                self.pending_place_requests.remove(&request_id);

                let ts_now = self.clock.get_time_ns();
                let rejected = OrderRejected::new(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    self.account_id,
                    Ustr::from(&format!("WebSocket send failed: {e}")),
                    UUID4::new(),
                    ts_now, // ts_event
                    ts_now, // ts_init
                    false,  // Not from reconciliation
                    false,  // Not due to post-only
                );
                let _ = self.send(NautilusWsMessage::OrderRejected(rejected));

                Err(anyhow::anyhow!("Failed to send place algo order: {e}"))
            }
        }
    }

    async fn handle_cancel_order(
        &mut self,
        client_order_id: Option<ClientOrderId>,
        venue_order_id: Option<VenueOrderId>,
        instrument_id: InstrumentId,
        trader_id: TraderId,
        strategy_id: StrategyId,
    ) -> anyhow::Result<()> {
        let mut builder = WsCancelOrderParamsBuilder::default();
        builder.inst_id(instrument_id.symbol.as_str());

        // Look up instIdCode from cache (required for WebSocket orders per OKX deprecation)
        if let Some(inst_id_code) = self.inst_id_code_cache.get(&instrument_id.symbol.inner()) {
            builder.inst_id_code(*inst_id_code.value());
        }

        if let Some(venue_order_id) = venue_order_id {
            builder.ord_id(venue_order_id.as_str());
        }

        if let Some(client_order_id) = client_order_id {
            builder.cl_ord_id(client_order_id.as_str());
        }

        let params = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build cancel params: {e}"))?;

        let request_id = self.generate_unique_request_id();

        let observation = self.request_observation(
            &request_id,
            OKXWsOperation::CancelOrder,
            client_order_id.unwrap_or_else(|| ClientOrderId::new("UNKNOWN")),
            instrument_id,
            venue_order_id,
            None,
            None,
        );
        self.emit_request_observation(observation.clone());

        // Track pending request if we have a client order ID
        if let Some(client_order_id) = client_order_id {
            self.pending_cancel_requests.insert(
                request_id.clone(),
                (
                    client_order_id,
                    trader_id,
                    strategy_id,
                    instrument_id,
                    venue_order_id,
                    observation,
                ),
            );
        }

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::CancelOrder,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize cancel request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_CANCEL.as_slice()))
            .await
        {
            Ok(()) => {
                self.observe_send_completed(&request_id);
                log::debug!("Sent cancel order request");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send cancel order after retries: error={e}");

                self.pending_cancel_requests.remove(&request_id);

                if let Some(client_order_id) = client_order_id {
                    let ts_now = self.clock.get_time_ns();
                    let rejected = OrderCancelRejected::new(
                        trader_id,
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Ustr::from(&format!("WebSocket send failed: {e}")),
                        UUID4::new(),
                        ts_now, // ts_event
                        ts_now, // ts_init
                        false,  // Not from reconciliation
                        venue_order_id,
                        Some(self.account_id),
                    );
                    let _ = self.send(NautilusWsMessage::OrderCancelRejected(rejected));
                }

                Err(anyhow::anyhow!("Failed to send cancel order: {e}"))
            }
        }
    }

    async fn handle_amend_order(
        &mut self,
        params: WsAmendOrderParams,
        client_order_id: ClientOrderId,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
    ) -> anyhow::Result<()> {
        let request_id = self.generate_unique_request_id();

        let observation = self.request_observation(
            &request_id,
            OKXWsOperation::AmendOrder,
            client_order_id,
            instrument_id,
            venue_order_id,
            params.new_px.clone(),
            params.new_sz.clone(),
        );
        self.emit_request_observation(observation.clone());

        self.pending_amend_requests.insert(
            request_id.clone(),
            (
                client_order_id,
                trader_id,
                strategy_id,
                instrument_id,
                venue_order_id,
                observation,
            ),
        );

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::AmendOrder,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize amend order request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_AMEND.as_slice()))
            .await
        {
            Ok(()) => {
                self.observe_send_completed(&request_id);
                log::debug!("Sent amend order request");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send amend order after retries: error={e}");

                self.pending_amend_requests.remove(&request_id);

                let ts_now = self.clock.get_time_ns();
                let rejected = OrderModifyRejected::new(
                    trader_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Ustr::from(&format!("WebSocket send failed: {e}")),
                    UUID4::new(),
                    ts_now, // ts_event
                    ts_now, // ts_init
                    false,  // Not from reconciliation
                    venue_order_id,
                    Some(self.account_id),
                );
                let _ = self.send(NautilusWsMessage::OrderModifyRejected(rejected));

                Err(anyhow::anyhow!("Failed to send amend order: {e}"))
            }
        }
    }

    async fn handle_cancel_algo_order(
        &mut self,
        client_order_id: Option<ClientOrderId>,
        algo_order_id: Option<VenueOrderId>,
        instrument_id: InstrumentId,
        trader_id: TraderId,
        strategy_id: StrategyId,
    ) -> anyhow::Result<()> {
        let mut builder = WsCancelAlgoOrderParamsBuilder::default();
        builder.inst_id(instrument_id.symbol.as_str());

        // Look up instIdCode from cache (required for WebSocket orders per OKX deprecation)
        if let Some(inst_id_code) = self.inst_id_code_cache.get(&instrument_id.symbol.inner()) {
            builder.inst_id_code(*inst_id_code.value());
        }

        if let Some(client_order_id) = &client_order_id {
            builder.algo_cl_ord_id(client_order_id.as_str());
        }

        if let Some(algo_id) = &algo_order_id {
            builder.algo_id(algo_id.as_str());
        }

        let params = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build cancel algo params: {e}"))?;

        let request_id = self.generate_unique_request_id();

        let observation = self.request_observation(
            &request_id,
            OKXWsOperation::CancelAlgos,
            client_order_id.unwrap_or_else(|| ClientOrderId::new("UNKNOWN")),
            instrument_id,
            algo_order_id,
            None,
            None,
        );
        self.emit_request_observation(observation.clone());
        // Track pending cancellation if we have a client order ID
        if let Some(client_order_id) = client_order_id {
            self.pending_cancel_requests.insert(
                request_id.clone(),
                (
                    client_order_id,
                    trader_id,
                    strategy_id,
                    instrument_id,
                    algo_order_id,
                    observation,
                ),
            );
        }

        let request = OKXWsRequest {
            id: Some(request_id.clone()),
            op: OKXWsOperation::CancelAlgos,
            exp_time: None,
            args: vec![params],
        };

        let payload = serde_json::to_string(&request)
            .map_err(|e| anyhow::anyhow!("Failed to serialize cancel algo request: {e}"))?;

        match self
            .send_with_retry(payload, Some(OKX_RATE_LIMIT_KEY_CANCEL.as_slice()))
            .await
        {
            Ok(()) => {
                self.observe_send_completed(&request_id);
                log::debug!("Sent cancel algo order request");
                Ok(())
            }
            Err(e) => {
                log::error!("Failed to send cancel algo order after retries: error={e}");

                self.pending_cancel_requests.remove(&request_id);

                if let Some(client_order_id) = client_order_id {
                    let ts_now = self.clock.get_time_ns();
                    let rejected = OrderCancelRejected::new(
                        trader_id,
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Ustr::from(&format!("WebSocket send failed: {e}")),
                        UUID4::new(),
                        ts_now, // ts_event
                        ts_now, // ts_init
                        false,  // Not from reconciliation
                        None,
                        Some(self.account_id),
                    );
                    let _ = self.send(NautilusWsMessage::OrderCancelRejected(rejected));
                }

                Err(anyhow::anyhow!("Failed to send cancel algo order: {e}"))
            }
        }
    }
}

/// Returns `true` when an OKX error payload represents a post-only rejection.
pub fn is_post_only_rejection(code: &str, data: &[Value]) -> bool {
    if code == OKX_POST_ONLY_ERROR_CODE {
        return true;
    }

    for entry in data {
        if let Some(s_code) = entry.get("sCode").and_then(|value| value.as_str())
            && s_code == OKX_POST_ONLY_ERROR_CODE
        {
            return true;
        }

        if let Some(inner_code) = entry.get("code").and_then(|value| value.as_str())
            && inner_code == OKX_POST_ONLY_ERROR_CODE
        {
            return true;
        }
    }

    false
}

/// Case-insensitive substring check.
#[inline]
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

/// Determines if an OKX WebSocket error should trigger a retry.
fn should_retry_okx_error(error: &OKXWsError) -> bool {
    match error {
        OKXWsError::OkxError { error_code, .. } => should_retry_error_code(error_code),
        OKXWsError::TungsteniteError(_) => true, // Network errors are retryable
        OKXWsError::ClientError(msg) => {
            // Retry on timeout and connection errors
            contains_ignore_ascii_case(msg, "timeout")
                || contains_ignore_ascii_case(msg, "timed out")
                || contains_ignore_ascii_case(msg, "connection")
                || contains_ignore_ascii_case(msg, "network")
        }
        OKXWsError::AuthenticationError(_)
        | OKXWsError::JsonError(_)
        | OKXWsError::ParsingError(_) => {
            // Don't retry authentication or parsing errors automatically
            false
        }
    }
}

/// Creates a timeout error for the retry manager.
fn create_okx_timeout_error(msg: String) -> OKXWsError {
    OKXWsError::ClientError(msg)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    };

    use dashmap::DashMap;
    use nautilus_model::{
        identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
        types::{Money, Quantity},
    };
    use nautilus_network::websocket::{AuthTracker, SubscriptionState};
    use rstest::rstest;

    use super::{NautilusWsMessage, OKXWsFeedHandler, OKXWsOperation};
    use crate::websocket::parse::OrderStateSnapshot;
    use nautilus_core::UnixNanos;

    const OKX_WS_TOPIC_DELIMITER: char = ':';

    #[allow(clippy::type_complexity)]
    fn create_test_handler() -> (
        OKXWsFeedHandler,
        tokio::sync::mpsc::UnboundedReceiver<NautilusWsMessage>,
        Arc<DashMap<ClientOrderId, (TraderId, StrategyId, InstrumentId)>>,
        Arc<DashMap<ClientOrderId, ClientOrderId>>,
    ) {
        let account_id = AccountId::new("OKX-001");
        let signal = Arc::new(AtomicBool::new(false));
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        let active_client_orders = Arc::new(DashMap::new());
        let client_id_aliases = Arc::new(DashMap::new());
        let inst_id_code_cache = Arc::new(DashMap::new());
        let auth_tracker = AuthTracker::new();
        let subscriptions_state = SubscriptionState::new(OKX_WS_TOPIC_DELIMITER);

        let handler = OKXWsFeedHandler::new(
            account_id,
            signal,
            cmd_rx,
            raw_rx,
            out_tx,
            active_client_orders.clone(),
            client_id_aliases.clone(),
            inst_id_code_cache,
            auth_tracker,
            subscriptions_state,
            Arc::new(AtomicU64::new(0)),
        );

        (handler, out_rx, active_client_orders, client_id_aliases)
    }

    #[rstest]
    fn test_is_post_only_rejection_detects_by_code() {
        assert!(super::is_post_only_rejection("51019", &[]));
    }

    #[rstest]
    fn test_is_post_only_rejection_detects_by_inner_code() {
        let data = vec![serde_json::json!({
            "sCode": "51019"
        })];
        assert!(super::is_post_only_rejection("50000", &data));
    }

    #[rstest]
    fn test_is_post_only_rejection_false_for_unrelated_error() {
        let data = vec![serde_json::json!({
            "sMsg": "Insufficient balance"
        })];
        assert!(!super::is_post_only_rejection("50000", &data));
    }

    #[rstest]
    fn test_handler_register_client_order_aliases_with_parent() {
        let (handler, _out_rx, _active, client_id_aliases) = create_test_handler();

        let child = Some(ClientOrderId::new("CHILD-001"));
        let parent = Some(ClientOrderId::new("PARENT-001"));

        let result = handler.register_client_order_aliases(&child, &parent);

        assert_eq!(result, Some(ClientOrderId::new("PARENT-001")));
        assert!(client_id_aliases.contains_key(&ClientOrderId::new("PARENT-001")));
        assert!(client_id_aliases.contains_key(&ClientOrderId::new("CHILD-001")));
        assert_eq!(
            *client_id_aliases
                .get(&ClientOrderId::new("CHILD-001"))
                .unwrap(),
            ClientOrderId::new("PARENT-001")
        );
    }

    #[rstest]
    fn test_handler_register_client_order_aliases_without_parent() {
        let (handler, _out_rx, _active, client_id_aliases) = create_test_handler();

        let child = Some(ClientOrderId::new("ORDER-001"));
        let parent: Option<ClientOrderId> = None;

        let result = handler.register_client_order_aliases(&child, &parent);

        assert_eq!(result, Some(ClientOrderId::new("ORDER-001")));
        assert!(client_id_aliases.contains_key(&ClientOrderId::new("ORDER-001")));
        assert_eq!(
            *client_id_aliases
                .get(&ClientOrderId::new("ORDER-001"))
                .unwrap(),
            ClientOrderId::new("ORDER-001")
        );
    }

    #[rstest]
    fn test_handler_cleanup_terminal_order_removes_all_state() {
        let (mut handler, _out_rx, active_client_orders, client_id_aliases) = create_test_handler();

        let canonical = ClientOrderId::new("PARENT-001");
        let child = ClientOrderId::new("CHILD-001");
        let venue_id = VenueOrderId::new("VENUE-001");
        let trader_id = TraderId::new("TRADER-001");
        let strategy_id = StrategyId::new("STRATEGY-001");
        let instrument_id = InstrumentId::from("ETH-USDT-PERP.OKX");

        active_client_orders.insert(canonical, (trader_id, strategy_id, instrument_id));
        client_id_aliases.insert(canonical, canonical);
        client_id_aliases.insert(child, canonical);
        handler
            .fee_cache
            .insert(venue_id.inner(), Money::from("0.001 USDT"));
        handler
            .filled_qty_cache
            .insert(venue_id.inner(), Quantity::from("1.0"));
        handler.order_state_cache.insert(
            canonical,
            OrderStateSnapshot {
                ts_event: UnixNanos::default(),
                venue_order_id: venue_id,
                quantity: Quantity::from("1.0"),
                price: None,
            },
        );

        handler.cleanup_terminal_order(&canonical, &venue_id);

        assert!(!active_client_orders.contains_key(&canonical));
        assert!(!client_id_aliases.contains_key(&canonical));
        assert!(!client_id_aliases.contains_key(&child));
        assert!(!handler.fee_cache.contains_key(&venue_id.inner()));
        assert!(!handler.filled_qty_cache.contains_key(&venue_id.inner()));
        assert!(!handler.order_state_cache.contains_key(&canonical));
    }

    #[rstest]
    fn test_handler_cleanup_terminal_order_removes_multiple_children() {
        let (mut handler, _out_rx, _active, client_id_aliases) = create_test_handler();

        let canonical = ClientOrderId::new("PARENT-001");
        let child1 = ClientOrderId::new("CHILD-001");
        let child2 = ClientOrderId::new("CHILD-002");
        let child3 = ClientOrderId::new("CHILD-003");
        let venue_id = VenueOrderId::new("VENUE-001");

        client_id_aliases.insert(canonical, canonical);
        client_id_aliases.insert(child1, canonical);
        client_id_aliases.insert(child2, canonical);
        client_id_aliases.insert(child3, canonical);

        handler.cleanup_terminal_order(&canonical, &venue_id);

        assert!(!client_id_aliases.contains_key(&canonical));
        assert!(!client_id_aliases.contains_key(&child1));
        assert!(!client_id_aliases.contains_key(&child2));
        assert!(!client_id_aliases.contains_key(&child3));
        assert!(client_id_aliases.is_empty());
    }

    #[rstest]
    fn test_handler_cleanup_does_not_affect_other_orders() {
        let (mut handler, _out_rx, active_client_orders, client_id_aliases) = create_test_handler();

        let canonical1 = ClientOrderId::new("PARENT-001");
        let child1 = ClientOrderId::new("CHILD-001");
        let venue_id1 = VenueOrderId::new("VENUE-001");

        let canonical2 = ClientOrderId::new("PARENT-002");
        let child2 = ClientOrderId::new("CHILD-002");
        let venue_id2 = VenueOrderId::new("VENUE-002");

        let trader_id = TraderId::new("TRADER-001");
        let strategy_id = StrategyId::new("STRATEGY-001");
        let instrument_id = InstrumentId::from("ETH-USDT-PERP.OKX");

        active_client_orders.insert(canonical1, (trader_id, strategy_id, instrument_id));
        active_client_orders.insert(canonical2, (trader_id, strategy_id, instrument_id));
        client_id_aliases.insert(canonical1, canonical1);
        client_id_aliases.insert(child1, canonical1);
        client_id_aliases.insert(canonical2, canonical2);
        client_id_aliases.insert(child2, canonical2);
        handler
            .fee_cache
            .insert(venue_id1.inner(), Money::from("0.001 USDT"));
        handler
            .fee_cache
            .insert(venue_id2.inner(), Money::from("0.002 USDT"));

        handler.cleanup_terminal_order(&canonical1, &venue_id1);

        assert!(!active_client_orders.contains_key(&canonical1));
        assert!(!client_id_aliases.contains_key(&canonical1));
        assert!(!client_id_aliases.contains_key(&child1));
        assert!(!handler.fee_cache.contains_key(&venue_id1.inner()));

        assert!(active_client_orders.contains_key(&canonical2));
        assert!(client_id_aliases.contains_key(&canonical2));
        assert!(client_id_aliases.contains_key(&child2));
        assert!(handler.fee_cache.contains_key(&venue_id2.inner()));
    }

    mod channel_routing {
        use crate::websocket::{
            handler::PendingOrderParams,
            messages::{ExecutionReport, WsPostOrderParams},
        };
        use ahash::AHashSet;
        use nautilus_core::nanos::UnixNanos;
        use nautilus_model::{
            enums::MarketStatusAction,
            identifiers::{InstrumentId, Symbol},
            instruments::{CryptoPerpetual, CurrencyPair, Instrument, InstrumentAny},
            types::{Currency, Price, Quantity},
        };
        use rstest::rstest;
        use ustr::Ustr;

        use super::*;
        use crate::{
            common::{enums::OKXBookAction, testing::load_test_json},
            websocket::{enums::OKXWsChannel, messages::OKXWsMessage},
        };

        fn create_spot_instrument() -> InstrumentAny {
            let instrument_id = InstrumentId::from("BTC-USDT.OKX");
            InstrumentAny::CurrencyPair(CurrencyPair::new(
                instrument_id,
                Symbol::from("BTC-USDT"),
                Currency::BTC(),
                Currency::USDT(),
                2,
                8,
                Price::from("0.01"),
                Quantity::from("0.00000001"),
                None, // multiplier
                None, // lot_size
                None, // max_quantity
                None, // min_quantity
                None, // max_notional
                None, // min_notional
                None, // max_price
                None, // min_price
                None, // margin_init
                None, // margin_maint
                None, // maker_fee
                None, // taker_fee
                None, // info
                UnixNanos::default(),
                UnixNanos::default(),
            ))
        }

        fn create_swap_instrument() -> InstrumentAny {
            let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");
            InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
                instrument_id,
                Symbol::from("BTC-USDT-SWAP"),
                Currency::BTC(),
                Currency::USDT(),
                Currency::USDT(),
                false,
                2,
                8,
                Price::from("0.01"),
                Quantity::from("0.00000001"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                UnixNanos::default(),
                UnixNanos::default(),
            ))
        }

        fn create_handler_with_instruments(instruments: Vec<InstrumentAny>) -> OKXWsFeedHandler {
            let (mut handler, _, _, _) = create_test_handler();
            for inst in instruments {
                handler
                    .instruments_cache
                    .insert(inst.symbol().inner(), inst);
            }
            handler
        }

        #[rstest]
        #[case(true)]
        #[case(false)]
        fn test_amend_response_and_order_push_are_independent(#[case] response_first: bool) {
            let mut handler = create_handler_with_instruments(vec![create_swap_instrument()]);
            insert_pending_amend(&mut handler, "req-1");
            let context = handler.pending_amend_requests.get("req-1").unwrap();
            handler
                .active_client_orders
                .insert(context.0, (context.1, context.2, context.3));
            let mut payload: serde_json::Value =
                serde_json::from_str(&load_test_json("ws_orders.json")).unwrap();
            let item = &mut payload["data"][0];
            for (key, value) in [
                ("clOrdId", "TEST-ORDER"),
                ("ordId", "12345"),
                ("tradeId", ""),
                ("state", "live"),
                ("ordType", "limit"),
                ("px", "100"),
                ("sz", "1"),
                ("accFillSz", "0"),
                ("fillSz", "0"),
                ("fillPx", ""),
                ("fillTime", ""),
                ("avgPx", ""),
                ("fee", "0"),
                ("cTime", "1000"),
                ("uTime", "1000"),
            ] {
                item[key] = value.into();
            }
            let initial =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(1000u64));
            assert!(matches!(initial, Some(NautilusWsMessage::OrderAccepted(_))));
            assert!(matches!(
                handler.pending_messages.pop_front(),
                Some(NautilusWsMessage::OrderUpdated(_))
            ));
            let acknowledge = |handler: &mut OKXWsFeedHandler| {
                assert!(
                    handler
                        .handle_order_response(
                            Some("req-1".into()),
                            OKXWsOperation::AmendOrder,
                            "0".into(),
                            String::new(),
                            vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                            UnixNanos::from(2000u64)
                        )
                        .is_none()
                );
            };
            if response_first {
                acknowledge(&mut handler);
                assert_eq!(
                    handler.order_state_cache[&ClientOrderId::new("TEST-ORDER")].quantity,
                    Quantity::from("1")
                );
            }
            payload["data"][0]["sz"] = "2".into();
            payload["data"][0]["uTime"] = "2000".into();
            let updated =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(3000u64));
            assert!(matches!(updated, Some(NautilusWsMessage::OrderUpdated(_))));
            if !response_first {
                acknowledge(&mut handler);
            }
            assert_eq!(
                handler.order_state_cache[&ClientOrderId::new("TEST-ORDER")].quantity,
                Quantity::from("2")
            );
            assert!(handler.pending_amend_requests.is_empty());
            assert!(
                handler
                    .handle_orders_data(payload["data"].clone(), UnixNanos::from(4000u64))
                    .is_none()
            );
        }

        #[rstest]
        #[case(true)]
        #[case(false)]
        fn test_placement_response_seeds_later_amendments(
            #[case] response_first: bool,
            #[values(false, true)] saturated: bool,
        ) {
            let (mut handler, mut observations, _, _) = create_test_handler();
            let instrument = create_swap_instrument();
            handler
                .instruments_cache
                .insert(instrument.symbol().inner(), instrument);
            if saturated {
                handler.observations_emitted = 4097;
            }
            insert_pending_amend(&mut handler, "req-1");
            let context = handler.pending_amend_requests.get("req-1").unwrap();
            handler
                .active_client_orders
                .insert(context.0, (context.1, context.2, context.3));
            let mut payload: serde_json::Value =
                serde_json::from_str(&load_test_json("ws_orders.json")).unwrap();
            let item = &mut payload["data"][0];
            for (key, value) in [
                ("clOrdId", "TEST-ORDER"),
                ("ordId", "12345"),
                ("tradeId", ""),
                ("state", "live"),
                ("ordType", "limit"),
                ("px", "100"),
                ("sz", "1"),
                ("accFillSz", "0"),
                ("fillSz", "0"),
                ("fillPx", ""),
                ("fillTime", ""),
                ("avgPx", ""),
                ("fee", "0"),
                ("cTime", "1000"),
                ("uTime", "1000"),
            ] {
                item[key] = value.into();
            }
            let params: WsPostOrderParams = serde_json::from_value(serde_json::json!({
                "instId":"BTC-USDT-SWAP", "tdMode":"cross", "side":"buy",
                "ordType":"limit", "sz":"1", "px":"100", "clOrdId":"TEST-ORDER"
            }))
            .unwrap();
            let context = handler.pending_amend_requests["req-1"].clone();
            handler.pending_place_requests.insert(
                "place-1".into(),
                (
                    PendingOrderParams::Regular(params),
                    context.0,
                    context.1,
                    context.2,
                    context.3,
                ),
            );
            let placed = handler.handle_order_response(
                Some("place-1".into()),
                OKXWsOperation::Order,
                "0".into(),
                String::new(),
                vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                UnixNanos::from(900u64),
            );
            assert!(matches!(placed, Some(NautilusWsMessage::OrderAccepted(_))));
            assert!(handler.order_state_cache.is_empty());
            let initial =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(1000u64));
            assert!(matches!(initial, Some(NautilusWsMessage::OrderUpdated(_))));
            let acknowledge = |handler: &mut OKXWsFeedHandler| {
                assert!(
                    handler
                        .handle_order_response(
                            Some("req-1".into()),
                            OKXWsOperation::AmendOrder,
                            "0".into(),
                            String::new(),
                            vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                            UnixNanos::from(2000u64)
                        )
                        .is_none()
                );
            };
            if response_first {
                acknowledge(&mut handler);
                assert_eq!(
                    handler.order_state_cache[&ClientOrderId::new("TEST-ORDER")].quantity,
                    Quantity::from("1")
                );
            }
            payload["data"][0]["sz"] = "2".into();
            payload["data"][0]["uTime"] = "2000".into();
            let updated =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(3000u64));
            let Some(NautilusWsMessage::OrderUpdated(updated)) = updated else {
                panic!("missing update")
            };
            let updated_id = updated.event_id.to_string();
            if !response_first {
                acknowledge(&mut handler);
            }
            assert_eq!(
                handler.order_state_cache[&ClientOrderId::new("TEST-ORDER")].quantity,
                Quantity::from("2")
            );
            assert!(handler.pending_amend_requests.is_empty());
            payload["data"][0]["px"] = "110".into();
            payload["data"][0]["uTime"] = "3000".into();
            assert!(matches!(
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(3500u64)),
                Some(NautilusWsMessage::OrderUpdated(_))
            ));
            assert!(
                handler
                    .handle_orders_data(payload["data"].clone(), UnixNanos::from(4000u64))
                    .is_none()
            );
            // Older live terms must not regress the confirmed price or quantity.
            payload["data"][0]["px"] = "90".into();
            payload["data"][0]["uTime"] = "2000".into();
            assert!(
                handler
                    .handle_orders_data(payload["data"].clone(), UnixNanos::from(5000u64))
                    .is_none()
            );
            if saturated {
                let exhausted =
                    super::super::super::capture::read_capture(&handler.capture_state).unwrap();
                assert_eq!(exhausted.state, "exhausted");
                assert!(exhausted.observations_dropped > 0);
                // Diagnostic time advances without reconnect/restart. Getter
                // projection cannot independently refill the real budget.
                handler.observation_window_start -= std::time::Duration::from_secs(60);
                handler.observation_window_date -= 60_000_000_000;
                handler.publish_capture_state();
                for _ in 0..2 {
                    let status =
                        super::super::super::capture::read_capture(&handler.capture_state).unwrap();
                    assert_eq!(status.window_attempts, 0);
                    assert_eq!(status.state, "active");
                    assert_eq!(handler.observations_emitted, 4097);
                }
            }
            // An amendment and fill in the same push must deliver terms first.
            for (key, value) in [
                ("px", "120"),
                ("sz", "3"),
                ("state", "partially_filled"),
                ("fillSz", "0.5"),
                ("accFillSz", "0.5"),
                ("fillPx", "120"),
                ("tradeId", "42"),
                ("fillTime", "4000"),
                ("uTime", "4000"),
            ] {
                payload["data"][0][key] = value.into();
            }
            assert!(matches!(
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(6000u64)),
                Some(NautilusWsMessage::OrderUpdated(_))
            ));
            assert!(matches!(
                handler.pending_messages.pop_front(),
                Some(NautilusWsMessage::ExecutionReports(_))
            ));
            handler.drain_pending_requests_on_reconnect();
            for (key, value) in [
                ("px", "130"),
                ("uTime", "5000"),
                ("fillSz", "0"),
                ("tradeId", ""),
                ("state", "live"),
            ] {
                payload["data"][0][key] = value.into();
            }
            assert!(matches!(
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(7000u64)),
                Some(NautilusWsMessage::OrderUpdated(_))
            ));
            let mut diagnostics = Vec::new();
            while let Ok(NautilusWsMessage::RequestObservation(item)) = observations.try_recv() {
                diagnostics.push(item);
            }
            if !saturated {
                let forwarded = diagnostics
                    .iter()
                    .find(|item| item.event_id.as_deref() == Some(&updated_id))
                    .unwrap();
                assert_eq!(forwarded.outcome, "updated_forwarded");
                assert_eq!(forwarded.schema_version, 2);
                assert_eq!(forwarded.ts_received_ns, Some(3000));
                assert_eq!(forwarded.ts_venue_ns, Some(2_000_000_000));
                assert!(forwarded.request_id.is_empty());
                assert_eq!(forwarded.ts_handler_ns, 0);
                assert!(
                    diagnostics
                        .iter()
                        .any(|item| item.outcome == "duplicate_accepted_seeded"
                            && item.event_id.is_none())
                );
                assert!(diagnostics.iter().any(|item| item.outcome == "stale_or_duplicate" && item.event_id.is_none()));
            } else {
                assert!(handler.observations_dropped > 0);
                assert!(
                    diagnostics
                        .iter()
                        .any(|item| item.outcome == "updated_forwarded"
                            && item.ts_received_ns == Some(6000)
                            && item.event_id.is_some())
                );
            }
        }

        #[rstest]
        fn test_first_snapshot_and_tied_terms(
            #[values(false, true)] first_fill: bool,
            #[values(false, true)] seeded: bool,
        ) {
            let (mut handler, _, _, _) = create_test_handler();
            let instrument = create_swap_instrument();
            handler
                .instruments_cache
                .insert(instrument.symbol().inner(), instrument);
            insert_pending_amend(&mut handler, "req-1");
            let context = handler.pending_amend_requests["req-1"].clone();
            handler
                .active_client_orders
                .insert(context.0, (context.1, context.2, context.3));
            let params: WsPostOrderParams = serde_json::from_value(serde_json::json!({
                "instId":"BTC-USDT-SWAP", "tdMode":"cross", "side":"buy",
                "ordType":"limit", "sz":"1", "px":"100", "clOrdId":"TEST-ORDER"
            }))
            .unwrap();
            handler.pending_place_requests.insert(
                "place-1".into(),
                (
                    PendingOrderParams::Regular(params),
                    context.0,
                    context.1,
                    context.2,
                    context.3,
                ),
            );
            assert!(matches!(
                handler.handle_order_response(
                    Some("place-1".into()),
                    OKXWsOperation::Order,
                    "0".into(),
                    String::new(),
                    vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                    UnixNanos::from(900u64),
                ),
                Some(NautilusWsMessage::OrderAccepted(_))
            ));
            assert!(handler.order_state_cache.is_empty());
            let mut payload: serde_json::Value =
                serde_json::from_str(&load_test_json("ws_orders.json")).unwrap();
            for (key, value) in [
                ("clOrdId", "TEST-ORDER"),
                ("ordId", "12345"),
                ("state", "live"),
                ("ordType", "limit"),
                ("px", "100"),
                ("sz", "1"),
                ("accFillSz", "0"),
                ("fillSz", "0"),
                ("tradeId", ""),
                ("fillPx", ""),
                ("fillTime", ""),
                ("avgPx", ""),
                ("fee", "0"),
                ("cTime", "1000"),
                ("uTime", "1000"),
            ] {
                payload["data"][0][key] = value.into();
            }
            if seeded {
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(1000u64));
                handler.pending_messages.clear();
            }
            // A successful amend reply must not establish confirmed terms.
            assert!(
                handler
                    .handle_order_response(
                        Some("req-1".into()),
                        OKXWsOperation::AmendOrder,
                        "0".into(),
                        String::new(),
                        vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                        UnixNanos::from(2000u64),
                    )
                    .is_none()
            );
            assert_eq!(handler.order_state_cache.is_empty(), !seeded);
            payload["data"][0]["sz"] = "2".into();
            payload["data"][0]["px"] = "110".into();
            if first_fill {
                for (key, value) in [
                    ("state", "partially_filled"),
                    ("fillSz", "1.5"),
                    ("accFillSz", "1.5"),
                    ("fillPx", "110"),
                    ("tradeId", "77"),
                    ("fillTime", "1000"),
                    ("fee", "-1.25"),
                    ("feeCcy", "USDT"),
                ] {
                    payload["data"][0][key] = value.into();
                }
            }
            let Some(NautilusWsMessage::OrderUpdated(updated)) =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(3000u64))
            else {
                panic!(
                    "confirmed terms must precede the fill even without a seed or at a tied timestamp"
                );
            };
            assert_eq!(updated.reconciliation, 0); // Cannot bypass native terminal guards.
            #[cfg(feature = "python")]
            {
                use pyo3::prelude::*;
                Python::initialize();
                Python::attach(|py| {
                    let expected_id = updated.event_id.to_string();
                    let object = updated.into_pyobject(py).unwrap();
                    let values = object.call_method0("to_dict").unwrap();
                    assert_eq!(
                        values
                            .get_item("event_id")
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        expected_id
                    );
                    assert_eq!(
                        values
                            .get_item("reconciliation")
                            .unwrap()
                            .extract::<u8>()
                            .unwrap(),
                        0
                    );
                });
            }
            assert_eq!(updated.quantity, Quantity::from("2"));
            assert_eq!(updated.price.unwrap(), Price::from("110"));
            assert_eq!(updated.ts_event, UnixNanos::from(1_000_000_000u64));
            if first_fill {
                let Some(NautilusWsMessage::ExecutionReports(reports)) =
                    handler.pending_messages.pop_front()
                else {
                    panic!("missing fill after terms");
                };
                let ExecutionReport::Fill(fill) = &reports[0] else {
                    panic!("not a fill");
                };
                assert_eq!(fill.last_qty, Quantity::from("1.5"));
                assert_eq!(fill.commission, Money::from("1.25 USDT"));
            }
            assert!(handler.pending_messages.is_empty());
            // Replayed fills retain their trade identity for native deduplication;
            // they must not emit another terms event.
            let repeated =
                handler.handle_orders_data(payload["data"].clone(), UnixNanos::from(4000u64));
            if first_fill {
                let Some(NautilusWsMessage::ExecutionReports(reports)) = repeated else {
                    panic!("missing replay report");
                };
                let ExecutionReport::Fill(fill) = &reports[0] else {
                    panic!("not a replay fill");
                };
                assert_eq!(fill.trade_id.to_string(), "77");
            } else {
                assert!(repeated.is_none());
            }
            // Older changed terms remain fenced.
            payload["data"][0]["state"] = "live".into();
            payload["data"][0]["sz"] = "3".into();
            payload["data"][0]["uTime"] = "999".into();
            assert!(
                handler
                    .handle_orders_data(payload["data"].clone(), UnixNanos::from(5000u64))
                    .is_none()
            );
            assert_eq!(
                handler.order_state_cache[&context.0].quantity,
                Quantity::from("2")
            );
        }

        #[rstest]
        fn test_parse_raw_message_ticker_channel() {
            let json = load_test_json("ws_tickers.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::Data { arg, data } => {
                    assert!(
                        matches!(arg.channel, OKXWsChannel::Tickers),
                        "Expected Tickers channel"
                    );
                    assert_eq!(arg.inst_id, Some(Ustr::from("BTC-USDT")));
                    assert!(data.is_array());
                }
                _ => panic!("Expected OKXWsMessage::Data variant"),
            }
        }

        #[rstest]
        fn test_parse_raw_message_trades_channel() {
            let json = load_test_json("ws_trades.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::Data { arg, data } => {
                    assert!(
                        matches!(arg.channel, OKXWsChannel::Trades),
                        "Expected Trades channel"
                    );
                    assert_eq!(arg.inst_id, Some(Ustr::from("BTC-USD")));
                    assert!(data.is_array());
                }
                _ => panic!("Expected OKXWsMessage::Data variant"),
            }
        }

        #[rstest]
        fn test_parse_raw_message_books_channel() {
            let json = load_test_json("ws_books_snapshot.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::BookData { arg, action, data } => {
                    assert!(
                        matches!(arg.channel, OKXWsChannel::Books),
                        "Expected Books channel"
                    );
                    assert_eq!(arg.inst_id, Some(Ustr::from("BTC-USDT")));
                    assert!(
                        matches!(action, OKXBookAction::Snapshot),
                        "Expected snapshot action"
                    );
                    assert!(!data.is_empty());
                }
                _ => panic!("Expected OKXWsMessage::BookData variant"),
            }
        }

        #[rstest]
        fn test_parse_raw_message_candle_channel() {
            let json = load_test_json("ws_candle.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::Data { arg, data } => {
                    // Candle channel variant is Candle1Day for "candle1D"
                    assert!(
                        matches!(arg.channel, OKXWsChannel::Candle1Day),
                        "Expected Candle1Day channel, was {:?}",
                        arg.channel
                    );
                    assert_eq!(arg.inst_id, Some(Ustr::from("BTC-USDT")));
                    assert!(data.is_array());
                }
                _ => panic!("Expected OKXWsMessage::Data variant"),
            }
        }

        #[rstest]
        fn test_parse_raw_message_funding_rate_channel() {
            let json = load_test_json("ws_funding_rate.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::Data { arg, data } => {
                    assert!(
                        matches!(arg.channel, OKXWsChannel::FundingRate),
                        "Expected FundingRate channel"
                    );
                    assert_eq!(arg.inst_id, Some(Ustr::from("BTC-USDT-SWAP")));
                    assert!(data.is_array());
                }
                _ => panic!("Expected OKXWsMessage::Data variant"),
            }
        }

        #[rstest]
        fn test_parse_raw_message_bbo_tbt_channel() {
            let json = load_test_json("ws_bbo_tbt.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            match msg {
                OKXWsMessage::Data { arg, data } => {
                    assert!(
                        matches!(arg.channel, OKXWsChannel::BboTbt),
                        "Expected BboTbt channel"
                    );
                    assert!(data.is_array());
                }
                _ => panic!("Expected OKXWsMessage::Data variant"),
            }
        }

        #[rstest]
        fn test_handle_other_channel_data_tickers() {
            let mut handler = create_handler_with_instruments(vec![create_spot_instrument()]);
            let json = load_test_json("ws_tickers.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { arg, data } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(arg.channel, arg.inst_id, data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(!payloads.is_empty(), "Should produce data payloads");
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_other_channel_data_trades() {
            // Create instrument with BTC-USD symbol (matches test data)
            let instrument_id = InstrumentId::from("BTC-USD.OKX");
            let instrument = InstrumentAny::CurrencyPair(CurrencyPair::new(
                instrument_id,
                Symbol::from("BTC-USD"),
                Currency::BTC(),
                Currency::USD(),
                1,
                8,
                Price::from("0.1"),
                Quantity::from("0.00000001"),
                None, // multiplier
                None, // lot_size
                None, // max_quantity
                None, // min_quantity
                None, // max_notional
                None, // min_notional
                None, // max_price
                None, // min_price
                None, // margin_init
                None, // margin_maint
                None, // maker_fee
                None, // taker_fee
                None, // info
                UnixNanos::default(),
                UnixNanos::default(),
            ));

            let mut handler = create_handler_with_instruments(vec![instrument]);
            let json = load_test_json("ws_trades.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { arg, data } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(arg.channel, arg.inst_id, data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(!payloads.is_empty(), "Should produce trade data payloads");
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_book_data_snapshot() {
            let handler = create_handler_with_instruments(vec![create_spot_instrument()]);
            let json = load_test_json("ws_books_snapshot.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::BookData { arg, action, data } = msg else {
                panic!("Expected OKXWsMessage::BookData");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_book_data(arg, action, data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(!payloads.is_empty(), "Should produce order book payloads");
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_book_data_update() {
            let handler = create_handler_with_instruments(vec![create_spot_instrument()]);
            let json = load_test_json("ws_books_update.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::BookData { arg, action, data } = msg else {
                panic!("Expected OKXWsMessage::BookData");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_book_data(arg, action, data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(
                        !payloads.is_empty(),
                        "Should produce order book delta payloads"
                    );
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_other_channel_data_candles() {
            let mut handler = create_handler_with_instruments(vec![create_spot_instrument()]);
            let json = load_test_json("ws_candle.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { arg, data } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(arg.channel, arg.inst_id, data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(!payloads.is_empty(), "Should produce bar data payloads");
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_other_channel_data_funding_rate() {
            let mut handler = create_handler_with_instruments(vec![create_swap_instrument()]);
            let json = load_test_json("ws_funding_rate.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { arg, data } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(arg.channel, arg.inst_id, data, ts_init);

            // Funding rate returns FundingRates variant when rate changes
            assert!(result.is_none() || matches!(result, Some(NautilusWsMessage::FundingRates(_))));
        }

        #[rstest]
        fn test_handle_account_data_parses_successfully() {
            let mut handler = create_handler_with_instruments(vec![]);
            let json = load_test_json("ws_account.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { data, .. } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_account_data(data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::AccountUpdate(account_state) => {
                    assert!(
                        !account_state.balances.is_empty(),
                        "Should have balance data"
                    );
                }
                other => panic!("Expected NautilusWsMessage::AccountUpdate, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_other_channel_data_missing_instrument() {
            let mut handler = create_handler_with_instruments(vec![]);
            let json = load_test_json("ws_tickers.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { arg, data } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(arg.channel, arg.inst_id, data, ts_init);

            // Should return None when instrument is not in cache
            assert!(result.is_none());
        }

        #[rstest]
        fn test_pt_mixed_instrument_statuses() {
            let mut handler = create_handler_with_instruments(vec![]);
            let value: serde_json::Value =
                serde_json::from_str(&load_test_json("ws_instruments.json")).unwrap();
            let cases = [
                ("live", MarketStatusAction::Trading),
                ("post_only", MarketStatusAction::Quoting),
                ("suspend", MarketStatusAction::Suspend),
                ("rebase", MarketStatusAction::NotAvailableForTrading),
                ("preopen", MarketStatusAction::PreOpen),
                ("test", MarketStatusAction::NotAvailableForTrading),
            ];
            let rows: Vec<serde_json::Value> = cases
                .iter()
                .enumerate()
                .map(|(i, (wire, _))| {
                    let mut row = value["data"][0].clone();
                    row["instId"] = serde_json::json!(format!("TEST{i}-USDT-SWAP"));
                    row["state"] = serde_json::json!(wire);
                    row
                })
                .collect();
            let first = handler
                .handle_instruments_data(serde_json::json!(rows), UnixNanos::from(1_000_000_000u64))
                .expect("Mixed array must decode");
            let messages: Vec<_> = std::iter::once(first)
                .chain(handler.pending_messages.drain(..))
                .collect();
            assert_eq!(messages.len(), cases.len());
            for (i, (message, (wire, action))) in messages.into_iter().zip(cases).enumerate() {
                let status = match message {
                    NautilusWsMessage::Instrument(_, Some(status)) => status,
                    NautilusWsMessage::InstrumentStatus(status) => status,
                    other => panic!("Expected status, was {other:?}"),
                };
                assert_eq!(
                    status.instrument_id.to_string(),
                    format!("TEST{i}-USDT-SWAP.OKX")
                );
                assert_eq!(status.action, action);
                assert_eq!(status.is_trading, Some(wire == "live"));
            }
        }

        #[rstest]
        fn test_handle_instruments_data_emits_instrument_and_status() {
            let mut handler = create_handler_with_instruments(vec![create_swap_instrument()]);
            let json = load_test_json("ws_instruments.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::Data { data, .. } = msg else {
                panic!("Expected OKXWsMessage::Data");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_instruments_data(data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Instrument(inst, status) => {
                    assert_eq!(inst.id(), InstrumentId::from("BTC-USDT-SWAP.OKX"));
                    let status = status.expect("Expected InstrumentStatus");
                    assert_eq!(status.action, MarketStatusAction::Trading);
                    assert_eq!(status.is_trading, Some(true));
                }
                other => panic!("Expected NautilusWsMessage::Instrument, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_instruments_data_status_only_when_parse_fails() {
            let mut handler = create_handler_with_instruments(vec![]);

            // Valid OKXInstrument JSON but with empty tick/lot sizes to fail parsing
            let data = serde_json::json!([{
                "instType": "SWAP",
                "instId": "UNKNOWN-SWAP",
                "baseCcy": "",
                "ctMult": "1",
                "ctType": "linear",
                "ctVal": "",
                "ctValCcy": "",
                "expTime": "",
                "instFamily": "",
                "lever": "",
                "listTime": "",
                "lotSz": "",
                "minSz": "",
                "optType": "",
                "quoteCcy": "",
                "settleCcy": "",
                "state": "preopen",
                "stk": "",
                "tickSz": "",
                "uly": "",
                "ruleType": "",
                "maxLmtSz": "",
                "maxMktSz": "",
                "maxLmtAmt": "",
                "maxMktAmt": "",
                "maxTwapSz": "",
                "maxIcebergSz": "",
                "maxTriggerSz": "",
                "maxStopSz": ""
            }]);

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_instruments_data(data, ts_init);

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::InstrumentStatus(status) => {
                    assert_eq!(status.action, MarketStatusAction::PreOpen);
                    assert_eq!(status.is_trading, Some(false));
                }
                other => panic!("Expected NautilusWsMessage::InstrumentStatus, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_instruments_data_multiple_instruments() {
            let swap = create_swap_instrument();
            let spot = create_spot_instrument();
            let mut handler = create_handler_with_instruments(vec![swap, spot]);

            // Two instruments in data array
            let data = serde_json::json!([
                {
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                    "baseCcy": "",
                    "ctMult": "1",
                    "ctType": "linear",
                    "ctVal": "0.01",
                    "ctValCcy": "BTC",
                    "expTime": "",
                    "instFamily": "BTC-USDT",
                    "lever": "125",
                    "listTime": "1611916828000",
                    "lotSz": "1",
                    "minSz": "1",
                    "optType": "",
                    "quoteCcy": "",
                    "settleCcy": "USDT",
                    "state": "live",
                    "stk": "",
                    "tickSz": "0.1",
                    "uly": "BTC-USDT",
                    "ruleType": "normal",
                    "maxLmtSz": "100000000",
                    "maxMktSz": "30000",
                    "maxLmtAmt": "20000000",
                    "maxMktAmt": "",
                    "maxTwapSz": "100000000",
                    "maxIcebergSz": "100000000",
                    "maxTriggerSz": "100000000",
                    "maxStopSz": "30000"
                },
                {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "baseCcy": "BTC",
                    "ctMult": "",
                    "ctType": "",
                    "ctVal": "",
                    "ctValCcy": "",
                    "expTime": "",
                    "instFamily": "",
                    "lever": "",
                    "listTime": "1733454000000",
                    "lotSz": "0.00000001",
                    "minSz": "0.00001",
                    "optType": "",
                    "quoteCcy": "USDT",
                    "settleCcy": "",
                    "state": "live",
                    "stk": "",
                    "tickSz": "0.1",
                    "uly": "",
                    "ruleType": "normal",
                    "maxLmtSz": "9999999999",
                    "maxMktSz": "1000000",
                    "maxLmtAmt": "20000000",
                    "maxMktAmt": "1000000",
                    "maxTwapSz": "9999999999",
                    "maxIcebergSz": "9999999999",
                    "maxTriggerSz": "9999999999",
                    "maxStopSz": "1000000"
                }
            ]);

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_instruments_data(data, ts_init);

            assert!(result.is_some());
            assert_eq!(handler.pending_messages.len(), 1);
        }

        #[rstest]
        fn test_handle_index_tickers_routes_to_mapped_instrument() {
            let swap = create_swap_instrument();
            let mut handler = create_handler_with_instruments(vec![swap]);

            // Pre-populate index ticker mapping
            let mut set = AHashSet::new();
            set.insert(Ustr::from("BTC-USDT-SWAP"));
            handler.index_ticker_map.insert(Ustr::from("BTC-USDT"), set);

            let data = serde_json::json!([{
                "instId": "BTC-USDT",
                "idxPx": "103895",
                "high24h": "104985.1",
                "sodUtc0": "104812.5",
                "open24h": "103172.7",
                "low24h": "103008.2",
                "sodUtc8": "103141.2",
                "ts": "1746942707815"
            }]);

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(
                OKXWsChannel::IndexTickers,
                Some(Ustr::from("BTC-USDT")),
                data,
                ts_init,
            );

            assert!(result.is_some());
            match result.unwrap() {
                NautilusWsMessage::Data(payloads) => {
                    assert!(!payloads.is_empty(), "Should produce index price data");
                }
                other => panic!("Expected NautilusWsMessage::Data, was {other:?}"),
            }
        }

        #[rstest]
        fn test_handle_index_tickers_no_mapping_returns_none() {
            let swap = create_swap_instrument();
            let mut handler = create_handler_with_instruments(vec![swap]);

            let data = serde_json::json!([{
                "instId": "BTC-USDT",
                "idxPx": "103895",
                "high24h": "104985.1",
                "sodUtc0": "104812.5",
                "open24h": "103172.7",
                "low24h": "103008.2",
                "sodUtc8": "103141.2",
                "ts": "1746942707815"
            }]);

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(
                OKXWsChannel::IndexTickers,
                Some(Ustr::from("BTC-USDT")),
                data,
                ts_init,
            );

            assert!(result.is_none());
        }

        #[rstest]
        fn test_handle_index_tickers_multi_instrument_fanout() {
            let swap = create_swap_instrument();

            // Create a futures instrument with the same base pair
            let futures_id = InstrumentId::from("BTC-USDT-250627.OKX");
            let futures = InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
                futures_id,
                Symbol::from("BTC-USDT-250627"),
                Currency::BTC(),
                Currency::USDT(),
                Currency::USDT(),
                false,
                2,
                8,
                Price::from("0.01"),
                Quantity::from("0.00000001"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                UnixNanos::default(),
                UnixNanos::default(),
            ));

            let mut handler = create_handler_with_instruments(vec![swap, futures]);

            // Map base pair to both instruments
            let mut set = AHashSet::new();
            set.insert(Ustr::from("BTC-USDT-SWAP"));
            set.insert(Ustr::from("BTC-USDT-250627"));
            handler.index_ticker_map.insert(Ustr::from("BTC-USDT"), set);

            let data = serde_json::json!([{
                "instId": "BTC-USDT",
                "idxPx": "103895",
                "high24h": "104985.1",
                "sodUtc0": "104812.5",
                "open24h": "103172.7",
                "low24h": "103008.2",
                "sodUtc8": "103141.2",
                "ts": "1746942707815"
            }]);

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_other_channel_data(
                OKXWsChannel::IndexTickers,
                Some(Ustr::from("BTC-USDT")),
                data,
                ts_init,
            );

            assert!(result.is_some());
            assert_eq!(
                handler.pending_messages.len(),
                1,
                "Second instrument should be buffered in pending_messages"
            );
        }

        #[rstest]
        fn test_map_unmap_index_ticker_commands() {
            let mut handler = create_handler_with_instruments(vec![]);

            // Map first instrument
            handler
                .index_ticker_map
                .entry(Ustr::from("BTC-USDT"))
                .or_default()
                .insert(Ustr::from("BTC-USDT-SWAP"));
            assert!(
                handler
                    .index_ticker_map
                    .get(&Ustr::from("BTC-USDT"))
                    .unwrap()
                    .contains(&Ustr::from("BTC-USDT-SWAP"))
            );

            // Map second instrument to same base pair
            handler
                .index_ticker_map
                .entry(Ustr::from("BTC-USDT"))
                .or_default()
                .insert(Ustr::from("BTC-USDT-250627"));
            assert_eq!(
                handler
                    .index_ticker_map
                    .get(&Ustr::from("BTC-USDT"))
                    .unwrap()
                    .len(),
                2
            );

            // Unmap one
            if let Some(set) = handler.index_ticker_map.get_mut(&Ustr::from("BTC-USDT")) {
                set.remove(&Ustr::from("BTC-USDT-SWAP"));
                if set.is_empty() {
                    handler.index_ticker_map.remove(&Ustr::from("BTC-USDT"));
                }
            }
            assert_eq!(
                handler
                    .index_ticker_map
                    .get(&Ustr::from("BTC-USDT"))
                    .unwrap()
                    .len(),
                1
            );

            // Unmap last — key should be removed entirely
            if let Some(set) = handler.index_ticker_map.get_mut(&Ustr::from("BTC-USDT")) {
                set.remove(&Ustr::from("BTC-USDT-250627"));
                if set.is_empty() {
                    handler.index_ticker_map.remove(&Ustr::from("BTC-USDT"));
                }
            }
            assert!(
                !handler
                    .index_ticker_map
                    .contains_key(&Ustr::from("BTC-USDT"))
            );
        }

        #[rstest]
        fn test_handle_book_data_missing_instrument() {
            let handler = create_handler_with_instruments(vec![]);
            let json = load_test_json("ws_books_snapshot.json");
            let msg: OKXWsMessage = serde_json::from_str(&json).unwrap();

            let OKXWsMessage::BookData { arg, action, data } = msg else {
                panic!("Expected OKXWsMessage::BookData");
            };

            let ts_init = UnixNanos::from(1_000_000_000u64);
            let result = handler.handle_book_data(arg, action, data, ts_init);

            // Should return None when instrument is not in cache
            assert!(result.is_none());
        }
    }

    #[rstest]
    fn test_capture_status_survives_exhaustion_failure_reconnect_and_close() {
        use crate::websocket::{capture::read_capture, client::OKXWebSocketClient};
        let (mut handler, mut out, _, _) = create_test_handler();
        let mut client = OKXWebSocketClient::default();
        assert!(client.capture_status().is_none());
        client.capture_state = handler.capture_state.clone();
        insert_pending_amend(&mut handler, "req-status");
        let observation = handler.pending_amend_requests["req-status"].5.clone();
        handler.observations_emitted = 4095;
        handler.observations_total = 4095;
        for _ in 0..3 {
            handler.emit_request_observation(observation.clone());
        }
        assert!(matches!(
            out.try_recv(),
            Ok(NautilusWsMessage::RequestObservation(_))
        ));
        assert!(out.try_recv().is_err());
        let status = client.capture_status().unwrap();
        assert_eq!(status.state, "exhausted");
        assert_eq!(status.total_attempts, 4096);
        assert_eq!(status.observations_dropped, 2);
        let coherent = handler.capture_state.load_full().unwrap();
        for date in [1, u64::MAX, 2] {
            let dated = coherent.read(std::time::Instant::now(), date);
            assert_eq!(dated.window_attempts, 4096);
            assert_eq!(dated.window_start_ns, status.window_start_ns);
            assert_eq!(dated.ts_observed_ns, date);
            assert_eq!(handler.observations_emitted, 4096);
        }
        let generation = status.connection_generation;
        handler.observation_window_start -= std::time::Duration::from_secs(60);
        handler.observation_window_date -= 60_000_000_000;
        handler.publish_capture_state();
        let expired = client.capture_status().unwrap();
        assert_eq!(expired.window_attempts, 0);
        assert_eq!(handler.observations_emitted, 4096);
        handler.emit_request_observation(observation.clone());
        let current = client.capture_status().unwrap();
        assert_eq!(current.window_attempts, 1);
        assert_eq!(current.total_attempts, 4097);
        assert_eq!(current.observations_dropped, 2);
        assert_eq!(current.connection_generation, generation);
        drop(out);
        handler.emit_request_observation(observation.clone());
        let failed = client.capture_status().unwrap();
        assert_eq!(failed.total_attempts, 4098);
        assert_eq!(failed.observations_dropped, 3);
        handler.drain_pending_requests_on_reconnect();
        let reconnected = client.capture_status().unwrap();
        assert_eq!(reconnected.scope_id, status.scope_id);
        assert_eq!(reconnected.connection_generation, Some(1));
        assert_eq!(reconnected.window_attempts, 0);
        assert_eq!(reconnected.observations_dropped, 3);
        assert!(reconnected.status_sequence > status.status_sequence);
        let slot = handler.capture_state.clone();
        drop(handler);
        let closed = read_capture(&slot).unwrap();
        assert_eq!(closed.state, "closed");
        assert_eq!(client.capture_status().unwrap(), closed);
        assert_eq!(client.capture_status().unwrap(), closed);

        #[cfg(feature = "python")]
        {
            use pyo3::prelude::*;
            Python::initialize();
            Python::attach(|py| {
                let client = Py::new(py, client).unwrap();
                let status = client.call_method0(py, "capture_status").unwrap();
                assert_eq!(
                    status
                        .getattr(py, "observations_dropped")
                        .unwrap()
                        .extract::<u64>(py)
                        .unwrap(),
                    3
                );
                assert_eq!(
                    status
                        .getattr(py, "state")
                        .unwrap()
                        .extract::<String>(py)
                        .unwrap(),
                    "closed"
                );
                assert!(status.setattr(py, "state", "active").is_err());
                let globals = pyo3::types::PyDict::new(py);
                globals.set_item("native_client", client.bind(py)).unwrap();
                globals
                    .set_item(
                        "source",
                        include_str!("../../../../../nautilus_trader/adapters/okx/execution.py"),
                    )
                    .unwrap();
                py.run(&std::ffi::CString::new(r#"
import ast
from typing import Any
from types import SimpleNamespace
cls = next(node for node in ast.parse(source).body if isinstance(node, ast.ClassDef) and node.name == "OKXExecutionClient")
cls.bases = []
cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in {"_offer_ws_capture_status", "_handle_ws_capture_status"}]
exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])), "consumer.py", "exec"))
consumer = OKXExecutionClient()
consumer._ws_client = native_client
consumer._ws_business_client = SimpleNamespace(capture_status=lambda: None)
consumer._ws_capture_closed = {}
consumer._ws_capture_status_failures = 0
rows = []
consumer._handle_ws_capture_status = rows.append
consumer._offer_ws_capture_status()
consumer._offer_ws_capture_status()
assert len(rows) == 1
assert rows[0].observations_dropped == 3 and rows[0].state == "closed"
assert consumer._ws_capture_status_failures == 0
"#).unwrap(), Some(&globals), None).unwrap();
                assert_eq!(
                    status
                        .getattr(py, "status_sequence")
                        .unwrap()
                        .extract::<u64>(py)
                        .unwrap(),
                    closed.status_sequence
                );
            });
        }
    }

    #[rstest]
    fn test_drain_pending_requests_on_reconnect_clears_without_rejections() {
        let (mut handler, _out_rx, _active, _aliases) = create_test_handler();

        let trader_id = TraderId::new("TRADER-001");
        let strategy_id = StrategyId::new("STRATEGY-001");
        let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");
        let cancel_client_id = ClientOrderId::new("CANCEL-001");
        let amend_client_id = ClientOrderId::new("AMEND-001");

        handler.pending_cancel_requests.insert(
            "cancel-req-1".to_string(),
            (
                cancel_client_id,
                trader_id,
                strategy_id,
                instrument_id,
                Some(VenueOrderId::new("VENUE-001")),
                handler.request_observation(
                    "cancel-req-1",
                    OKXWsOperation::CancelOrder,
                    cancel_client_id,
                    instrument_id,
                    Some(VenueOrderId::new("VENUE-001")),
                    None,
                    None,
                ),
            ),
        );

        handler.pending_amend_requests.insert(
            "amend-req-1".to_string(),
            (
                amend_client_id,
                trader_id,
                strategy_id,
                instrument_id,
                Some(VenueOrderId::new("VENUE-002")),
                handler.request_observation(
                    "amend-req-1",
                    OKXWsOperation::AmendOrder,
                    amend_client_id,
                    instrument_id,
                    Some(VenueOrderId::new("VENUE-002")),
                    Some("100".into()),
                    Some("2".into()),
                ),
            ),
        );

        handler
            .pending_mass_cancel_requests
            .insert("mass-req-1".to_string(), instrument_id);

        handler.drain_pending_requests_on_reconnect();

        assert!(handler.pending_place_requests.is_empty());
        assert!(handler.pending_cancel_requests.is_empty());
        assert!(handler.pending_amend_requests.is_empty());
        assert!(handler.pending_mass_cancel_requests.is_empty());

        // No rejection events emitted, reconciliation handles ambiguous state
        assert!(handler.pending_messages.is_empty());
    }
    #[rstest]
    #[case(OKXWsOperation::AmendOrder)]
    #[case(OKXWsOperation::CancelOrder)]
    fn test_definitive_response_retires_only_correlation(#[case] op: OKXWsOperation) {
        let (mut handler, _out_rx, _active, _aliases) = create_test_handler();
        let context = (
            ClientOrderId::new("TEST-ORDER"),
            TraderId::new("TRADER-001"),
            StrategyId::new("STRATEGY-001"),
            InstrumentId::from("BTC-USDT-SWAP.OKX"),
            Some(VenueOrderId::new("12345")),
            handler.request_observation(
                "req-1",
                op.clone(),
                ClientOrderId::new("TEST-ORDER"),
                InstrumentId::from("BTC-USDT-SWAP.OKX"),
                Some(VenueOrderId::new("12345")),
                None,
                None,
            ),
        );
        if op == OKXWsOperation::AmendOrder {
            handler
                .pending_amend_requests
                .insert("req-1".into(), context);
        } else {
            handler
                .pending_cancel_requests
                .insert("req-1".into(), context);
        }
        let result = handler.handle_order_response(
            Some("req-1".into()),
            op,
            "0".into(),
            String::new(),
            vec![serde_json::json!({"sCode":"0", "clOrdId":"TEST-ORDER", "ordId":"12345"})],
            UnixNanos::from(2_000_000_000u64),
        );
        assert!(
            result.is_none(),
            "Response acknowledgement is not confirmed order state"
        );
        assert!(handler.pending_amend_requests.is_empty());
        assert!(handler.pending_cancel_requests.is_empty());
        assert!(handler.order_state_cache.is_empty());
    }

    fn insert_pending_amend(handler: &mut OKXWsFeedHandler, request_id: &str) {
        let client = ClientOrderId::new("TEST-ORDER");
        let instrument = InstrumentId::from("BTC-USDT-SWAP.OKX");
        let venue = Some(VenueOrderId::new("12345"));
        let mut observation = handler.request_observation(
            request_id,
            OKXWsOperation::AmendOrder,
            client,
            instrument,
            venue,
            Some("100".into()),
            Some("2".into()),
        );
        observation.ts_handler_ns = 1_000;
        observation.ts_send_completed_ns = Some(2_000);
        handler.pending_amend_requests.insert(
            request_id.into(),
            (
                client,
                TraderId::new("TRADER-001"),
                StrategyId::new("STRATEGY-001"),
                instrument,
                venue,
                observation,
            ),
        );
    }

    #[rstest]
    #[case(serde_json::json!([]))]
    #[case(serde_json::json!([{"clOrdId":"TEST-ORDER"}]))]
    #[case(serde_json::json!([{"clOrdId":"TEST-ORDER","sCode":0}]))]
    #[case(serde_json::json!([{"clOrdId":"TEST-ORDER","sCode":""}]))]
    #[case(serde_json::json!([{"clOrdId":"WRONG","ordId":"12345","sCode":"0"}]))]
    #[case(serde_json::json!([{"clOrdId":"TEST-ORDER","ordId":"99999","sCode":"0"}]))]
    #[case(serde_json::json!([{"sCode":"0"}]))]
    #[case(serde_json::json!([{"clOrdId":"TEST-ORDER","sCode":"0"},{"sCode":"0"}]))]
    fn test_indeterminate_response_retains_correlation(#[case] data: serde_json::Value) {
        let (mut handler, mut out, _, _) = create_test_handler();
        insert_pending_amend(&mut handler, "req-1");
        let response = handler.handle_order_response(
            Some("req-1".into()),
            OKXWsOperation::AmendOrder,
            "0".into(),
            String::new(),
            data.as_array().unwrap().clone(),
            UnixNanos::from(3000u64),
        );
        assert!(response.is_none());
        assert_eq!(handler.pending_amend_requests.len(), 1);
        for expected in ["response_received", "response_unresolved"] {
            let NautilusWsMessage::RequestObservation(observation) = out.try_recv().unwrap() else {
                panic!()
            };
            assert_eq!(observation.outcome, expected);
            assert!(observation.event_id.is_none());
        }
        assert!(out.try_recv().is_err());
    }

    #[rstest]
    fn test_response_requires_matching_operation_and_request() {
        let (mut handler, _, _, _) = create_test_handler();
        insert_pending_amend(&mut handler, "req-1");
        for (id, op) in [
            ("req-2", OKXWsOperation::AmendOrder),
            ("req-1", OKXWsOperation::CancelOrder),
        ] {
            assert!(
                handler
                    .handle_order_response(
                        Some(id.into()),
                        op,
                        "0".into(),
                        String::new(),
                        vec![serde_json::json!({"clOrdId":"TEST-ORDER","sCode":"0"})],
                        UnixNanos::from(3000u64)
                    )
                    .is_none()
            );
            assert_eq!(handler.pending_amend_requests.len(), 1);
        }
    }

    #[rstest]
    #[case("0", "51000", true)]
    #[case("1", "51000", true)]
    #[case("1", "0", false)]
    fn test_per_item_failure_and_contradictory_response(
        #[case] code: &str,
        #[case] item_code: &str,
        #[case] definitive: bool,
    ) {
        let (mut handler, mut out, _, _) = create_test_handler();
        insert_pending_amend(&mut handler, "req-1");
        let result = handler.handle_order_response(
            Some("req-1".into()),
            OKXWsOperation::AmendOrder,
            code.into(),
            String::new(),
            vec![serde_json::json!({"clOrdId":"TEST-ORDER","sCode":item_code})],
            UnixNanos::from(3000u64),
        );
        assert_eq!(
            matches!(result, Some(NautilusWsMessage::OrderModifyRejected(_))),
            definitive
        );
        assert_eq!(handler.pending_amend_requests.is_empty(), definitive);
        let NautilusWsMessage::RequestObservation(received) = out.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(received.outcome, "response_received");
        if definitive {
            let NautilusWsMessage::RequestObservation(observation) = out.try_recv().unwrap() else {
                panic!()
            };
            assert_eq!(observation.outcome, "rejected");
            assert_eq!(observation.ts_handler_ns, 1000);
            assert_eq!(observation.ts_send_completed_ns, Some(2000));
        }
    }

    #[rstest]
    fn test_duplicate_success_and_late_response_cannot_clear_new_request() {
        let (mut handler, mut out, _, _) = create_test_handler();
        insert_pending_amend(&mut handler, "req-1");
        for _ in 0..2 {
            assert!(
                handler
                    .handle_order_response(
                        Some("req-1".into()),
                        OKXWsOperation::AmendOrder,
                        "0".into(),
                        String::new(),
                        vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
                        UnixNanos::from(3000u64)
                    )
                    .is_none()
            );
        }
        for expected in [
            "response_received",
            "acknowledged",
            "response_received",
            "response_unmatched",
        ] {
            let NautilusWsMessage::RequestObservation(observation) = out.try_recv().unwrap() else {
                panic!()
            };
            assert_eq!(observation.outcome, expected);
        }
        assert!(out.try_recv().is_err());
        insert_pending_amend(&mut handler, "req-2");
        let event = handler.drain_pending_requests_on_reconnect();
        assert_eq!(event.pending.len(), 1);
        assert_eq!(event.pending[0].request_id, "req-2");
        insert_pending_amend(&mut handler, "req-3");
        handler.handle_order_response(
            Some("req-2".into()),
            OKXWsOperation::AmendOrder,
            "0".into(),
            String::new(),
            vec![serde_json::json!({"ordId":"12345","sCode":"0"})],
            UnixNanos::from(4000u64),
        );
        assert!(handler.pending_amend_requests.contains_key("req-3"));
    }

    #[rstest]
    fn test_reconnect_details_are_bounded_and_keep_original_terms_and_time() {
        let (mut handler, mut out, _, _) = create_test_handler();
        for i in 0..140 {
            insert_pending_amend(&mut handler, &format!("req-{i}"));
        }
        handler.observations_emitted = 4096;
        let observation = handler
            .pending_amend_requests
            .values()
            .next()
            .unwrap()
            .5
            .clone();
        handler.emit_request_observation(observation);
        let event = handler.drain_pending_requests_on_reconnect();
        assert_eq!(
            (
                event.pending_count,
                event.pending.len(),
                event.omitted_count
            ),
            (140, 128, 12)
        );
        assert_eq!(event.observations_dropped, 1);
        assert_eq!(event.connection_generation, 1);
        for item in event.pending {
            assert_eq!(item.ts_handler_ns, 1000);
            assert_eq!(item.ts_send_completed_ns, Some(2000));
            assert_eq!(item.connection_generation, 0);
            assert_eq!(item.requested_price.as_deref(), Some("100"));
            assert_eq!(item.requested_size.as_deref(), Some("2"));
            assert_eq!(item.outcome, "unresolved_on_reconnect");
        }
        assert!(out.try_recv().is_err(), "No fabricated lifecycle rejection");
        let empty = handler.drain_pending_requests_on_reconnect();
        assert_eq!(empty.pending_count, 0);
        assert_eq!(empty.connection_generation, 2);
        assert_eq!(empty.observations_dropped, 1);
    }
    #[tokio::test]
    async fn test_reconnect_signal_bypasses_exhausted_diagnostics_with_zero_pending() {
        let (mut handler, mut out, _, _) = create_test_handler();
        let (raw_tx, raw_rx) = tokio::sync::mpsc::unbounded_channel();
        handler.raw_rx = raw_rx;
        handler.observations_emitted = 4096;
        insert_pending_amend(&mut handler, "req-1");
        let observation = handler.pending_amend_requests.remove("req-1").unwrap().5;
        handler.emit_request_observation(observation);
        raw_tx
            .send(tokio_tungstenite::tungstenite::Message::Text(
                nautilus_network::RECONNECTED.into(),
            ))
            .unwrap();
        let signal = handler.next().await.unwrap();
        let NautilusWsMessage::Reconnected(ref event) = signal else {
            panic!()
        };
        assert_eq!(event.pending_count, 0);
        assert_eq!(event.observations_dropped, 1);
        handler.send(signal).unwrap();
        assert!(matches!(
            out.try_recv().unwrap(),
            NautilusWsMessage::Reconnected(_)
        ));
    }

    #[rstest]
    fn test_cancel_item_failure_is_definitive_without_canceled_event() {
        let (mut handler, _, _, _) = create_test_handler();
        insert_pending_amend(&mut handler, "req-1");
        let mut pending = handler.pending_amend_requests.remove("req-1").unwrap();
        pending.5.operation = OKXWsOperation::CancelOrder.to_string();
        handler
            .pending_cancel_requests
            .insert("req-1".into(), pending);
        let result = handler.handle_order_response(
            Some("req-1".into()),
            OKXWsOperation::CancelOrder,
            "0".into(),
            String::new(),
            vec![serde_json::json!({"ordId":"12345","sCode":"51400"})],
            UnixNanos::from(3000u64),
        );
        assert!(matches!(
            result,
            Some(NautilusWsMessage::OrderCancelRejected(_))
        ));
        assert!(handler.pending_cancel_requests.is_empty());
        assert!(handler.order_state_cache.is_empty());
    }
}
