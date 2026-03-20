//! WebSocket client for live event streaming from the Canon gateway.
//!
//! Connects to `WS /events`, parses `WsMessage` JSON, and routes each variant
//! to the correct signal update. Reconnects with exponential backoff
//! (2s, 4s, 8s, 16s, max 30s).

use leptos::prelude::*;
use wasm_bindgen::prelude::*;
use web_sys::{MessageEvent, WebSocket};

use crate::gateway::gateway_ws_url;
use crate::state::{
    AppState, ConnectionStatus, DataMode, DeadLetterEntry, InfraStatus, LogEntry,
    OversightReqStatus, ShipStatus, WsMessage,
};

/// Maximum number of log entries to keep in the sidebar.
const MAX_LOG_ENTRIES: usize = 60;

/// Maximum backoff delay in milliseconds.
const MAX_BACKOFF_MS: u32 = 30_000;

/// Initial backoff delay in milliseconds.
const INITIAL_BACKOFF_MS: u32 = 2_000;

/// Attempt to connect to the gateway WebSocket.
/// Falls back silently when the gateway is unavailable (demo mode).
pub fn connect_ws(state: AppState) {
    connect_ws_with_backoff(state, INITIAL_BACKOFF_MS);
}

fn connect_ws_with_backoff(state: AppState, backoff_ms: u32) {
    let url = gateway_ws_url();

    let ws = match WebSocket::new(&url) {
        Ok(ws) => ws,
        Err(_) => {
            // Gateway not available -- stay in demo mode, retry later.
            state.connection.set(ConnectionStatus::Disconnected);
            schedule_reconnect(state, backoff_ms);
            return;
        }
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    state.connection.set(ConnectionStatus::Reconnecting);

    // -- on open: mark connected, switch to live mode, reset backoff --
    let state_open = state;
    let onopen = Closure::<dyn FnMut()>::new(move || {
        state_open.connection.set(ConnectionStatus::Connected);
        state_open.data_mode.set(DataMode::Live);
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    // -- on message: parse WsMessage and dispatch --
    let state_msg = state;
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |evt: MessageEvent| {
        if let Some(text) = evt.data().as_string() {
            handle_ws_message(&text, state_msg);
        }
    });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    // -- on close: schedule reconnect with exponential backoff --
    let state_close = state;
    let onclose = Closure::<dyn FnMut()>::new(move || {
        state_close.connection.set(ConnectionStatus::Reconnecting);
        // Double the backoff, capped at MAX_BACKOFF_MS
        let next_backoff = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        schedule_reconnect(state_close, next_backoff);
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();

    // -- on error: let onclose handle reconnection --
    let onerror = Closure::<dyn FnMut()>::new(move || {});
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();
}

fn schedule_reconnect(state: AppState, backoff_ms: u32) {
    let _ = gloo_timers::callback::Timeout::new(backoff_ms, move || {
        connect_ws_with_backoff(state, backoff_ms);
    });
}

fn handle_ws_message(text: &str, state: AppState) {
    let msg: WsMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => return,
    };

    match msg {
        WsMessage::Event(live_event) => {
            let entry = LogEntry {
                id: uuid::Uuid::new_v4(),
                timestamp: live_event.timestamp.clone(),
                version: live_event.version,
                service: live_event.service.clone(),
                event_name: live_event.event_type.clone(),
                aggregate_id: uuid::Uuid::parse_str(&live_event.aggregate_id)
                    .unwrap_or_else(|_| uuid::Uuid::new_v4()),
                correlation_id: uuid::Uuid::parse_str(&live_event.correlation_id)
                    .unwrap_or_else(|_| uuid::Uuid::new_v4()),
                is_new: true,
            };

            state.log_entries.update(|entries| {
                // Mark previous newest as not new
                if let Some(first) = entries.first_mut() {
                    first.is_new = false;
                }
                entries.insert(0, entry);
                entries.truncate(MAX_LOG_ENTRIES);
            });

            // Also update oversight if relevant event types come through
            update_oversight_from_event(state, &live_event.event_type);
        }

        WsMessage::ShipUpdate(ship_update) => {
            let ship_id = match uuid::Uuid::parse_str(&ship_update.id) {
                Ok(id) => id,
                Err(_) => return,
            };
            let new_status = match ship_update.status.as_str() {
                "docked" | "Docked" => ShipStatus::Docked,
                "transit" | "Transit" => ShipStatus::Transit,
                "dead" | "Dead" => ShipStatus::Dead,
                _ => return,
            };

            state.ships.update(|ships| {
                if let Some(ship) = ships.iter_mut().find(|s| s.id == ship_id) {
                    ship.status = new_status;
                    ship.fuel_pct = ship_update.fuel_pct;
                    ship.version = ship_update.version;
                }
            });
        }

        WsMessage::StationUpdate(station_update) => {
            let station_id = match uuid::Uuid::parse_str(&station_update.id) {
                Ok(id) => id,
                Err(_) => return,
            };

            state.stations.update(|stations| {
                if let Some(station) = stations.iter_mut().find(|s| s.id == station_id) {
                    station.stock_low = station_update.stock_low;
                }
            });
        }

        WsMessage::OversightUpdate(oversight_update) => {
            state.oversight.update(|o| {
                o.visible = true;
                o.handler_id = oversight_update.handler_id.clone();
                match oversight_update.status.as_str() {
                    "ready" | "Ready" => {
                        o.arrival_status = OversightReqStatus::Met;
                        o.manifest_status = OversightReqStatus::Met;
                    }
                    "discarded" | "Discarded" => {
                        o.visible = false;
                    }
                    _ => {
                        // "pending" or "not_ready" -- keep existing requirement states
                    }
                }
            });
        }

        WsMessage::DeadLetter(dl) => {
            let entry = DeadLetterEntry {
                id: uuid::Uuid::parse_str(&dl.id).unwrap_or_else(|_| uuid::Uuid::new_v4()),
                event_type: dl.event_name.clone(),
                service: String::new(),
                aggregate_id: String::new(),
                error: dl.error.clone(),
                attempts: 3,
                requeued: false,
                created_at: String::new(),
            };
            state.dead_letters.update(|entries| {
                entries.push(entry);
            });
        }

        WsMessage::InfraStatus(infra) => {
            state.infra.set(InfraStatus {
                kafka: infra.kafka,
                yugabyte: infra.yugabyte,
                cassandra: infra.cassandra,
            });
        }
    }
}

/// Update the oversight strip when we receive relevant event types from
/// the WebSocket event stream.
fn update_oversight_from_event(state: AppState, event_type: &str) {
    match event_type {
        "ShipArrivedAtStation" => {
            state.oversight.update(|o| {
                o.arrival_status = OversightReqStatus::Met;
            });
        }
        "ManifestCreated" => {
            state.oversight.update(|o| {
                o.manifest_status = OversightReqStatus::Met;
            });
        }
        "UnloadingStarted" => {
            let state_hide = state;
            let _ = gloo_timers::callback::Timeout::new(1000, move || {
                state_hide.oversight.update(|o| {
                    o.visible = false;
                });
            });
        }
        _ => {}
    }
}
