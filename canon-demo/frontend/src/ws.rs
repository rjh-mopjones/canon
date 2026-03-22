//! WebSocket client for live event streaming from the Canon gateway.
//!
//! Connects to `WS /events`, parses `WsMessage` JSON, and routes each variant
//! to the correct signal update. All game state changes (ship position, station
//! stock, oversight gates, event log entries) are driven by real events arriving
//! over this WebSocket -- never by local simulation or fake timers.
//!
//! Reconnects with exponential backoff (2s, 4s, 8s, 16s, max 30s).

use std::cell::Cell;
use std::rc::Rc;

use leptos::prelude::*;
use wasm_bindgen::prelude::*;
use web_sys::{MessageEvent, WebSocket};

use crate::gateway::gateway_ws_url;
use crate::state::{
    AppState, CargoLoad, ConnectionStatus, DeadLetterEntry, InfraStatus, LogEntry,
    OversightReqStatus, OversightState, PendingCommand, ShipStatus, WsMessage, REPLENISH_AMOUNT,
    STOCK_LOW_THRESHOLD,
};

/// Maximum number of log entries to keep in the event log strip.
const MAX_LOG_ENTRIES: usize = 60;

/// Maximum backoff delay in milliseconds.
const MAX_BACKOFF_MS: u32 = 30_000;

/// Initial backoff delay in milliseconds.
const INITIAL_BACKOFF_MS: u32 = 2_000;

/// Transit duration in ms -- canvas animation for ship movement.
const TRANSIT_DURATION_MS: f64 = 4200.0;

/// Attempt to connect to the gateway WebSocket.
/// Shows connection error when the gateway is unavailable.
pub fn connect_ws(state: AppState) {
    connect_ws_with_backoff(state, INITIAL_BACKOFF_MS);
}

fn connect_ws_with_backoff(state: AppState, backoff_ms: u32) {
    let url = gateway_ws_url();

    let ws = match WebSocket::new(&url) {
        Ok(ws) => ws,
        Err(_) => {
            // Gateway not available -- show disconnected state, retry later.
            state.connection.set(ConnectionStatus::Disconnected);
            schedule_reconnect(state, backoff_ms);
            return;
        }
    };

    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    state.connection.set(ConnectionStatus::Reconnecting);

    // Track whether the connection was successfully opened so we can
    // reset backoff on close (single-threaded WASM -- Rc<Cell> is fine).
    let was_opened = Rc::new(Cell::new(false));

    // -- on open: mark connected --
    let state_open = state;
    let was_opened_open = Rc::clone(&was_opened);
    let onopen = Closure::<dyn FnMut()>::new(move || {
        was_opened_open.set(true);
        state_open.connection.set(ConnectionStatus::Connected);
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
    let was_opened_close = Rc::clone(&was_opened);
    let onclose = Closure::<dyn FnMut()>::new(move || {
        state_close.connection.set(ConnectionStatus::Reconnecting);
        let next_backoff = if was_opened_close.get() {
            INITIAL_BACKOFF_MS
        } else {
            (backoff_ms * 2).min(MAX_BACKOFF_MS)
        };
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

            // Drive game state from real events
            handle_game_event(state, &live_event.event_type);

            // Update oversight from real events
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
                    ship.events_since_snapshot = (ship_update.version as u32) % ship.snapshot_every;
                }
            });

            // Clear pending command when ship status changes confirm the action
            state.pending_command.set(PendingCommand::None);
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

/// Drive game state from real pipeline events arriving via WebSocket.
///
/// Ship position, cargo state, and station stock are updated only when
/// real events confirm the action through the Canon pipeline.
fn handle_game_event(state: AppState, event_type: &str) {
    match event_type {
        "ShipDeparted" => {
            // The pipeline confirmed the departure -- start the ship transit animation.
            // The ship's destination was set when the POST was sent (pending state).
            // Now we know the pipeline accepted it, so start animating.
            let now_ms = web_sys::window()
                .and_then(|w| w.performance())
                .map(|p| p.now())
                .unwrap_or(0.0);

            state.ships.update(|ships| {
                if let Some(ship) = ships.first_mut() {
                    if ship.status != ShipStatus::Transit {
                        ship.from_pct_x = Some(ship.left_pct);
                        ship.from_pct_y = Some(ship.top_pct);
                        ship.flight_start_ms = Some(now_ms);
                        ship.flight_duration_ms = Some(TRANSIT_DURATION_MS);
                        ship.status = ShipStatus::Transit;
                        ship.current_station_idx = None;
                        ship.canvas_x = None;
                        ship.canvas_y = None;
                    }
                }
            });

            // Clear pending state
            state.pending_command.set(PendingCommand::None);

            // Show oversight strip for this voyage
            let dest_name = state.ships.with_untracked(|ships| {
                ships.first().and_then(|s| {
                    s.destination_station_idx.and_then(|di| {
                        state
                            .stations
                            .with_untracked(|stations| stations.get(di).map(|st| st.name.clone()))
                    })
                })
            });

            if let Some(name) = dest_name {
                state.oversight.set(OversightState {
                    visible: true,
                    handler_id: "unloading-handler".to_string(),
                    gate_title: format!("Cargo unloading \u{2014} VSS MERIDIAN \u{2192} {}", name),
                    arrival_status: OversightReqStatus::Pending,
                    manifest_status: OversightReqStatus::Pending,
                });
            }
        }

        "ShipArrivedAtStation" | "ShipDocked" => {
            // Ship has arrived -- dock it at the destination station.
            state.ships.update(|ships| {
                if let Some(ship) = ships.first_mut() {
                    if let Some(dest_idx) = ship.destination_station_idx {
                        let (dest_left, dest_top) = state.stations.with_untracked(|stations| {
                            stations
                                .get(dest_idx)
                                .map(|s| (s.left_pct, s.top_pct))
                                .unwrap_or((50.0, 50.0))
                        });
                        ship.status = ShipStatus::Docked;
                        ship.current_station_idx = Some(dest_idx);
                        ship.destination_station_idx = None;
                        ship.left_pct = dest_left;
                        ship.top_pct = dest_top;
                        ship.flight_start_ms = None;
                        ship.flight_duration_ms = None;
                        ship.from_pct_x = None;
                        ship.from_pct_y = None;
                        ship.canvas_x = None;
                        ship.canvas_y = None;
                    }
                }
            });
        }

        "CargoLoaded" => {
            // Cargo has been loaded -- update local cargo state from the pipeline event.
            let current_idx = state
                .ships
                .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));
            if let Some(idx) = current_idx {
                if let Some(dest_idx) = crate::state::supply_destination(idx) {
                    state.cargo.set(Some(CargoLoad {
                        destination_idx: dest_idx,
                        amount_pct: REPLENISH_AMOUNT as u32,
                        manifest_id: None,
                    }));
                }
            }
            state.pending_command.set(PendingCommand::None);
        }

        "CargoUnloaded" | "CargoReceived" => {
            // Delivery confirmed by the pipeline -- replenish station stock.
            if let Some(cargo) = state.cargo.get_untracked() {
                state.stations.update(|stations| {
                    if let Some(station) = stations.get_mut(cargo.destination_idx) {
                        station.stock_pct = (station.stock_pct + REPLENISH_AMOUNT).min(100.0);
                        station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;
                    }
                });
                state.cargo.set(None);
            }
            state.pending_command.set(PendingCommand::None);
        }

        _ => {}
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
