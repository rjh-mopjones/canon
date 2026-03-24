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
    AppState, CargoLoad, ConnectionStatus, DeadLetterEntry, InfraStatus, LiveEvent, LogEntry,
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

/// Minimum flight animation time in ms. Arrival events that arrive before
/// this threshold are queued and applied after the animation completes.
const MIN_FLIGHT_DURATION_MS: f64 = 3000.0;

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

    // Store the WS reference so restart_game can send RegisterSession later.
    state.ws.set(Some(ws.clone()));

    // Track whether the connection was successfully opened so we can
    // reset backoff on close (single-threaded WASM -- Rc<Cell> is fine).
    let was_opened = Rc::new(Cell::new(false));

    // Clone WS for use inside onopen closure.
    let ws_for_session = ws.clone();

    // -- on open: mark connected, create session --
    let state_open = state;
    let was_opened_open = Rc::clone(&was_opened);
    let onopen = Closure::<dyn FnMut()>::new(move || {
        was_opened_open.set(true);
        state_open.connection.set(ConnectionStatus::Connected);
        // Create session via POST /sessions, then send RegisterSession over WS
        create_session_and_register(state_open, ws_for_session.clone());
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    // -- on message: parse WsMessage and dispatch --
    let state_msg = state;
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |evt: MessageEvent| {
        let data = evt.data();
        if let Some(text) = data.as_string() {
            handle_ws_message(&text, state_msg);
        } else {
            web_sys::console::warn_1(
                &format!(
                    "WS: non-string message type={:?}",
                    data.js_typeof().as_string()
                )
                .into(),
            );
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
    wasm_bindgen_futures::spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(backoff_ms).await;
        connect_ws_with_backoff(state, backoff_ms);
    });
}

/// Create a new session via `POST /sessions`, store the session_id in
/// `AppState`, send a `RegisterSession` message over the WebSocket so the
/// server filters events for this session, then hydrate initial state.
pub fn create_session_and_register(state: AppState, ws: WebSocket) {
    let base = crate::gateway::gateway_base_url();
    wasm_bindgen_futures::spawn_local(async move {
        // POST /sessions
        let url = format!("{base}/sessions");
        let resp = match gloo_net::http::Request::post(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to create session: {e}").into());
                return;
            }
        };

        if !resp.ok() {
            web_sys::console::warn_1(&format!("POST /sessions failed: {}", resp.status()).into());
            return;
        }

        #[derive(serde::Deserialize)]
        struct SessionResponse {
            session_id: uuid::Uuid,
            #[allow(dead_code)]
            ship_id: uuid::Uuid,
            #[allow(dead_code)]
            stations: Vec<SessionStation>,
        }
        #[derive(serde::Deserialize)]
        struct SessionStation {
            #[allow(dead_code)]
            id: uuid::Uuid,
            #[allow(dead_code)]
            name: String,
            #[allow(dead_code)]
            capacity_kg: f32,
            #[allow(dead_code)]
            initial_stock_pct: f64,
        }

        let session: SessionResponse = match resp.json().await {
            Ok(s) => s,
            Err(e) => {
                web_sys::console::warn_1(&format!("Failed to parse session response: {e}").into());
                return;
            }
        };

        // Store session_id
        state.session_id.set(Some(session.session_id));

        // Send RegisterSession message over WS
        let reg_msg = serde_json::json!({
            "type": "RegisterSession",
            "session_id": session.session_id.to_string()
        });
        if let Ok(json) = serde_json::to_string(&reg_msg) {
            let _ = ws.send_with_str(&json);
        }

        // Poll until the pipeline has processed the bootstrap commands.
        // We check GET /stations?session_id=xxx until 4 stations appear,
        // rather than using a fixed sleep. This adapts to pipeline load.
        let sid_str = session.session_id.to_string();
        let stations_url = format!("{base}/stations?session_id={sid_str}");
        for attempt in 0..30 {
            gloo_timers::future::TimeoutFuture::new(1_000).await;
            if let Ok(resp) = gloo_net::http::Request::get(&stations_url).send().await {
                if let Ok(stations) = resp.json::<Vec<serde_json::Value>>().await {
                    if stations.len() >= 4 {
                        break;
                    }
                }
            }
            if attempt == 29 {
                web_sys::console::warn_1(&"session bootstrap: stations not ready after 30s".into());
            }
        }

        // Hydrate with session-specific data
        crate::hydrate::hydrate_from_gateway(state, session.session_id);
    });
}

fn handle_ws_message(text: &str, state: AppState) {
    let msg: WsMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            web_sys::console::warn_1(
                &format!(
                    "WS parse error: {e} | raw: {}",
                    &text[..text.len().min(200)]
                )
                .into(),
            );
            return;
        }
    };

    match msg {
        WsMessage::Event(live_event) => {
            // Server-side WS filtering ensures only events for our session
            // arrive here, so no client-side aggregate-ID check is needed.

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
            handle_game_event(state, &live_event);

            // Update oversight from real events
            update_oversight_from_event(state, &live_event);
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
                    let was_not_transit = ship.status != ShipStatus::Transit;
                    ship.status = new_status;
                    ship.fuel_pct = ship_update.fuel_pct;
                    ship.version = ship_update.version;
                    ship.events_since_snapshot = (ship_update.version as u32) % ship.snapshot_every;

                    // If the ship just transitioned to Transit via a ShipUpdate
                    // (e.g. the ShipDeparted Event hasn't arrived yet) and
                    // animation fields are not set, initialise them so the ship
                    // starts moving on the canvas immediately.
                    if new_status == ShipStatus::Transit
                        && was_not_transit
                        && ship.flight_start_ms.is_none()
                    {
                        let now_ms = web_sys::window()
                            .and_then(|w| w.performance())
                            .map(|p| p.now())
                            .unwrap_or(0.0);
                        ship.from_pct_x = Some(ship.left_pct);
                        ship.from_pct_y = Some(ship.top_pct);
                        ship.flight_start_ms = Some(now_ms);
                        ship.flight_duration_ms = Some(TRANSIT_DURATION_MS);
                        ship.current_station_idx = None;
                        ship.canvas_x = None;
                        ship.canvas_y = None;
                    }
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
fn handle_game_event(state: AppState, live_event: &LiveEvent) {
    let event_type = live_event.event_type.as_str();
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
                    // Guard on flight_start_ms instead of status: a ShipUpdate
                    // message may have already set status=Transit before this
                    // Event arrives, but without the animation fields the ship
                    // would appear stuck. We only skip if animation is already
                    // running (flight_start_ms is Some).
                    if ship.flight_start_ms.is_none() {
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

        "ShipArrivedAtStation" | "ShipDocked" | "ShipDockedAtStation" => {
            // Ship has arrived -- dock it at the destination station.
            // Parse the station_id from the event payload to know WHERE the ship docked,
            // rather than relying on destination_station_idx which may not be set
            // (e.g. on page reload or when events replay from offset 0).
            let station_id_from_event = live_event
                .payload
                .as_ref()
                .and_then(|p| p.get("station_id"))
                .and_then(|v| v.as_str())
                .and_then(|s| uuid::Uuid::parse_str(s).ok());

            let station_idx_from_event = station_id_from_event.and_then(|sid| {
                state
                    .stations
                    .with_untracked(|stations| stations.iter().position(|st| st.id == sid))
            });

            // If the flight animation hasn't completed its minimum duration yet,
            // defer the docking to avoid teleporting the ship.
            let now_ms = web_sys::window()
                .and_then(|w| w.performance())
                .map(|p| p.now())
                .unwrap_or(0.0);

            let should_defer = state.ships.with_untracked(|ships| {
                ships.first().is_some_and(|ship| {
                    if let Some(start) = ship.flight_start_ms {
                        let elapsed = now_ms - start;
                        elapsed < MIN_FLIGHT_DURATION_MS
                    } else {
                        false
                    }
                })
            });

            if should_defer {
                let remaining_ms = state.ships.with_untracked(|ships| {
                    ships.first().map_or(0.0, |ship| {
                        ship.flight_start_ms
                            .map(|start| (MIN_FLIGHT_DURATION_MS - (now_ms - start)).max(100.0))
                            .unwrap_or(100.0)
                    })
                });
                let state_deferred = state;
                // Use gloo_timers future instead of callback::Timeout because
                // Timeout cancels on drop and `let _ = ...` drops immediately.
                wasm_bindgen_futures::spawn_local(async move {
                    gloo_timers::future::TimeoutFuture::new(remaining_ms as u32).await;
                    dock_ship(state_deferred, station_idx_from_event);
                });
            } else {
                dock_ship(state, station_idx_from_event);
            }
        }

        "CargoLoaded" => {
            // Cargo has been loaded -- update local cargo state from the pipeline event.
            // Use the manifest_id from the most recent ManifestCreated event, or from
            // the user's manual load_cargo flow.
            let manifest_id = state.last_manifest_id.get_untracked().or_else(|| {
                state
                    .cargo
                    .with_untracked(|c| c.as_ref().and_then(|c| c.manifest_id))
            });
            let current_idx = state
                .ships
                .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));
            if let Some(idx) = current_idx {
                if let Some(dest_idx) = crate::state::supply_destination(idx) {
                    state.cargo.set(Some(CargoLoad {
                        destination_idx: dest_idx,
                        amount_pct: REPLENISH_AMOUNT as u32,
                        manifest_id,
                    }));
                }
            }
            state.pending_command.set(PendingCommand::None);
        }

        "StockDrained" => {
            // Stock drain event from the pipeline -- update station stock levels.
            // The payload carries station_id, drain_kg, and remaining_kg.
            if let Some(payload) = &live_event.payload {
                let station_id = payload["station_id"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok());
                let remaining_kg = payload["remaining_kg"].as_f64();

                if let (Some(sid), Some(remaining)) = (station_id, remaining_kg) {
                    let mut any_depleted = false;

                    state.stations.update(|stations| {
                        if let Some(station) = stations.iter_mut().find(|s| s.id == sid) {
                            // Convert remaining_kg to percentage of capacity.
                            let capacity = if station.capacity_kg > 0.0 {
                                station.capacity_kg
                            } else {
                                5000.0 // fallback for stations hydrated before capacity was known
                            };
                            station.stock_pct = (remaining / capacity * 100.0).max(0.0);
                            station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;

                            if station.stock_pct <= 0.0 {
                                any_depleted = true;
                            }
                        }
                    });

                    if any_depleted && !state.game_over.get_untracked() {
                        state.game_over.set(true);
                    }
                }
            }
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

/// Apply the docking state change: snap ship to destination and mark Docked.
/// Dock the ship at a station. Tries `event_station_idx` first (from the WS
/// event payload), then falls back to `destination_station_idx` (set when the
/// user clicked a planet). This ensures docking works even on page reload or
/// when events replay from offset 0.
fn dock_ship(state: AppState, event_station_idx: Option<usize>) {
    state.ships.update(|ships| {
        if let Some(ship) = ships.first_mut() {
            let dest_idx = event_station_idx.or(ship.destination_station_idx);

            if let Some(idx) = dest_idx {
                let (dest_left, dest_top) = state.stations.with_untracked(|stations| {
                    stations
                        .get(idx)
                        .map(|s| (s.left_pct, s.top_pct))
                        .unwrap_or((50.0, 50.0))
                });
                ship.status = ShipStatus::Docked;
                ship.current_station_idx = Some(idx);
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

/// Update the oversight strip when we receive relevant event types from
/// the WebSocket event stream.
fn update_oversight_from_event(state: AppState, event: &LiveEvent) {
    match event.event_type.as_str() {
        "ShipArrivedAtStation" => {
            state.oversight.update(|o| {
                o.arrival_status = OversightReqStatus::Met;
            });
        }
        "ManifestCreated" => {
            state.oversight.update(|o| {
                o.manifest_status = OversightReqStatus::Met;
            });
            // Store manifest_id so CargoLoaded handler can pick it up.
            if let Ok(mid) = event.aggregate_id.parse::<uuid::Uuid>() {
                state.last_manifest_id.set(Some(mid));
            }
        }
        "UnloadingStarted" => {
            let state_hide = state;
            wasm_bindgen_futures::spawn_local(async move {
                gloo_timers::future::TimeoutFuture::new(1000).await;
                state_hide.oversight.update(|o| {
                    o.visible = false;
                });
            });
        }
        _ => {}
    }
}
