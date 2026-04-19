use std::cell::Cell;
use std::rc::Rc;

use leptos::prelude::*;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

use crate::canvas_map;
use crate::gateway::gateway_base_url;
use crate::state::{
    begin_pending_command, clear_pending_command, clear_pending_command_after_min_feedback,
    supply_destination, AppState, CommandError, ConnectionStatus, LogEntry, OversightReqStatus,
    OversightState, PendingCommand, ShipStatus, STARTING_STOCK, STOCK_LOW_THRESHOLD,
};

fn set_command_error(state: AppState, message: impl Into<String>) {
    clear_pending_command(state);
    state.command_error.set(Some(CommandError {
        message: message.into(),
    }));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract just `HH:MM:SS.mmm` from an ISO timestamp string.
fn format_time(ts: &str) -> String {
    // Input like "2026-03-24T23:04:983797384+00:00" — grab the time part after 'T'
    if let Some(t_pos) = ts.find('T') {
        let after_t = &ts[t_pos + 1..];
        // Take up to the timezone offset (+/- or Z)
        let time_part = after_t.split(['+', 'Z']).next().unwrap_or(after_t);
        // Truncate to HH:MM:SS.mmm (12 chars)
        if time_part.len() > 12 {
            return time_part[..12].to_string();
        }
        return time_part.to_string();
    }
    ts.to_string()
}

// ---------------------------------------------------------------------------
// Supply chain game logic
// ---------------------------------------------------------------------------

/// Reset the game: create a new session (fresh ship + stations), reset
/// client-side signals, and re-register on the existing WebSocket so the
/// server starts filtering events for the new session.
fn restart_game(state: AppState) {
    state.game_over.set(false);
    state.cargo.set(None);
    state.command_error.set(None);
    clear_pending_command(state);
    state.ship_canvas_pos.set(None);

    state.stations.update(|stations| {
        for (i, station) in stations.iter_mut().enumerate() {
            if let Some(starting) = STARTING_STOCK.get(i) {
                station.stock_pct = *starting;
                station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;
            }
        }
    });

    // Clear the current ship so the loading overlay stays visible until the
    // fresh session has fully hydrated through the pipeline again.
    state.ships.set(Vec::new());

    // Clear log and oversight
    state.log_entries.update(|entries| entries.clear());
    state.oversight.set(OversightState {
        visible: false,
        handler_id: String::new(),
        gate_title: String::new(),
        arrival_status: OversightReqStatus::Pending,
        manifest_status: OversightReqStatus::Pending,
    });

    // Create a new session — the poll loop picks up the new session_id.
    crate::polling::create_new_session(state);
}

// ---------------------------------------------------------------------------
// Gateway command posting
// ---------------------------------------------------------------------------

/// Post a DepartForStation command to the gateway.
/// Sets pending state while waiting, handles errors on failure.
/// The ship only moves when a real ShipDeparted event arrives via WebSocket.
fn depart_ship(state: AppState, ship_idx: usize, dest_idx: usize) {
    // Block departures during game over or if already pending
    if state.game_over.get_untracked() {
        set_command_error(state, "Cannot depart: supply chain collapsed");
        return;
    }
    if state.pending_command.get_untracked() != PendingCommand::None {
        set_command_error(
            state,
            "Command already in progress — waiting for pipeline...",
        );
        return;
    }

    // Block if not connected
    if state.connection.get_untracked() != ConnectionStatus::Connected {
        state.command_error.set(Some(CommandError {
            message: "Cannot depart: gateway not connected".to_string(),
        }));
        return;
    }

    let ship_id = state
        .ships
        .with_untracked(|ships| ships.get(ship_idx).map(|s| s.id));
    let station_id = state
        .stations
        .with_untracked(|stations| stations.get(dest_idx).map(|s| s.id));

    let (ship_id, station_id) = match (ship_id, station_id) {
        (Some(s), Some(d)) => (s, d),
        _ => {
            set_command_error(state, "Fleet is still loading — try again in a moment");
            return;
        }
    };

    // Set destination on ship so the WS handler knows where to animate to
    state.ships.update(|ships| {
        if let Some(ship) = ships.get_mut(ship_idx) {
            ship.destination_station_idx = Some(dest_idx);
        }
    });

    // Set pending state and clear previous errors
    begin_pending_command(state, PendingCommand::Departing);

    let base = gateway_base_url();
    spawn_local(async move {
        #[derive(serde::Serialize)]
        struct DepartBody {
            voyage_id: Uuid,
            destination: Uuid,
        }
        let body = DepartBody {
            voyage_id: Uuid::new_v4(),
            destination: station_id,
        };
        let url = format!("{base}/fleet/ships/{ship_id}/depart");
        let body_json = match serde_json::to_string(&body) {
            Ok(j) => j,
            Err(_) => {
                set_command_error(state, "Failed to serialize departure command");
                // Clear destination since command failed
                state.ships.update(|ships| {
                    if let Some(ship) = ships.get_mut(ship_idx) {
                        ship.destination_station_idx = None;
                    }
                });
                return;
            }
        };

        let result = gloo_net::http::Request::post(&url)
            .header("Content-Type", "application/json")
            .body(body_json);

        match result {
            Ok(req) => match req.send().await {
                Ok(resp) => {
                    if !resp.ok() {
                        let status = resp.status();
                        let body_text = resp.text().await.unwrap_or_default();
                        set_command_error(
                            state,
                            format!(
                                "Departure rejected ({}): {}",
                                status,
                                if body_text.is_empty() {
                                    "unknown error"
                                } else {
                                    &body_text
                                }
                            ),
                        );
                        // Clear destination since command failed
                        state.ships.update(|ships| {
                            if let Some(ship) = ships.get_mut(ship_idx) {
                                ship.destination_station_idx = None;
                            }
                        });
                    }
                    // On success: pending state stays until WebSocket delivers
                    // the ShipDeparted event which triggers the animation.
                }
                Err(e) => {
                    set_command_error(state, format!("Failed to send departure command: {e}"));
                    // Clear destination since command failed
                    state.ships.update(|ships| {
                        if let Some(ship) = ships.get_mut(ship_idx) {
                            ship.destination_station_idx = None;
                        }
                    });
                }
            },
            Err(_) => {
                set_command_error(state, "Failed to build departure request");
                // Clear destination since command failed
                state.ships.update(|ships| {
                    if let Some(ship) = ships.get_mut(ship_idx) {
                        ship.destination_station_idx = None;
                    }
                });
            }
        }
    });
}

/// Post a LoadCargo command to the gateway.
fn load_cargo(state: AppState) {
    if state.pending_command.get_untracked() != PendingCommand::None {
        set_command_error(
            state,
            "Command already in progress — waiting for pipeline...",
        );
        return;
    }
    if state.connection.get_untracked() != ConnectionStatus::Connected {
        state.command_error.set(Some(CommandError {
            message: "Cannot load cargo: gateway not connected".to_string(),
        }));
        return;
    }

    let current_station_idx = state
        .ships
        .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));

    let idx = match current_station_idx {
        Some(i) => i,
        None => {
            set_command_error(state, "Cannot load supplies: ship is not docked yet");
            return;
        }
    };

    let dest_idx = match supply_destination(idx) {
        Some(d) => d,
        None => {
            set_command_error(
                state,
                "Cannot load supplies: no supply destination for this station",
            );
            return;
        }
    };

    // Already carrying cargo? No-op.
    if state.cargo.get_untracked().is_some() {
        set_command_error(
            state,
            "Supplies already loaded — fly to the destination to deliver",
        );
        return;
    }

    let ship_id = state
        .ships
        .with_untracked(|ships| ships.first().map(|s| s.id));
    let ship_id = match ship_id {
        Some(id) => id,
        None => {
            set_command_error(state, "Fleet is still loading — try again in a moment");
            return;
        }
    };

    begin_pending_command(state, PendingCommand::Loading);

    let base = gateway_base_url();
    spawn_local(async move {
        // Step 1: Create manifest (gateway expects ship_id + voyage_id)
        let manifest_url = format!("{base}/cargo/manifests");
        let voyage_id = Uuid::new_v4();
        #[derive(serde::Serialize)]
        struct ManifestBody {
            ship_id: Uuid,
            voyage_id: Uuid,
        }
        let manifest_json = match serde_json::to_string(&ManifestBody { ship_id, voyage_id }) {
            Ok(j) => j,
            Err(_) => {
                set_command_error(state, "Failed to serialize manifest command");
                return;
            }
        };

        let manifest_resp = match gloo_net::http::Request::post(&manifest_url)
            .header("Content-Type", "application/json")
            .body(manifest_json)
        {
            Ok(req) => match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    set_command_error(state, format!("Failed to create manifest: {e}"));
                    return;
                }
            },
            Err(_) => {
                set_command_error(state, "Failed to build manifest request");
                return;
            }
        };

        if !manifest_resp.ok() {
            set_command_error(
                state,
                format!("Create manifest rejected ({})", manifest_resp.status()),
            );
            return;
        }

        // Extract manifest_id for subsequent LoadCargo call
        let manifest_id: Option<Uuid> = manifest_resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v["aggregate_id"].as_str().and_then(|s| s.parse().ok()));

        let manifest_id = match manifest_id {
            Some(id) => id,
            None => {
                set_command_error(state, "Failed to parse manifest ID");
                return;
            }
        };

        // Store manifest_id for delivery later
        state.cargo.update(|c| {
            if let Some(ref mut cargo) = c {
                cargo.manifest_id = Some(manifest_id);
            }
        });

        // Step 2: Load cargo into the manifest
        let load_url = format!("{base}/cargo/manifests/{manifest_id}/load");
        #[derive(serde::Serialize)]
        struct LoadBody {
            item_id: Uuid,
            weight_kg: f32,
            description: String,
        }
        let load_json = match serde_json::to_string(&LoadBody {
            item_id: Uuid::new_v4(),
            weight_kg: 1000.0,
            description: "Supply crates".to_string(),
        }) {
            Ok(j) => j,
            Err(_) => {
                set_command_error(state, "Failed to serialize load cargo request");
                return;
            }
        };

        match gloo_net::http::Request::post(&load_url)
            .header("Content-Type", "application/json")
            .body(load_json)
        {
            Ok(req) => match req.send().await {
                Ok(resp) if !resp.ok() => {
                    set_command_error(state, format!("Load cargo rejected ({})", resp.status()));
                }
                Ok(_) => {
                    // Optimistically set cargo state on HTTP 200.
                    // dest_idx was captured at call time (not response time)
                    // to avoid using a stale station_idx if the ship moved
                    // during the async POST.
                    state.cargo.set(Some(crate::state::CargoLoad {
                        destination_idx: dest_idx,
                        amount_pct: crate::state::REPLENISH_AMOUNT as u32,
                        manifest_id: Some(manifest_id),
                    }));
                    clear_pending_command_after_min_feedback(state);
                }
                Err(e) => {
                    set_command_error(state, format!("Failed to load cargo: {e}"));
                }
            },
            Err(_) => {
                set_command_error(state, "Failed to build load cargo request");
            }
        }
    });
}

/// Post a DeliverCargo command to the gateway.
fn deliver_cargo(state: AppState) {
    if state.pending_command.get_untracked() != PendingCommand::None {
        set_command_error(
            state,
            "Command already in progress — waiting for pipeline...",
        );
        return;
    }
    if state.connection.get_untracked() != ConnectionStatus::Connected {
        state.command_error.set(Some(CommandError {
            message: "Cannot deliver: gateway not connected".to_string(),
        }));
        return;
    }

    let current_station_idx = state
        .ships
        .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));

    let current_idx = match current_station_idx {
        Some(i) => i,
        None => {
            set_command_error(state, "Cannot deliver supplies: ship is not docked yet");
            return;
        }
    };

    let cargo = match state.cargo.get_untracked() {
        Some(c) => c,
        None => {
            set_command_error(state, "No supplies loaded — load cargo first");
            return;
        }
    };

    if cargo.destination_idx != current_idx {
        set_command_error(state, "Deliver supplies at the marked destination station");
        return;
    }

    let has_ship = state.ships.with_untracked(|ships| !ships.is_empty());
    if !has_ship {
        set_command_error(state, "Fleet is still loading — try again in a moment");
        return;
    }

    begin_pending_command(state, PendingCommand::Delivering);

    let base = gateway_base_url();
    spawn_local(async move {
        // Post to the station cargo received endpoint
        let station_id = state
            .stations
            .with_untracked(|stations| stations.get(current_idx).map(|s| s.id));
        let station_id = match station_id {
            Some(id) => id,
            None => {
                set_command_error(state, "Cannot deliver supplies: station is still loading");
                return;
            }
        };

        // Gateway expects RecordCargoReceivedRequest { manifest_id, weight_kg }
        let manifest_id = state
            .cargo
            .with_untracked(|c| c.as_ref().and_then(|c| c.manifest_id));
        let manifest_id = match manifest_id {
            Some(id) => id,
            None => {
                set_command_error(state, "No manifest ID — load cargo first");
                return;
            }
        };

        #[derive(serde::Serialize)]
        struct DeliverBody {
            manifest_id: Uuid,
            weight_kg: f32,
        }
        let deliver_json = match serde_json::to_string(&DeliverBody {
            manifest_id,
            weight_kg: 1000.0,
        }) {
            Ok(j) => j,
            Err(_) => {
                set_command_error(state, "Failed to serialize delivery command");
                return;
            }
        };

        let url = format!("{base}/stations/{station_id}/cargo");
        let result = gloo_net::http::Request::post(&url)
            .header("Content-Type", "application/json")
            .body(deliver_json);

        match result {
            Ok(req) => match req.send().await {
                Ok(resp) => {
                    if !resp.ok() {
                        let status = resp.status();
                        set_command_error(state, format!("Delivery rejected ({})", status));
                    } else {
                        // Optimistically clear cargo + pending on HTTP 200.
                        // The CargoReceived event via WS may be delayed by the
                        // Kafka publisher catch-up. Clear state now so the
                        // player can continue immediately.
                        state.cargo.set(None);
                        clear_pending_command_after_min_feedback(state);
                        // Replenish the station stock locally
                        state.stations.update(|stations| {
                            if let Some(station) = stations.get_mut(current_idx) {
                                station.stock_pct =
                                    (station.stock_pct + crate::state::REPLENISH_AMOUNT).min(100.0);
                                station.stock_low =
                                    station.stock_pct < crate::state::STOCK_LOW_THRESHOLD;
                            }
                        });
                    }
                }
                Err(e) => {
                    set_command_error(state, format!("Failed to deliver cargo: {e}"));
                }
            },
            Err(_) => {
                set_command_error(state, "Failed to build delivery request");
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

#[component]
pub fn LiveFleetPage(state: AppState) -> impl IntoView {
    let log_open = RwSignal::new(false);
    setup_command_error_logging(state);

    // Show loading overlay until the first ready snapshot is applied.
    let is_loading = move || state.ships.with(|s| s.is_empty());
    let loading_text = move || match state.connection.get() {
        ConnectionStatus::Disconnected => "Creating fresh session...",
        ConnectionStatus::Reconnecting => "Reconnecting to fleet systems...",
        ConnectionStatus::Connected => "Initialising fleet systems...",
    };
    let loading_subtext = move || match state.connection.get() {
        ConnectionStatus::Disconnected => {
            "Spinning up a ship, stations, and the first pipeline events."
        }
        ConnectionStatus::Reconnecting => {
            "Waiting for the gateway and pipeline to come back online."
        }
        ConnectionStatus::Connected => "Waiting for the bootstrap events to flow through Canon.",
    };

    view! {
        <div class="content-area">
            {move || {
                if is_loading() {
                        view! {
                            <div class="loading-overlay">
                                <div class="loading-spinner"></div>
                                <p class="loading-text">{loading_text}</p>
                                <p class="loading-subtext">{loading_subtext}</p>
                            </div>
                        }.into_any()
                } else {
                    view! {
                        <div class="live-main">
                            <div class="map-wrap">
                                <MapBar state=state log_open=log_open />
                                <MapCanvas state=state />
                                <StationCards state=state />
                                <ShipActionBar state=state />
                            </div>
                        </div>
                    }.into_any()
                }
            }}
            <EventLogPanel state=state log_open=log_open />
        </div>
    }
}

/// Log command errors to browser console (no UI banner).
fn setup_command_error_logging(state: AppState) {
    Effect::new(move |_| {
        if let Some(err) = state.command_error.get() {
            web_sys::console::warn_1(&format!("Command error: {}", err.message).into());
        }
    });
}

#[component]
fn MapBar(state: AppState, log_open: RwSignal<bool>) -> impl IntoView {
    let ships = state.ships;
    let stations = state.stations;
    let pending = state.pending_command;
    let connection = state.connection;
    // Show transit status or destination buttons
    let is_transit = move || {
        ships.with(|s| {
            s.first()
                .map(|sh| sh.status == ShipStatus::Transit)
                .unwrap_or(false)
        })
    };

    let transit_dest_name = move || {
        ships.with(|s| {
            s.first().and_then(|sh| {
                sh.destination_station_idx
                    .and_then(|di| stations.with(|st| st.get(di).map(|d| d.name.clone())))
            })
        })
    };

    let toggle_class = move || {
        if log_open.get() {
            "log-toggle active"
        } else {
            "log-toggle"
        }
    };

    let log_count = move || state.event_count.get() as usize;

    view! {
        <div class="map-bar">
            <div class="bar-lbl">
                "VSS Meridian"
                <Show when=move || pending.get() != PendingCommand::None>
                    <span class="pending-indicator">" (pending...)"</span>
                </Show>
            </div>
            <div class="dest-bar">
                {move || {
                    if is_transit() {
                        let dest_name = transit_dest_name().unwrap_or_default();
                        view! {
                            <span class="transit-indicator">
                                {format!("\u{25B6} En route \u{2192} {}", dest_name)}
                            </span>
                        }
                            .into_any()
                    } else {
                        let st = stations.get();
                        let current_station = ships
                            .with(|s| s.first().and_then(|sh| sh.current_station_idx));
                        let is_pending = pending.get() != PendingCommand::None;
                        let is_disconnected = connection.get() != ConnectionStatus::Connected;
                        let has_ship = ships.with(|s| !s.is_empty());
                        view! {
                            <div class="dest-bar-btns">
                                {st
                                    .iter()
                                    .enumerate()
                                    .map(|(si, station)| {
                                        let is_current = current_station == Some(si);
                                        let sname = station.name.clone();
                                        let label = if is_current {
                                            format!("\u{25c9} {}", sname)
                                        } else {
                                            sname
                                        };
                                        let state_btn = state;
                                        let dest = si;
                                        let disabled = is_current || is_pending || is_disconnected || !has_ship;
                                        let btn_class = if is_current {
                                            "dest-tab active"
                                        } else {
                                            "dest-tab"
                                        };
                                        view! {
                                            <button
                                                class=btn_class
                                                disabled=disabled
                                                on:click=move |_| {
                                                    depart_ship(state_btn, 0, dest);
                                                }
                                            >
                                                {label}
                                            </button>
                                        }
                                    })
                                    .collect::<Vec<_>>()}
                            </div>
                        }
                            .into_any()
                    }
                }}
            </div>
            <button class=toggle_class on:click=move |_| log_open.update(|v| *v = !*v)>
                <div class="dot"></div>
                "Event Log "
                <span class="log-count">{log_count}</span>
            </button>
        </div>
    }
}

#[component]
fn MapCanvas(state: AppState) -> impl IntoView {
    let oversight = state.oversight;
    let selected = state.selected_ship;

    let canvas_ref = NodeRef::<leptos::html::Canvas>::new();

    // Frame counter for animations (stars blink, flame flicker, warning pulse)
    let tick = Rc::new(Cell::new(0u32));

    // Start requestAnimationFrame render loop once canvas is available
    {
        let tick = Rc::clone(&tick);
        Effect::new(move |_| {
            let Some(canvas_el) = canvas_ref.get() else {
                return;
            };
            let canvas = canvas_el;

            // Size canvas to container
            if let Some(parent) = canvas.parent_element() {
                canvas.set_width(parent.client_width() as u32);
                canvas.set_height(parent.client_height() as u32);
            }

            let ctx = match canvas
                .get_context("2d")
                .ok()
                .flatten()
                .and_then(|c| c.dyn_into::<web_sys::CanvasRenderingContext2d>().ok())
            {
                Some(c) => c,
                None => return,
            };

            // Kick off the animation loop
            let raf_id: Rc<Cell<i32>> = Rc::new(Cell::new(0));
            let tick_inner = Rc::clone(&tick);
            let raf_id_inner = Rc::clone(&raf_id);

            // We need a recursive closure via Rc
            type RafClosure = Rc<std::cell::RefCell<Option<Closure<dyn FnMut()>>>>;
            let f: RafClosure = Rc::new(std::cell::RefCell::new(None));
            let g = Rc::clone(&f);

            let state_draw = state;
            *g.borrow_mut() = Some(Closure::new(move || {
                let canvas_el = match canvas_ref.get() {
                    Some(el) => el,
                    None => return,
                };

                // Resize if needed
                if let Some(parent) = canvas_el.parent_element() {
                    let pw = parent.client_width() as u32;
                    let ph = parent.client_height() as u32;
                    if canvas_el.width() != pw || canvas_el.height() != ph {
                        canvas_el.set_width(pw);
                        canvas_el.set_height(ph);
                    }
                }

                let w = canvas_el.width() as f64;
                let h = canvas_el.height() as f64;

                // Detect theme
                let light = web_sys::window()
                    .and_then(|win| win.document())
                    .and_then(|doc| doc.body())
                    .map(|body| body.class_list().contains("light"))
                    .unwrap_or(false);

                let now_ms = web_sys::window()
                    .and_then(|win| win.performance())
                    .map(|p| p.now())
                    .unwrap_or(0.0);

                let current_tick = tick_inner.get();
                tick_inner.set(current_tick.wrapping_add(1));

                // Get mutable access to ships for canvas_x/y update
                let stations = state_draw.stations.get_untracked();
                let mut ships_data = state_draw.ships.get_untracked();

                canvas_map::draw_map(
                    &ctx,
                    w,
                    h,
                    &stations,
                    &mut ships_data,
                    current_tick,
                    light,
                    now_ms,
                );

                // Write the animated canvas position to a dedicated signal so
                // popup placement works without triggering the full reactive
                // graph (MapBar, station cards, etc.) 60 times per second.
                if let Some(ship) = ships_data.first() {
                    if let (Some(sx), Some(sy)) = (ship.canvas_x, ship.canvas_y) {
                        state_draw.ship_canvas_pos.set(Some((sx, sy)));
                    }
                }

                // Schedule next frame
                if let Some(win) = web_sys::window() {
                    if let Some(ref cb) = *f.borrow() {
                        if let Ok(id) = win.request_animation_frame(cb.as_ref().unchecked_ref()) {
                            raf_id_inner.set(id);
                        }
                    }
                }
            }));

            // Start the loop
            if let Some(win) = web_sys::window() {
                if let Some(ref cb) = *g.borrow() {
                    if let Ok(id) = win.request_animation_frame(cb.as_ref().unchecked_ref()) {
                        raf_id.set(id);
                    }
                }
            }
        });
    }

    // Click handler on the canvas container -- detects clicks on ships/stations
    let on_canvas_click = move |evt: leptos::ev::MouseEvent| {
        let Some(canvas_el) = canvas_ref.get() else {
            return;
        };
        let rect = canvas_el.get_bounding_client_rect();
        let w = canvas_el.width() as f64;
        let h = canvas_el.height() as f64;
        let scale_x = w / rect.width();
        let scale_y = h / rect.height();
        let cx = (evt.client_x() as f64 - rect.left()) * scale_x;
        let cy = (evt.client_y() as f64 - rect.top()) * scale_y;

        let stations = state.stations.get_untracked();
        let mut ships = state.ships.get_untracked();

        // Inject the latest animated canvas position so hit testing during
        // transit uses the correct location (ships signal no longer carries
        // per-frame canvas_x/canvas_y).
        if let Some((sx, sy)) = state.ship_canvas_pos.get_untracked() {
            if let Some(ship) = ships.first_mut() {
                ship.canvas_x = Some(sx);
                ship.canvas_y = Some(sy);
            }
        }

        match canvas_map::hit_test(cx, cy, w, h, &stations, &ships) {
            canvas_map::CanvasHit::Ship(idx) => {
                // Toggle popup: if same ship is already selected, close; otherwise open
                if selected.get_untracked() == Some(idx) {
                    selected.set(None);
                } else {
                    selected.set(Some(idx));
                }
            }
            canvas_map::CanvasHit::Station(dest_idx) => {
                // Close popup if open
                selected.set(None);
                // Fly VSS Meridian to this station
                let ships_data = state.ships.get_untracked();
                if let Some(ship) = ships_data.first() {
                    if ship.status == ShipStatus::Transit {
                        return; // already in transit
                    }
                    if ship.current_station_idx == Some(dest_idx) {
                        return; // already here
                    }
                    depart_ship(state, 0, dest_idx);
                }
            }
            canvas_map::CanvasHit::None => {
                // Click on empty space: close popup if open
                selected.set(None);
            }
        }
    };

    // Dismiss popup when clicking the backdrop
    let on_backdrop_click = move |evt: leptos::ev::MouseEvent| {
        evt.stop_propagation();
        selected.set(None);
    };

    view! {
        <div class="map-canvas" on:click=on_canvas_click>
            <canvas
                node_ref=canvas_ref
                style="position:absolute;inset:0;width:100%;height:100%;"
            />

            <Show when=move || selected.get().is_some()>
                <div class="popup-backdrop" on:click=on_backdrop_click></div>
                {move || {
                    let sel_idx = selected.get().unwrap_or(0);
                    view! { <ShipPopup state=state ship_idx=sel_idx canvas_ref=canvas_ref /> }
                }}
            </Show>

            <OversightStrip oversight=oversight />
        </div>
    }
}

#[component]
fn ShipPopup(
    state: AppState,
    ship_idx: usize,
    canvas_ref: NodeRef<leptos::html::Canvas>,
) -> impl IntoView {
    let ships = state.ships;
    let stations = state.stations;
    let pending = state.pending_command;
    let connection = state.connection;

    let ship_data = move || ships.with(|s| s.get(ship_idx).cloned());

    let canvas_pos = state.ship_canvas_pos;

    let popup_style = move || {
        let canvas_w = canvas_ref
            .get()
            .map(|el| el.parent_element().map(|p| p.client_width()).unwrap_or(800))
            .unwrap_or(800) as f64;
        let canvas_h = canvas_ref
            .get()
            .map(|el| {
                el.parent_element()
                    .map(|p| p.client_height())
                    .unwrap_or(500)
            })
            .unwrap_or(500) as f64;

        // Read position from the dedicated canvas-pos signal (updated by the
        // animation loop) instead of from the ships signal, avoiding 60 fps
        // reactive churn on MapBar / station cards.
        let (sx, sy) = canvas_pos.get().unwrap_or_else(|| {
            ships.with(|s| {
                s.get(ship_idx)
                    .map(|ship| {
                        (
                            ship.left_pct / 100.0 * canvas_w,
                            ship.top_pct / 100.0 * canvas_h,
                        )
                    })
                    .unwrap_or((canvas_w / 2.0, canvas_h / 2.0))
            })
        });

        // Place popup to the right of the ship by default
        let mut left = sx + 22.0;
        let mut top = (sy - 20.0).max(8.0);
        // Clamp to canvas edges (popup is ~240px wide, ~270px tall)
        let popup_w = 240.0;
        let popup_h = 270.0;
        if left + popup_w > canvas_w {
            left = sx - popup_w - 12.0;
        }
        if top + popup_h > canvas_h {
            top = canvas_h - popup_h - 5.0;
        }
        if top < 8.0 {
            top = 8.0;
        }
        format!("left: {left}px; top: {top}px;")
    };

    let on_popup_click = |evt: leptos::ev::MouseEvent| {
        evt.stop_propagation();
    };

    view! {
        <div class="cmd-popup" style=popup_style on:click=on_popup_click>
            {move || {
                let data = ship_data();
                let st = stations.get();
                let is_pending = pending.get() != PendingCommand::None;
                let is_disconnected = connection.get() != ConnectionStatus::Connected;
                match data {
                    Some(ship) => {
                        let display_name = ship.name.to_uppercase();
                        let at_station = ship
                            .current_station_idx
                            .and_then(|i| st.get(i).map(|s| s.name.clone()));
                        let status_detail = match ship.status {
                            ShipStatus::Transit => {
                                let dest_name = ship
                                    .destination_station_idx
                                    .and_then(|di| st.get(di).map(|d| d.name.clone()))
                                    .unwrap_or_else(|| "unknown".to_string());
                                format!("En route \u{2192} {}", dest_name)
                            }
                            ShipStatus::Dead => "DECOMMISSIONED".to_string(),
                            ShipStatus::Docked => match at_station {
                                Some(ref name) => {
                                    format!("Docked at {} \u{00b7} ready to depart", name)
                                }
                                None => "Idle \u{00b7} select a destination".to_string(),
                            },
                        };
                        let fuel_display = format!("{:.0}%", ship.fuel_pct);
                        let version_display = format!("v.{}", ship.version);
                        let snap_pct = if ship.snapshot_every > 0 {
                            ((ship.events_since_snapshot as f64)
                                / (ship.snapshot_every as f64)
                                * 100.0)
                                .min(100.0)
                        } else {
                            0.0
                        };
                        let snap_fill_style = format!("width:{}%;", snap_pct);
                        let snap_count = format!(
                            "{}/{}",
                            ship.events_since_snapshot, ship.snapshot_every
                        );
                        let fuel_class =
                            if ship.fuel_pct < 30.0 { "pi-v a" } else { "pi-v g" };
                        view! {
                            <div>
                                <div class="popup-ship">{display_name}</div>
                                <div class="popup-stat">{status_detail}</div>
                                <div class="popup-hint">"Select destination:"</div>
                                <div class="dest-list">
                                    {st
                                        .iter()
                                        .enumerate()
                                        .map(|(si, station)| {
                                            let is_current =
                                                ship.current_station_idx == Some(si);
                                            let is_in_transit =
                                                ship.status == ShipStatus::Transit;
                                            let is_dead = ship.status == ShipStatus::Dead;
                                            let disabled =
                                                is_current || is_in_transit || is_dead
                                                || is_pending || is_disconnected;
                                            let sname = station.name.clone();
                                            let indicator = if is_current {
                                                "\u{25c9} HERE"
                                            } else if disabled {
                                                "\u{2014}"
                                            } else {
                                                "\u{2192}"
                                            };
                                            let btn_class = if disabled {
                                                "dest-btn cur"
                                            } else {
                                                "dest-btn"
                                            };
                                            let state_btn = state;
                                            let sidx = ship_idx;
                                            let dest = si;
                                            view! {
                                                <button
                                                    class=btn_class
                                                    disabled=disabled
                                                    on:click=move |evt: leptos::ev::MouseEvent| {
                                                        evt.stop_propagation();
                                                        // Fire departure before closing popup so the
                                                        // command dispatches even if the reactive
                                                        // system unmounts the component synchronously.
                                                        depart_ship(state_btn, sidx, dest);
                                                        state_btn.selected_ship.set(None);
                                                    }
                                                >
                                                    <span>{sname}</span>
                                                    <span class="arr">{indicator}</span>
                                                </button>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </div>
                                <div class="popup-info">
                                    <div class="pi-row">
                                        <span class="pi-k">"Fuel"</span>
                                        <span class=fuel_class>{fuel_display}</span>
                                    </div>
                                    <div class="pi-row">
                                        <span class="pi-k">"Aggregate version"</span>
                                        <span class="pi-v c">{version_display}</span>
                                    </div>
                                    <div class="snap-wrap">
                                        <div class="snap-lbl">
                                            <span>"Events since snapshot"</span>
                                            <span>{snap_count}</span>
                                        </div>
                                        <div class="snap-track">
                                            <div
                                                class="snap-fill"
                                                style=snap_fill_style
                                            ></div>
                                            <div
                                                class="snap-mark"
                                                style="left:0%;"
                                            ></div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        }
                            .into_any()
                    }
                    None => view! { <div></div> }.into_any(),
                }
            }}
        </div>
    }
}

#[component]
fn OversightStrip(oversight: RwSignal<OversightState>) -> impl IntoView {
    let strip_class = move || {
        let o = oversight.get();
        if o.visible {
            "os-strip"
        } else {
            "os-strip hidden"
        }
    };

    let arrival_class = move || {
        let o = oversight.get();
        if o.arrival_status == OversightReqStatus::Met {
            "os-req met"
        } else {
            "os-req"
        }
    };

    let manifest_class = move || {
        let o = oversight.get();
        if o.manifest_status == OversightReqStatus::Met {
            "os-req met"
        } else {
            "os-req"
        }
    };

    let arrival_icon = move || {
        let o = oversight.get();
        if o.arrival_status == OversightReqStatus::Met {
            "\u{2713}"
        } else {
            "\u{25CB}"
        }
    };

    let manifest_icon = move || {
        let o = oversight.get();
        if o.manifest_status == OversightReqStatus::Met {
            "\u{2713}"
        } else {
            "\u{25CB}"
        }
    };

    let badge_class = move || {
        let o = oversight.get();
        if o.arrival_status == OversightReqStatus::Met
            && o.manifest_status == OversightReqStatus::Met
        {
            "os-badge os-rdy"
        } else {
            "os-badge os-nr"
        }
    };

    let badge_text = move || {
        let o = oversight.get();
        if o.arrival_status == OversightReqStatus::Met
            && o.manifest_status == OversightReqStatus::Met
        {
            "Ready"
        } else {
            "Not Ready"
        }
    };

    view! {
        <div class=strip_class>
            <div class="os-hdr">
                <div class="os-title">
                    {move || oversight.get().gate_title.clone()}
                </div>
                <span class=badge_class>{badge_text}</span>
            </div>
            <div class="os-reqs">
                <div class=arrival_class>
                    <span class="ic">{arrival_icon}</span>
                    <span class="lb">"ShipArrivedAtStation (navigation)"</span>
                </div>
                <div class=manifest_class>
                    <span class="ic">{manifest_icon}</span>
                    <span class="lb">"ManifestCreated (cargo)"</span>
                </div>
            </div>
        </div>
    }
}

/// Returns the CSS colour for a station stock percentage.
/// Green (>50%), amber (25-50%), red (<25%). Uses CSS variables.
pub fn stock_color_var(pct: f64) -> &'static str {
    if pct > 50.0 {
        "var(--green)"
    } else if pct > 25.0 {
        "var(--amber)"
    } else {
        "var(--red)"
    }
}

#[component]
fn StationCards(state: AppState) -> impl IntoView {
    // Reactively render station cards — re-renders when state.stations changes.
    // The snapshot-push transport updates state.stations via apply_snapshot.
    view! {
        <div class="station-cards">
            {move || {
                let stations = state.stations.get();
                stations
                    .iter()
                    .enumerate()
                    .map(|(idx, station)| {
                    let pct = station.stock_pct;
                    let color = stock_color_var(pct);
                    let fill_style = format!("width:{pct:.1}%;background:{color};");
                    let pct_style = format!("color:{color};");
                    let pct_display = format!("{pct:.1}%");
                    let name = station.name.clone();
                    let supplied_by = station.supplied_by_name.clone();
                    let card_id = format!("stn-card-{idx}");
                    let fill_id = format!("stn-fill-{idx}");
                    let pct_id = format!("stn-pct-{idx}");
                    let state_click = state;

                    view! {
                        <div
                            id=card_id
                            class="stn-card"
                            on:click=move |_| {
                                let ships_data = state_click.ships.get_untracked();
                                if let Some(ship) = ships_data.first() {
                                    if ship.status == ShipStatus::Transit {
                                        return;
                                    }
                                    if ship.current_station_idx == Some(idx) {
                                        return;
                                    }
                                    depart_ship(state_click, 0, idx);
                                }
                            }
                        >
                            <div class="stn-card-name">{name}</div>
                            <div class="stn-card-sub">
                                {format!("Supplied from {supplied_by}")}
                            </div>
                            <div class="stn-card-bar">
                                <div id=fill_id class="stn-card-fill" style=fill_style></div>
                            </div>
                            <div id=pct_id class="stn-card-pct" style=pct_style>{pct_display}</div>
                        </div>
                    }
                })
                .collect_view()
            }}
        </div>
    }
}

// ---------------------------------------------------------------------------
// Ship Action Bar -- contextual row below station cards
// ---------------------------------------------------------------------------

/// Determines the contextual state for the ship action bar display.
enum ActionBarState {
    /// Ship is in transit with no cargo
    FlyingEmpty,
    /// Ship is in transit carrying cargo for a destination station
    FlyingLoaded { dest_name: String },
    /// Ship is docked at a station with no cargo
    DockedEmpty {
        station_name: String,
        next_station_name: String,
    },
    /// Ship is docked at the correct station to deliver cargo
    DockedCorrectStation { station_name: String },
    /// Ship is docked but cargo is for a different station
    DockedWrongStation { cargo_dest_name: String },
    /// A command is pending -- waiting for pipeline confirmation
    Pending {
        description: String,
        button_label: String,
    },
    /// Game over -- a station hit 0%
    GameOver,
}

fn get_action_bar_state(state: AppState) -> ActionBarState {
    if state.game_over.get() {
        return ActionBarState::GameOver;
    }

    // Check pending state first
    let pending = state.pending_command.get();
    if pending != PendingCommand::None {
        let (description, button_label) = match pending {
            PendingCommand::Departing => (
                "Departure command sent \u{2014} waiting for pipeline...".to_string(),
                "Departing...".to_string(),
            ),
            PendingCommand::Loading => (
                "Loading command sent \u{2014} waiting for pipeline...".to_string(),
                "Loading supplies...".to_string(),
            ),
            PendingCommand::Delivering => (
                "Delivery command sent \u{2014} waiting for pipeline...".to_string(),
                "Delivering supplies...".to_string(),
            ),
            PendingCommand::None => (String::new(), String::new()),
        };
        return ActionBarState::Pending {
            description,
            button_label,
        };
    }

    let ship = state.ships.with(|s| s.first().cloned());
    let cargo = state.cargo.get();
    let stations = state.stations.get();

    let Some(ship) = ship else {
        return ActionBarState::FlyingEmpty;
    };

    let station_name = |idx: usize| -> String {
        stations
            .get(idx)
            .map(|s| s.name.clone())
            .unwrap_or_default()
    };

    match ship.status {
        ShipStatus::Transit => match cargo {
            Some(cargo_load) => ActionBarState::FlyingLoaded {
                dest_name: station_name(cargo_load.destination_idx),
            },
            None => ActionBarState::FlyingEmpty,
        },
        ShipStatus::Docked => {
            let current_idx = ship.current_station_idx;
            match (current_idx, cargo) {
                (Some(cur_idx), None) => {
                    let next_name = supply_destination(cur_idx)
                        .map(&station_name)
                        .unwrap_or_default();
                    ActionBarState::DockedEmpty {
                        station_name: station_name(cur_idx),
                        next_station_name: next_name,
                    }
                }
                (Some(cur_idx), Some(cargo_load)) => {
                    if cargo_load.destination_idx == cur_idx {
                        ActionBarState::DockedCorrectStation {
                            station_name: station_name(cur_idx),
                        }
                    } else {
                        ActionBarState::DockedWrongStation {
                            cargo_dest_name: station_name(cargo_load.destination_idx),
                        }
                    }
                }
                // Not docked at any station (e.g. initial state in centre)
                (None, _) => ActionBarState::FlyingEmpty,
            }
        }
        ShipStatus::Dead => ActionBarState::GameOver,
    }
}

#[component]
fn ShipActionBar(state: AppState) -> impl IntoView {
    let connection = state.connection;

    view! {
        <div class="ship-action-bar">
            <Show when=move || state.command_error.get().is_some()>
                <span class="action-msg error-msg">
                    {move || state.command_error.get().map(|err| err.message).unwrap_or_default()}
                </span>
            </Show>
            {move || {
                let is_disconnected = connection.get() != ConnectionStatus::Connected;
                let bar_state = get_action_bar_state(state);
                match bar_state {
                    ActionBarState::FlyingEmpty => {
                        view! {
                            <span class="action-msg">"Click a planet to fly there"</span>
                        }
                            .into_any()
                    }
                    ActionBarState::FlyingLoaded { dest_name } => {
                        view! {
                            <span class="action-msg">
                                "Carrying supplies \u{2014} fly to "
                                <strong>{dest_name}</strong>
                                " to deliver"
                            </span>
                        }
                            .into_any()
                    }
                    ActionBarState::DockedEmpty {
                        station_name,
                        next_station_name,
                    } => {
                        let state_load = state;
                        view! {
                            <span class="action-msg">
                                "Docked at " <strong>{station_name}</strong>
                            </span>
                            <button
                                class="action-btn load-btn"
                                disabled=is_disconnected
                                on:click=move |_| load_cargo(state_load)
                            >
                                {format!("Load supplies for {next_station_name}")}
                            </button>
                        }
                            .into_any()
                    }
                    ActionBarState::DockedCorrectStation { station_name } => {
                        let state_deliver = state;
                        view! {
                            <span class="action-msg">
                                "Docked at " <strong>{station_name}</strong>
                            </span>
                            <button
                                class="action-btn deliver-btn"
                                disabled=is_disconnected
                                on:click=move |_| { deliver_cargo(state_deliver); }
                            >
                                "Deliver supplies here"
                            </button>
                        }
                            .into_any()
                    }
                    ActionBarState::DockedWrongStation { cargo_dest_name } => {
                        view! {
                            <span class="action-msg">
                                "Supplies are for "
                                <strong>{cargo_dest_name}</strong>
                                " \u{2014} fly there to deliver"
                            </span>
                        }
                            .into_any()
                    }
                    ActionBarState::Pending {
                        description,
                        button_label,
                    } => {
                        view! {
                            <span class="action-msg pending-msg pending">{description}</span>
                            <button class="action-btn pending-btn pending" disabled=true>
                                {button_label}
                            </button>
                        }
                            .into_any()
                    }
                    ActionBarState::GameOver => {
                        let state_restart = state;
                        view! {
                            <span class="action-msg game-over-msg">
                                "Supply chain collapsed"
                            </span>
                            <button
                                class="action-btn restart-btn"
                                on:click=move |_| restart_game(state_restart)
                            >
                                "Restart"
                            </button>
                        }
                            .into_any()
                    }
                }
            }}
        </div>
    }
}

#[component]
fn EventLogPanel(state: AppState, log_open: RwSignal<bool>) -> impl IntoView {
    let entries = state.log_entries;
    let highlighted = state.highlighted_corr;

    let highlight_random = move |_| {
        entries.with(|e| {
            if let Some(entry) = e.first() {
                let corr = entry.correlation_id;
                if highlighted.get() == Some(corr) {
                    highlighted.set(None);
                } else {
                    highlighted.set(Some(corr));
                }
            }
        });
    };

    let panel_class = move || {
        if log_open.get() {
            "log-panel open"
        } else {
            "log-panel"
        }
    };

    view! {
        <div class=panel_class>
            <div class="log-panel-hdr">
                <div style="display:flex;align-items:center;gap:10px;">
                    <span class="bar-lbl">"Event Log"</span>
                    <div class="live-badge"><div class="dot"></div>"Live"</div>
                </div>
                <button class="log-panel-close" on:click=move |_| log_open.set(false)>
                    "\u{2715} Close"
                </button>
            </div>
            <div class="log-body">
                <For
                    each=move || entries.get()
                    key=|entry| entry.id
                    children=move |entry| {
                        view! { <LogEntryRow entry=entry highlighted=highlighted /> }
                    }
                />
            </div>
            <div class="log-footer">
                "Events are append-only \u{2014} "
                <a on:click=highlight_random>"highlight random correlation"</a>
            </div>
        </div>
    }
}

#[component]
fn LogEntryRow(entry: LogEntry, highlighted: RwSignal<Option<Uuid>>) -> impl IntoView {
    let corr_id = entry.correlation_id;
    let is_highlighted = move || highlighted.get() == Some(corr_id);

    let row_class = move || {
        let mut cls = "log-item".to_string();
        if entry.is_new {
            cls.push_str(" fresh");
        }
        if is_highlighted() {
            cls.push_str(" lit");
        }
        cls
    };

    let svc_class = match entry.service.as_str() {
        "fleet" => "svc sf",
        "cargo" => "svc sc",
        "nav" => "svc sn",
        "supply" => "svc su",
        "station" => "svc ss",
        _ => "svc",
    };

    let agg_short = format!("{}...", &entry.aggregate_id.to_string()[..8]);

    let on_click = move |evt: leptos::ev::MouseEvent| {
        evt.stop_propagation();
        if highlighted.get() == Some(corr_id) {
            highlighted.set(None);
        } else {
            highlighted.set(Some(corr_id));
        }
    };

    view! {
        <div class=row_class on:click=on_click>
            <span class="log-ts">{format_time(&entry.timestamp)}</span>
            <div class="log-row">
                <span class=svc_class>{entry.service.clone()}</span>
                <span class="log-name">{entry.event_name.clone()}</span>
                <span class="log-agg">{agg_short}</span>
            </div>
        </div>
    }
}
