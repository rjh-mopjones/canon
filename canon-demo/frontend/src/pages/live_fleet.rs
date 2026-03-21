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
    supply_destination, AppState, CommandError, ConnectionStatus, LogEntry, OversightReqStatus,
    OversightState, PendingCommand, ShipStatus, DRAIN_RATES, STARTING_STOCK, STOCK_LOW_THRESHOLD,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Stock drain interval in ms (3 seconds per tick)
const DRAIN_INTERVAL_MS: u32 = 3000;

// ---------------------------------------------------------------------------
// Supply chain game logic (client-side game mechanics)
// ---------------------------------------------------------------------------

/// Start the stock drain timer. Called once when the LiveFleetPage mounts.
/// Returns nothing -- the interval handle is leaked intentionally (lives for
/// the lifetime of the page).
///
/// Stock drain is a client-side game mechanic that creates urgency. It is not
/// an event-sourced domain concept. Replenishment (delivery) is driven by real
/// events arriving via WebSocket.
fn start_stock_drain(state: AppState) {
    let _ = gloo_timers::callback::Interval::new(DRAIN_INTERVAL_MS, move || {
        // Skip drain if game is already over
        if state.game_over.get_untracked() {
            return;
        }

        let mut any_depleted = false;

        state.stations.update(|stations| {
            for (i, station) in stations.iter_mut().enumerate() {
                if let Some(rate) = DRAIN_RATES.get(i) {
                    station.stock_pct = (station.stock_pct - rate).max(0.0);
                    station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;

                    if station.stock_pct <= 0.0 {
                        any_depleted = true;
                    }
                }
            }
        });

        if any_depleted {
            trigger_game_over(state);
        }
    });
}

/// Handle game over -- stop draining.
fn trigger_game_over(state: AppState) {
    state.game_over.set(true);
}

/// Reset the game: restore stock levels, clear cargo, clear game over.
fn restart_game(state: AppState) {
    state.game_over.set(false);
    state.cargo.set(None);
    state.command_error.set(None);

    state.stations.update(|stations| {
        for (i, station) in stations.iter_mut().enumerate() {
            if let Some(starting) = STARTING_STOCK.get(i) {
                station.stock_pct = *starting;
                station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;
            }
        }
    });

    // Reset ship to undocked centre
    state.ships.update(|ships| {
        if let Some(ship) = ships.first_mut() {
            ship.status = ShipStatus::Docked;
            ship.current_station_idx = None;
            ship.destination_station_idx = None;
            ship.left_pct = 50.0;
            ship.top_pct = 50.0;
            ship.canvas_x = None;
            ship.canvas_y = None;
            ship.from_pct_x = None;
            ship.from_pct_y = None;
            ship.flight_start_ms = None;
            ship.flight_duration_ms = None;
            ship.fuel_pct = 72.0;
        }
    });

    // Clear log
    state.log_entries.update(|entries| entries.clear());
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
        return;
    }
    if state.pending_command.get_untracked() != PendingCommand::None {
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
        _ => return,
    };

    // Set destination on ship so the WS handler knows where to animate to
    state.ships.update(|ships| {
        if let Some(ship) = ships.get_mut(ship_idx) {
            ship.destination_station_idx = Some(dest_idx);
        }
    });

    // Set pending state and clear previous errors
    state.pending_command.set(PendingCommand::Departing);
    state.command_error.set(None);

    let base = gateway_base_url();
    spawn_local(async move {
        #[derive(serde::Serialize)]
        struct DepartBody {
            destination: Uuid,
        }
        let body = DepartBody {
            destination: station_id,
        };
        let url = format!("{base}/fleet/ships/{ship_id}/depart");
        let body_json = match serde_json::to_string(&body) {
            Ok(j) => j,
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to serialize departure command".to_string(),
                }));
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
                        state.pending_command.set(PendingCommand::None);
                        state.command_error.set(Some(CommandError {
                            message: format!(
                                "Departure rejected ({}): {}",
                                status,
                                if body_text.is_empty() {
                                    "unknown error"
                                } else {
                                    &body_text
                                }
                            ),
                        }));
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
                    state.pending_command.set(PendingCommand::None);
                    state.command_error.set(Some(CommandError {
                        message: format!("Failed to send departure command: {e}"),
                    }));
                    // Clear destination since command failed
                    state.ships.update(|ships| {
                        if let Some(ship) = ships.get_mut(ship_idx) {
                            ship.destination_station_idx = None;
                        }
                    });
                }
            },
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to build departure request".to_string(),
                }));
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
        None => return,
    };

    let _dest_idx = match supply_destination(idx) {
        Some(d) => d,
        None => return,
    };

    // Already carrying cargo? No-op.
    if state.cargo.get_untracked().is_some() {
        return;
    }

    let ship_id = state
        .ships
        .with_untracked(|ships| ships.first().map(|s| s.id));
    let ship_id = match ship_id {
        Some(id) => id,
        None => return,
    };

    state.pending_command.set(PendingCommand::Loading);
    state.command_error.set(None);

    let base = gateway_base_url();
    spawn_local(async move {
        // Create manifest then load cargo
        let manifest_url = format!("{base}/cargo/manifests");
        #[derive(serde::Serialize)]
        struct ManifestBody {
            ship_id: Uuid,
        }
        let manifest_json = match serde_json::to_string(&ManifestBody { ship_id }) {
            Ok(j) => j,
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to serialize manifest command".to_string(),
                }));
                return;
            }
        };

        let manifest_result = gloo_net::http::Request::post(&manifest_url)
            .header("Content-Type", "application/json")
            .body(manifest_json);

        let send_result = match manifest_result {
            Ok(req) => req.send().await,
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to build load cargo request".to_string(),
                }));
                return;
            }
        };

        match send_result {
            Ok(resp) if !resp.ok() => {
                let status = resp.status();
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: format!("Load cargo rejected ({})", status),
                }));
            }
            Ok(_) => {
                // Cargo loaded event will arrive via WebSocket and update state
            }
            Err(e) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: format!("Failed to load cargo: {e}"),
                }));
            }
        }
    });
}

/// Post a DeliverCargo command to the gateway.
fn deliver_cargo(state: AppState) {
    if state.pending_command.get_untracked() != PendingCommand::None {
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
        None => return,
    };

    let cargo = match state.cargo.get_untracked() {
        Some(c) => c,
        None => return,
    };

    if cargo.destination_idx != current_idx {
        return;
    }

    let ship_id = state
        .ships
        .with_untracked(|ships| ships.first().map(|s| s.id));
    let ship_id = match ship_id {
        Some(id) => id,
        None => return,
    };

    state.pending_command.set(PendingCommand::Delivering);
    state.command_error.set(None);

    let base = gateway_base_url();
    spawn_local(async move {
        // Post to the station cargo received endpoint
        let station_id = state
            .stations
            .with_untracked(|stations| stations.get(current_idx).map(|s| s.id));
        let station_id = match station_id {
            Some(id) => id,
            None => {
                state.pending_command.set(PendingCommand::None);
                return;
            }
        };

        #[derive(serde::Serialize)]
        struct DeliverBody {
            ship_id: Uuid,
        }
        let deliver_json = match serde_json::to_string(&DeliverBody { ship_id }) {
            Ok(j) => j,
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to serialize delivery command".to_string(),
                }));
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
                        state.pending_command.set(PendingCommand::None);
                        state.command_error.set(Some(CommandError {
                            message: format!("Delivery rejected ({})", status),
                        }));
                    }
                    // Delivery event will arrive via WebSocket and update state
                }
                Err(e) => {
                    state.pending_command.set(PendingCommand::None);
                    state.command_error.set(Some(CommandError {
                        message: format!("Failed to deliver cargo: {e}"),
                    }));
                }
            },
            Err(_) => {
                state.pending_command.set(PendingCommand::None);
                state.command_error.set(Some(CommandError {
                    message: "Failed to build delivery request".to_string(),
                }));
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

#[component]
pub fn LiveFleetPage(state: AppState) -> impl IntoView {
    // Start the stock drain timer on mount (leaks intentionally -- lives for page lifetime).
    Effect::new(move |_| {
        start_stock_drain(state);
    });

    view! {
        <div class="content-area">
            <div class="map-wrap">
                <ConnectionBanner state=state />
                <MapBar state=state />
                <MapCanvas state=state />
                <StationCards state=state />
                <ShipActionBar state=state />
            </div>
            <EventLogStrip state=state />
        </div>
    }
}

/// Banner showing connection status and command errors.
/// Displayed when the gateway is not connected or when a command fails.
#[component]
fn ConnectionBanner(state: AppState) -> impl IntoView {
    let connection = state.connection;
    let command_error = state.command_error;

    let show_banner =
        move || connection.get() != ConnectionStatus::Connected || command_error.get().is_some();

    let banner_class = move || {
        if connection.get() != ConnectionStatus::Connected {
            "connection-banner disconnected"
        } else {
            "connection-banner error"
        }
    };

    let banner_text = move || {
        if connection.get() == ConnectionStatus::Reconnecting {
            "Reconnecting to gateway...".to_string()
        } else if connection.get() == ConnectionStatus::Disconnected {
            "Backend unavailable \u{2014} commands disabled".to_string()
        } else if let Some(err) = command_error.get() {
            err.message
        } else {
            String::new()
        }
    };

    let on_dismiss = move |_| {
        state.command_error.set(None);
    };

    view! {
        <Show when=show_banner>
            <div class=banner_class>
                <span class="banner-text">{banner_text}</span>
                <Show when=move || command_error.get().is_some()>
                    <button class="banner-dismiss" on:click=on_dismiss>
                        "\u{2715}"
                    </button>
                </Show>
            </div>
        </Show>
    }
}

#[component]
fn MapBar(state: AppState) -> impl IntoView {
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
                        view! {
                            <div class="dest-bar-btns">
                                {st
                                    .iter()
                                    .enumerate()
                                    .map(|(si, station)| {
                                        let is_current = current_station == Some(si);
                                        let sname = station.name.clone();
                                        let state_btn = state;
                                        let dest = si;
                                        let disabled = is_current || is_pending || is_disconnected;
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
                                                {sname}
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

                // Write back canvas_x/y so popup placement and hit testing work.
                // Only call update (which notifies subscribers) when positions changed.
                let needs_update = state_draw.ships.with_untracked(|ships| {
                    ships.iter().enumerate().any(|(i, ship)| {
                        ships_data
                            .get(i)
                            .map(|drawn| {
                                ship.canvas_x != drawn.canvas_x || ship.canvas_y != drawn.canvas_y
                            })
                            .unwrap_or(false)
                    })
                });
                if needs_update {
                    state_draw.ships.update(|ships| {
                        for (i, ship) in ships.iter_mut().enumerate() {
                            if let Some(drawn) = ships_data.get(i) {
                                ship.canvas_x = drawn.canvas_x;
                                ship.canvas_y = drawn.canvas_y;
                            }
                        }
                    });
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
        let ships = state.ships.get_untracked();

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

        ships.with(|s| {
            s.get(ship_idx)
                .map(|ship| {
                    // Use canvas pixel positions for popup placement
                    let sx = ship.canvas_x.unwrap_or(ship.left_pct / 100.0 * canvas_w);
                    let sy = ship.canvas_y.unwrap_or(ship.top_pct / 100.0 * canvas_h);
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
                })
                .unwrap_or_default()
        })
    };

    let on_popup_click = |evt: leptos::ev::MouseEvent| {
        evt.stop_propagation();
    };

    view! {
        <div class="ship-popup" style=popup_style on:click=on_popup_click>
            {move || {
                let data = ship_data();
                let st = stations.get();
                let is_pending = pending.get() != PendingCommand::None;
                let is_disconnected = connection.get() != ConnectionStatus::Connected;
                match data {
                    Some(ship) => {
                        let display_name =
                            format!("VSS {}", ship.name.to_uppercase());
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
                        let version_display = format!("v{}", ship.version);
                        let snap_pct = if ship.snapshot_every > 0 {
                            ((ship.events_since_snapshot as f64)
                                / (ship.snapshot_every as f64)
                                * 100.0)
                                .min(100.0)
                        } else {
                            0.0
                        };
                        let snap_fill_style = format!("width: {}%;", snap_pct);
                        let snap_count = format!(
                            "{}/{}",
                            ship.events_since_snapshot, ship.snapshot_every
                        );
                        let fuel_class =
                            if ship.fuel_pct < 30.0 { "pi-v a" } else { "pi-v g" };
                        view! {
                            <div>
                                <div class="ship-popup-name">{display_name}</div>
                                <div class="ship-popup-status">{status_detail}</div>
                                <div class="ship-popup-hint">"Select destination:"</div>
                                <div class="ship-popup-destinations">
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
                                            let state_btn = state;
                                            let sidx = ship_idx;
                                            let dest = si;
                                            view! {
                                                <button
                                                    class="dest-btn"
                                                    disabled=disabled
                                                    on:click=move |evt: leptos::ev::MouseEvent| {
                                                        evt.stop_propagation();
                                                        state_btn.selected_ship.set(None);
                                                        depart_ship(state_btn, sidx, dest);
                                                    }
                                                >
                                                    {sname}
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
                                            <span class="snap-count">{snap_count}</span>
                                        </div>
                                        <div class="snapshot-bar-container">
                                            <div
                                                class="snapshot-bar-fill"
                                                style=snap_fill_style
                                            ></div>
                                            <div
                                                class="snapshot-bar-marker"
                                                style="left: 0%;"
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
            "oversight-strip"
        } else {
            "oversight-strip hidden"
        }
    };

    let arrival_class = move || {
        let o = oversight.get();
        if o.arrival_status == OversightReqStatus::Met {
            "oversight-req met"
        } else {
            "oversight-req"
        }
    };

    let manifest_class = move || {
        let o = oversight.get();
        if o.manifest_status == OversightReqStatus::Met {
            "oversight-req met"
        } else {
            "oversight-req"
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
            "oversight-status-badge ready"
        } else {
            "oversight-status-badge not-ready"
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
            <div class="oversight-hdr">
                <div class="oversight-hdr-left">
                    <div class="oversight-handler-id">
                        {move || oversight.get().handler_id.clone()}
                    </div>
                    <div class="oversight-gate-title">
                        {move || oversight.get().gate_title.clone()}
                    </div>
                </div>
                <span class=badge_class>{badge_text}</span>
            </div>
            <div class="oversight-reqs">
                <div class=arrival_class>
                    <span class="req-icon">{arrival_icon}</span>
                    <span class="req-label">"ShipArrivedAtStation (navigation)"</span>
                </div>
                <div class=manifest_class>
                    <span class="req-icon">{manifest_icon}</span>
                    <span class="req-label">"ManifestCreated (cargo)"</span>
                </div>
            </div>
        </div>
    }
}

/// Returns the CSS colour for a station stock percentage.
/// Green (>50%), amber (25-50%), red (<25%). Uses CSS variables.
fn stock_color_var(pct: f64) -> &'static str {
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
    let stations = state.stations;
    let ships = state.ships;

    view! {
        <div class="station-cards">
            {move || {
                let st = stations.get();
                let sh = ships.get();

                st.iter()
                    .enumerate()
                    .map(|(idx, station)| {
                        // Highlight card if any non-dead ship is docked at this station
                        let is_active = sh.iter().any(|s| {
                            s.status == ShipStatus::Docked && s.current_station_idx == Some(idx)
                        });
                        let card_class = if is_active {
                            "stn-card active-stn"
                        } else {
                            "stn-card"
                        };
                        let pct = station.stock_pct;
                        let color = stock_color_var(pct);
                        let fill_style = format!("width:{pct}%;background:{color};");
                        let pct_style = format!("color:{color};");
                        let pct_display = format!("{pct:.0}%");
                        let supplied_by = station.supplied_by_name.clone();
                        let name = station.name.clone();
                        let state_click = state;
                        let dest_idx = idx;

                        view! {
                            <div
                                class=card_class
                                on:click=move |_| {
                                    // Fly the first live ship to this station (same as clicking planet)
                                    let ships_data = state_click.ships.get_untracked();
                                    if let Some(ship) = ships_data.first() {
                                        if ship.status == ShipStatus::Transit {
                                            return;
                                        }
                                        if ship.current_station_idx == Some(dest_idx) {
                                            return;
                                        }
                                        depart_ship(state_click, 0, dest_idx);
                                    }
                                }
                            >
                                <div class="stn-card-name">{name}</div>
                                <div class="stn-card-sub">
                                    {format!("Supplied from {supplied_by}")}
                                </div>
                                <div class="stn-card-bar">
                                    <div class="stn-card-fill" style=fill_style></div>
                                </div>
                                <div class="stn-card-pct" style=pct_style>{pct_display}</div>
                            </div>
                        }
                    })
                    .collect::<Vec<_>>()
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
    Pending { description: String },
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
        let desc = match pending {
            PendingCommand::Departing => {
                "Departure command sent \u{2014} waiting for pipeline...".to_string()
            }
            PendingCommand::Loading => {
                "Loading command sent \u{2014} waiting for pipeline...".to_string()
            }
            PendingCommand::Delivering => {
                "Delivery command sent \u{2014} waiting for pipeline...".to_string()
            }
            PendingCommand::None => String::new(),
        };
        return ActionBarState::Pending { description: desc };
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
                    ActionBarState::Pending { description } => {
                        view! {
                            <span class="action-msg pending-msg">{description}</span>
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
fn EventLogStrip(state: AppState) -> impl IntoView {
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

    view! {
        <div class="event-log-strip">
            <div class="log-header">
                <span class="bar-lbl">"Event log"</span>
                <div class="live-badge"><div class="dot"></div>"Live"</div>
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
                "Click any event to trace its correlation chain "
                <a on:click=highlight_random>"(highlight random)"</a>
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
            <span class="log-ts">{entry.timestamp.clone()}</span>
            <div class="log-row">
                <span class=svc_class>{entry.service.clone()}</span>
                <span class="log-name">{entry.event_name.clone()}</span>
                <span class="log-agg">{agg_short}</span>
            </div>
        </div>
    }
}
