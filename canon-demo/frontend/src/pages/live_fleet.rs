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
    supply_destination, AppState, CargoLoad, DataMode, LogEntry, OversightReqStatus,
    OversightState, ShipStatus, DRAIN_RATES, REPLENISH_AMOUNT, STARTING_STOCK,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_LOG_ENTRIES: usize = 60;
const GAMMA_OUTPOST_IDX: usize = 2;

// Voyage event chain delays (ms)
const CHAIN: &[(u32, &str, &str)] = &[
    (0, "fleet", "ShipDeparted"),
    (500, "nav", "RoutePlanned"),
    (1200, "cargo", "ManifestCreated"),
    (1600, "nav", "PositionUpdated"),
    (2800, "nav", "PositionUpdated"),
    (4200, "nav", "ShipArrivedAtStation"),
    (4600, "station", "ShipDocked"),
    (4900, "cargo", "UnloadingStarted"),
    (5400, "cargo", "CargoUnloaded"),
    (5900, "cargo", "CargoUnloaded"),
    (6200, "station", "CargoReceived"),
    (7200, "cargo", "ManifestClosed"),
];

// Extra chain for Gamma Outpost destination (appended after CargoReceived)
const GAMMA_EXTRA: &[(u32, &str, &str)] = &[
    (800, "station", "StationStockLow"),
    (1300, "supply", "ResupplyRequested"),
    (1800, "supply", "ResupplyDispatched"),
    (2300, "fleet", "ResupplyScheduled"),
];

// Transit duration in ms — canvas animation, no CSS transition needed.
const TRANSIT_DURATION_MS: u32 = 4200;

// Stock drain interval in ms (3 seconds per tick)
const DRAIN_INTERVAL_MS: u32 = 3000;

// Stock low warning threshold (percentage)
const STOCK_LOW_THRESHOLD: f64 = 20.0;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_hms() -> String {
    let perf = web_sys::window()
        .and_then(|w| w.performance())
        .map(|p| p.now())
        .unwrap_or(0.0);
    let secs = (perf / 1000.0) as u64;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    let ms = (perf as u64) % 1000;
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

fn push_log_entry(state: AppState, service: &str, event_name: &str, agg_id: Uuid, corr_id: Uuid) {
    let version = state.ships.with_untracked(|ships| {
        ships
            .iter()
            .find(|s| s.id == agg_id)
            .map(|s| s.version)
            .unwrap_or(0)
    });

    let entry = LogEntry {
        id: Uuid::new_v4(),
        timestamp: now_hms(),
        version,
        service: service.to_string(),
        event_name: event_name.to_string(),
        aggregate_id: agg_id,
        correlation_id: corr_id,
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
}

// ---------------------------------------------------------------------------
// Supply chain game logic
// ---------------------------------------------------------------------------

/// Start the stock drain timer. Called once when the LiveFleetPage mounts.
/// Returns nothing — the interval handle is leaked intentionally (lives for
/// the lifetime of the page).
fn start_stock_drain(state: AppState) {
    let _ = gloo_timers::callback::Interval::new(DRAIN_INTERVAL_MS, move || {
        // Skip drain if game is already over
        if state.game_over.get_untracked() {
            return;
        }

        let mut any_depleted = false;
        let mut low_stations: Vec<(Uuid, String)> = Vec::new();

        state.stations.update(|stations| {
            for (i, station) in stations.iter_mut().enumerate() {
                if let Some(rate) = DRAIN_RATES.get(i) {
                    station.stock_pct = (station.stock_pct - rate).max(0.0);
                    station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;

                    if station.stock_pct <= 0.0 {
                        any_depleted = true;
                    }

                    // Randomly fire StationStockLow when crossing threshold
                    // (fire when stock just crossed below 20%, or randomly at ~33% chance
                    // when already below)
                    if station.stock_pct < STOCK_LOW_THRESHOLD
                        && station.stock_pct + rate >= STOCK_LOW_THRESHOLD
                    {
                        low_stations.push((station.id, station.name.clone()));
                    }
                }
            }
        });

        // Fire StationStockLow events
        for (station_id, _station_name) in &low_stations {
            let corr = Uuid::new_v4();
            push_log_entry(state, "station", "StationStockLow", *station_id, corr);
        }

        if any_depleted {
            trigger_game_over(state);
        }
    });
}

/// Handle game over — stop draining and fire StationOffline event.
fn trigger_game_over(state: AppState) {
    state.game_over.set(true);

    // Find the first depleted station for the event
    let depleted_id = state
        .stations
        .with_untracked(|stations| stations.iter().find(|s| s.stock_pct <= 0.0).map(|s| s.id));

    if let Some(station_id) = depleted_id {
        let corr = Uuid::new_v4();
        push_log_entry(state, "station", "StationOffline", station_id, corr);
    }
}

/// Reset the game: restore stock levels, clear cargo, clear game over.
fn restart_game(state: AppState) {
    state.game_over.set(false);
    state.cargo.set(None);

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

/// Load supplies at the current station (for the next station in the supply loop).
fn load_cargo(state: AppState) {
    let current_station_idx = state
        .ships
        .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));

    let idx = match current_station_idx {
        Some(i) => i,
        None => return,
    };

    let dest_idx = match supply_destination(idx) {
        Some(d) => d,
        None => return,
    };

    // Already carrying cargo? No-op.
    if state.cargo.get_untracked().is_some() {
        return;
    }

    state.cargo.set(Some(CargoLoad {
        destination_idx: dest_idx,
        amount_pct: REPLENISH_AMOUNT as u32,
    }));

    // Fire CargoLoaded event
    let ship_id = state
        .ships
        .with_untracked(|ships| ships.first().map(|s| s.id));
    if let Some(sid) = ship_id {
        let corr = Uuid::new_v4();
        push_log_entry(state, "cargo", "CargoLoaded", sid, corr);
    }
}

/// Deliver supplies at the current station (if it matches the cargo destination).
/// Returns true if delivery succeeded, false otherwise.
fn deliver_cargo(state: AppState) -> bool {
    let current_station_idx = state
        .ships
        .with_untracked(|ships| ships.first().and_then(|s| s.current_station_idx));

    let current_idx = match current_station_idx {
        Some(i) => i,
        None => return false,
    };

    let cargo = match state.cargo.get_untracked() {
        Some(c) => c,
        None => return false,
    };

    if cargo.destination_idx != current_idx {
        return false;
    }

    // Deliver: replenish station stock
    state.stations.update(|stations| {
        if let Some(station) = stations.get_mut(current_idx) {
            station.stock_pct = (station.stock_pct + REPLENISH_AMOUNT).min(100.0);
            station.stock_low = station.stock_pct < STOCK_LOW_THRESHOLD;
        }
    });

    state.cargo.set(None);

    // Fire events
    let (ship_id, station_id) = state.ships.with_untracked(|ships| {
        let sid = ships.first().map(|s| s.id);
        (
            sid,
            state
                .stations
                .with_untracked(|stations| stations.get(current_idx).map(|s| s.id)),
        )
    });

    let corr = Uuid::new_v4();
    if let Some(sid) = ship_id {
        push_log_entry(state, "cargo", "CargoUnloaded", sid, corr);
    }
    if let Some(station_id) = station_id {
        push_log_entry(state, "station", "StockReplenished", station_id, corr);
    }

    true
}

// ---------------------------------------------------------------------------
// Flight scheduling
// ---------------------------------------------------------------------------

fn schedule_departure(state: AppState, ship_idx: usize, dest_idx: usize) {
    // Block departures during game over
    if state.game_over.get_untracked() {
        return;
    }

    let stations = state.stations.get_untracked();
    let dest = &stations[dest_idx];
    let dest_left = dest.left_pct;
    let dest_top = dest.top_pct;
    let is_gamma = dest_idx == GAMMA_OUTPOST_IDX;

    // Capture the current performance.now() for canvas animation start time
    let now_ms = web_sys::window()
        .and_then(|w| w.performance())
        .map(|p| p.now())
        .unwrap_or(0.0);

    // Update ship to transit with canvas animation fields
    let (ship_id, corr_id) = state
        .ships
        .try_update(|ships| {
            if let Some(ship) = ships.get_mut(ship_idx) {
                // Record origin for route line drawing and animation
                ship.from_pct_x = Some(ship.left_pct);
                ship.from_pct_y = Some(ship.top_pct);
                ship.flight_start_ms = Some(now_ms);
                ship.flight_duration_ms = Some(TRANSIT_DURATION_MS as f64);
                ship.status = ShipStatus::Transit;
                ship.destination_station_idx = Some(dest_idx);
                ship.current_station_idx = None;
                ship.fuel_pct = (ship.fuel_pct - 5.0).max(10.0);
                // Clear cached canvas positions so they get recomputed by draw loop
                ship.canvas_x = None;
                ship.canvas_y = None;
                (ship.id, Uuid::new_v4())
            } else {
                (Uuid::new_v4(), Uuid::new_v4())
            }
        })
        .unwrap_or((Uuid::new_v4(), Uuid::new_v4()));

    // Show oversight strip
    state.oversight.set(OversightState {
        visible: true,
        handler_id: format!("unloading-handler-{}", &corr_id.to_string()[..8]),
        gate_title: "Cargo Unloading Gate".into(),
        arrival_status: OversightReqStatus::Pending,
        manifest_status: OversightReqStatus::Pending,
    });

    // Fire event chain
    fire_event_chain(state, ship_idx, dest_idx, ship_id, corr_id, is_gamma);

    // On arrival (after transit duration), dock the ship. No auto-departure.
    let state_arrive = state;
    let _ = gloo_timers::callback::Timeout::new(TRANSIT_DURATION_MS, move || {
        state_arrive.ships.update(|ships| {
            if let Some(ship) = ships.get_mut(ship_idx) {
                ship.status = ShipStatus::Docked;
                ship.current_station_idx = Some(dest_idx);
                ship.destination_station_idx = None;
                ship.left_pct = dest_left;
                ship.top_pct = dest_top;
                ship.version += 12;
                ship.events_since_snapshot =
                    (ship.events_since_snapshot + 12) % ship.snapshot_every;
                // Clear flight animation fields
                ship.flight_start_ms = None;
                ship.flight_duration_ms = None;
                ship.from_pct_x = None;
                ship.from_pct_y = None;
                ship.canvas_x = None;
                ship.canvas_y = None;
            }
        });
    });
}

fn fire_event_chain(
    state: AppState,
    _ship_idx: usize,
    dest_idx: usize,
    ship_id: Uuid,
    corr_id: Uuid,
    is_gamma: bool,
) {
    for &(delay_ms, service, event_name) in CHAIN {
        let state_evt = state;
        let svc = service.to_string();
        let evt = event_name.to_string();
        let sid = ship_id;
        let cid = corr_id;

        if delay_ms == 0 {
            push_log_entry(state_evt, &svc, &evt, sid, cid);
            // Update oversight for specific events
            update_oversight_for_event(state_evt, &evt);
        } else {
            let _ = gloo_timers::callback::Timeout::new(delay_ms, move || {
                push_log_entry(state_evt, &svc, &evt, sid, cid);
                update_oversight_for_event(state_evt, &evt);
            });
        }
    }

    if is_gamma {
        let base_delay = 6200; // after CargoReceived
        let gamma_corr = Uuid::new_v4();
        let station_id = state.stations.with_untracked(|s| s[dest_idx].id);

        for &(extra_delay, service, event_name) in GAMMA_EXTRA {
            let total = base_delay + extra_delay;
            let state_evt = state;
            let svc = service.to_string();
            let evt = event_name.to_string();
            let aid = if service == "station" {
                station_id
            } else {
                ship_id
            };
            let cid = gamma_corr;

            let _ = gloo_timers::callback::Timeout::new(total, move || {
                push_log_entry(state_evt, &svc, &evt, aid, cid);
            });
        }
    }
}

fn update_oversight_for_event(state: AppState, event_name: &str) {
    match event_name {
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
            // Hide oversight strip 1000ms after unloading starts
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

/// Post a departure command to the live gateway.
/// This is used when `data_mode == Live` so the real Canon pipeline handles it.
fn post_departure_to_gateway(state: AppState, ship_idx: usize, dest_idx: usize) {
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
            Err(_) => return,
        };

        // Fire-and-forget: the gateway will broadcast events via WebSocket.
        // If the POST fails, the local simulation fallback still runs.
        if let Ok(req) = gloo_net::http::Request::post(&url)
            .header("Content-Type", "application/json")
            .body(body_json)
        {
            let _ = req.send().await;
        }
    });

    // Also run local simulation for immediate visual feedback.
    // In live mode, the WebSocket will patch the authoritative state;
    // the local simulation provides instant UI responsiveness.
    schedule_departure(state, ship_idx, dest_idx);
}

// Autonomous flight loop removed — ship only moves on user command.

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

#[component]
pub fn LiveFleetPage(state: AppState) -> impl IntoView {
    // Start the stock drain timer on mount (leaks intentionally — lives for page lifetime).
    Effect::new(move |_| {
        start_stock_drain(state);
    });

    view! {
        <div class="content-area">
            <div class="map-wrap">
                <MapBar state=state />
                <MapCanvas state=state />
                <StationCards state=state />
                <ShipActionBar state=state />
            </div>
            <EventLogStrip state=state />
        </div>
    }
}

#[component]
fn MapBar(state: AppState) -> impl IntoView {
    let ships = state.ships;
    let stations = state.stations;

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
            <div class="bar-lbl">"VSS Meridian"</div>
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
                                        let btn_class = if is_current {
                                            "dest-tab active"
                                        } else {
                                            "dest-tab"
                                        };
                                        view! {
                                            <button
                                                class=btn_class
                                                disabled=is_current
                                                on:click=move |_| {
                                                    if state_btn.data_mode.get_untracked() == DataMode::Live {
                                                        post_departure_to_gateway(state_btn, 0, dest);
                                                    } else {
                                                        schedule_departure(state_btn, 0, dest);
                                                    }
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

    // Click handler on the canvas container — detects clicks on ships/stations
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

        // If popup is open, close it
        if selected.get_untracked().is_some() {
            selected.set(None);
            return;
        }

        let stations = state.stations.get_untracked();
        let ships = state.ships.get_untracked();

        match canvas_map::hit_test(cx, cy, w, h, &stations, &ships) {
            canvas_map::CanvasHit::Ship(idx) => {
                selected.set(Some(idx));
            }
            canvas_map::CanvasHit::Station(dest_idx) => {
                // Fly VSS Meridian to this station
                let ships_data = state.ships.get_untracked();
                if let Some(ship) = ships_data.first() {
                    if ship.status == ShipStatus::Transit {
                        return; // already in transit
                    }
                    if ship.current_station_idx == Some(dest_idx) {
                        return; // already here
                    }
                    if state.data_mode.get_untracked() == DataMode::Live {
                        post_departure_to_gateway(state, 0, dest_idx);
                    } else {
                        schedule_departure(state, 0, dest_idx);
                    }
                }
            }
            canvas_map::CanvasHit::None => {}
        }
    };

    view! {
        <div class="map-canvas" on:click=on_canvas_click>
            <canvas
                node_ref=canvas_ref
                style="position:absolute;inset:0;width:100%;height:100%;"
            />

            <Show when=move || {
                let sel = selected.get();
                sel.is_some()
            }>
                {move || {
                    let sel_idx = selected.get().unwrap_or(0);
                    view! { <ShipPopup state=state ship_idx=sel_idx /> }
                }}
            </Show>

            <OversightStrip oversight=oversight />
        </div>
    }
}

#[component]
fn ShipPopup(state: AppState, ship_idx: usize) -> impl IntoView {
    let ships = state.ships;
    let stations = state.stations;

    let ship_data = move || ships.with(|s| s.get(ship_idx).cloned());

    let popup_style = move || {
        ships.with(|s| {
            s.get(ship_idx)
                .map(|ship| {
                    // Use canvas pixel positions for popup placement
                    let sx = ship.canvas_x.unwrap_or(ship.left_pct / 100.0 * 800.0);
                    let sy = ship.canvas_y.unwrap_or(ship.top_pct / 100.0 * 500.0);
                    let left = sx + 22.0;
                    let top = (sy - 20.0).max(8.0);
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
                match data {
                    Some(ship) => {
                        let at_station = ship
                            .current_station_idx
                            .and_then(|i| st.get(i).map(|s| s.name.clone()));
                        let status_detail = match ship.status {
                            ShipStatus::Transit => "IN TRANSIT".to_string(),
                            ShipStatus::Dead => "DECOMMISSIONED".to_string(),
                            ShipStatus::Docked => match at_station {
                                Some(name) => format!("DOCKED at {}", name),
                                None => "IDLE".to_string(),
                            },
                        };
                        let fuel_display = format!("{:.0}%", ship.fuel_pct);
                        let version_display = format!("v{}", ship.version);
                        let snap_pct = if ship.snapshot_every > 0 {
                            ((ship.events_since_snapshot as f64) / (ship.snapshot_every as f64)
                                * 100.0)
                                .min(100.0)
                        } else {
                            0.0
                        };
                        let snap_fill_style = format!("width: {}%;", snap_pct);
                        let snap_label = format!(
                            "{}/{} events since snapshot",
                            ship.events_since_snapshot, ship.snapshot_every
                        );
                        let fuel_class = if ship.fuel_pct < 30.0 { "pi-v a" } else { "pi-v g" };
                        view! {
                            <div>
                                <div class="ship-popup-name">{ship.name.clone()}</div>
                                <div class="ship-popup-status">{status_detail}</div>
                                <div class="ship-popup-hint">"Select destination:"</div>
                                <div class="ship-popup-destinations">
                                    {st
                                        .iter()
                                        .enumerate()
                                        .map(|(si, station)| {
                                            let is_current = ship.current_station_idx == Some(si);
                                            let is_in_transit = ship.status == ShipStatus::Transit;
                                            let disabled = is_current || is_in_transit;
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
                                                        if state_btn.data_mode.get_untracked() == DataMode::Live {
                                                            post_departure_to_gateway(state_btn, sidx, dest);
                                                        } else {
                                                            schedule_departure(state_btn, sidx, dest);
                                                        }
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
                                        <div class="snapshot-bar-container">
                                            <div class="snapshot-bar-fill" style=snap_fill_style></div>
                                            <div class="snapshot-bar-marker" style="left: 0%;"></div>
                                        </div>
                                        <div class="snapshot-bar-label">{snap_label}</div>
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
            <div>
                <div class="oversight-handler-id">
                    {move || oversight.get().handler_id.clone()}
                </div>
                <div class="oversight-gate-title">
                    {move || oversight.get().gate_title.clone()}
                </div>
            </div>
            <div class="oversight-reqs">
                <div class=arrival_class>
                    <span class="req-icon">{arrival_icon}</span>
                    <span class="req-label">"ShipArrivedAtStation"</span>
                </div>
                <div class=manifest_class>
                    <span class="req-icon">{manifest_icon}</span>
                    <span class="req-label">"ManifestCreated"</span>
                </div>
            </div>
            <span class=badge_class>{badge_text}</span>
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
                                        if state_click.data_mode.get_untracked() == DataMode::Live {
                                            post_departure_to_gateway(state_click, 0, dest_idx);
                                        } else {
                                            schedule_departure(state_click, 0, dest_idx);
                                        }
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

/// Ship action bar — contextual row that changes based on ship/cargo/game state.
#[component]
fn ShipActionBar(state: AppState) -> impl IntoView {
    let ships = state.ships;
    let stations = state.stations;
    let cargo = state.cargo;
    let game_over = state.game_over;

    view! {
        <div class="ship-action-bar">
            {move || {
                // Game over state
                if game_over.get() {
                    return view! {
                        <div class="action-bar-content game-over">
                            <span class="action-bar-text game-over-text">
                                "Supply chain collapsed"
                            </span>
                            <button
                                class="action-bar-btn restart-btn"
                                on:click=move |_| restart_game(state)
                            >
                                "Restart"
                            </button>
                        </div>
                    }
                        .into_any();
                }

                let ship = ships.with(|s| s.first().cloned());
                let st = stations.get();
                let current_cargo = cargo.get();

                match ship {
                    None => {
                        view! {
                            <div class="action-bar-content">
                                <span class="action-bar-text">"No ship available"</span>
                            </div>
                        }
                            .into_any()
                    }
                    Some(ship) => {
                        let is_transit = ship.status == ShipStatus::Transit;
                        let current_idx = ship.current_station_idx;
                        let current_name = current_idx
                            .and_then(|i| st.get(i).map(|s| s.name.clone()));

                        if is_transit {
                            // In transit
                            match current_cargo {
                                Some(cargo_load) => {
                                    let dest_name = st
                                        .get(cargo_load.destination_idx)
                                        .map(|s| s.name.clone())
                                        .unwrap_or_default();
                                    view! {
                                        <div class="action-bar-content">
                                            <span class="action-bar-text carrying">
                                                {format!(
                                                    "Carrying supplies \u{2014} fly to {} to deliver",
                                                    dest_name,
                                                )}
                                            </span>
                                        </div>
                                    }
                                        .into_any()
                                }
                                None => {
                                    view! {
                                        <div class="action-bar-content">
                                            <span class="action-bar-text">
                                                "Click a planet to fly there"
                                            </span>
                                        </div>
                                    }
                                        .into_any()
                                }
                            }
                        } else {
                            // Docked
                            match (current_idx, current_name.clone()) {
                                (Some(idx), Some(name)) => {
                                    match current_cargo {
                                        Some(cargo_load) => {
                                            if cargo_load.destination_idx == idx {
                                                // Right station — deliver
                                                let state_deliver = state;
                                                view! {
                                                    <div class="action-bar-content">
                                                        <span class="action-bar-text">
                                                            {format!("Docked at {}", name)}
                                                        </span>
                                                        <button
                                                            class="action-bar-btn deliver-btn"
                                                            on:click=move |_| {
                                                                deliver_cargo(state_deliver);
                                                            }
                                                        >
                                                            "Deliver supplies here"
                                                        </button>
                                                    </div>
                                                }
                                                    .into_any()
                                            } else {
                                                // Wrong station
                                                let dest_name = st
                                                    .get(cargo_load.destination_idx)
                                                    .map(|s| s.name.clone())
                                                    .unwrap_or_default();
                                                view! {
                                                    <div class="action-bar-content">
                                                        <span class="action-bar-text wrong-dest">
                                                            {format!(
                                                                "Supplies are for {} \u{2014} fly there to deliver",
                                                                dest_name,
                                                            )}
                                                        </span>
                                                    </div>
                                                }
                                                    .into_any()
                                            }
                                        }
                                        None => {
                                            // Docked, no cargo — offer to load
                                            let dest_idx = supply_destination(idx);
                                            let dest_name = dest_idx
                                                .and_then(|d| st.get(d).map(|s| s.name.clone()));
                                            let state_load = state;
                                            match dest_name {
                                                Some(dn) => {
                                                    view! {
                                                        <div class="action-bar-content">
                                                            <span class="action-bar-text">
                                                                {format!("Docked at {}", name)}
                                                            </span>
                                                            <button
                                                                class="action-bar-btn load-btn"
                                                                on:click=move |_| {
                                                                    load_cargo(state_load);
                                                                }
                                                            >
                                                                {format!("Load supplies for {}", dn)}
                                                            </button>
                                                        </div>
                                                    }
                                                        .into_any()
                                                }
                                                None => {
                                                    view! {
                                                        <div class="action-bar-content">
                                                            <span class="action-bar-text">
                                                                {format!("Docked at {}", name)}
                                                            </span>
                                                        </div>
                                                    }
                                                        .into_any()
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    // Not docked at any station
                                    view! {
                                        <div class="action-bar-content">
                                            <span class="action-bar-text">
                                                "Click a planet to fly there"
                                            </span>
                                        </div>
                                    }
                                        .into_any()
                                }
                            }
                        }
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
