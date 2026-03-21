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
    AppState, DataMode, LogEntry, OversightReqStatus, OversightState, ShipState, ShipStatus,
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

fn pseudo_random_u32(seed: u32) -> u32 {
    // Simple xorshift for WASM (no rand crate needed)
    let mut x = seed.wrapping_add(1);
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    x
}

fn random_station_idx_excluding(exclude: usize, seed: u32) -> usize {
    let r = pseudo_random_u32(seed) % 3;
    let mut idx = r as usize;
    if idx >= exclude {
        idx += 1;
    }
    idx
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
// Flight scheduling
// ---------------------------------------------------------------------------

fn schedule_departure(state: AppState, ship_idx: usize, dest_idx: usize) {
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

    // On arrival (after transit duration), dock the ship and schedule next departure
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

        // Wait 4000-9000ms then depart again
        let seed = (ship_idx as u32)
            .wrapping_mul(17)
            .wrapping_add(js_sys::Date::now() as u32);
        let wait = 4000 + (pseudo_random_u32(seed) % 5001);
        let next_dest = random_station_idx_excluding(dest_idx, seed.wrapping_add(3));

        let state_next = state_arrive;
        let _ = gloo_timers::callback::Timeout::new(wait, move || {
            schedule_departure(state_next, ship_idx, next_dest);
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

fn start_autonomous_loop(state: AppState) {
    // Stagger departures 1800ms apart for ships 0-3
    for i in 0..4usize {
        let state_dep = state;
        let delay = (i as u32) * 1800;
        let _ = gloo_timers::callback::Timeout::new(delay, move || {
            let current = state_dep
                .ships
                .with_untracked(|ships| ships.get(i).and_then(|s| s.current_station_idx));
            if let Some(cur) = current {
                let seed = (i as u32)
                    .wrapping_mul(31)
                    .wrapping_add(js_sys::Date::now() as u32);
                let dest = random_station_idx_excluding(cur, seed);
                schedule_departure(state_dep, i, dest);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

#[component]
pub fn LiveFleetPage(state: AppState) -> impl IntoView {
    // Start autonomous flight loop on mount (guarded to prevent duplicates on tab switch)
    let state_init = state;
    Effect::new(move |_| {
        if !state_init.loop_started.get_untracked() {
            state_init.loop_started.set(true);
            start_autonomous_loop(state_init);
        }
    });

    view! {
        <div class="content-area">
            <div class="map-wrap">
                <MapBar ships=state.ships />
                <MapCanvas state=state />
            </div>
            <Sidebar state=state />
        </div>
    }
}

#[component]
fn MapBar(ships: RwSignal<Vec<ShipState>>) -> impl IntoView {
    let docked_count = move || {
        ships.with(|s| {
            s.iter()
                .filter(|sh| sh.status == ShipStatus::Docked)
                .count()
        })
    };
    let transit_count = move || {
        ships.with(|s| {
            s.iter()
                .filter(|sh| sh.status == ShipStatus::Transit)
                .count()
        })
    };
    let offline_count =
        move || ships.with(|s| s.iter().filter(|sh| sh.status == ShipStatus::Dead).count());

    view! {
        <div class="map-bar">
            <div class="bar-lbl">"Active Fleet"</div>
            <div class="pills">
                <span class="pill pg">
                    {move || format!("\u{25B2} Docked {}", docked_count())}
                </span>
                <span class="pill pc">
                    {move || format!("\u{25B6} Transit {}", transit_count())}
                </span>
                <span class="pill pr">
                    {move || format!("\u{2715} Offline {}", offline_count())}
                </span>
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
            let canvas: web_sys::HtmlCanvasElement = canvas_el.into();

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
            let f: Rc<std::cell::RefCell<Option<Closure<dyn FnMut()>>>> =
                Rc::new(std::cell::RefCell::new(None));
            let g = Rc::clone(&f);

            let state_draw = state;
            *g.borrow_mut() = Some(Closure::new(move || {
                let canvas_el: web_sys::HtmlCanvasElement = match canvas_ref.get() {
                    Some(el) => el.into(),
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

                // Write back canvas_x/y so popup placement and hit testing work
                state_draw.ships.update(|ships| {
                    for (i, ship) in ships.iter_mut().enumerate() {
                        if let Some(drawn) = ships_data.get(i) {
                            ship.canvas_x = drawn.canvas_x;
                            ship.canvas_y = drawn.canvas_y;
                        }
                    }
                });

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
        let canvas: web_sys::HtmlCanvasElement = canvas_el.into();
        let rect = canvas.get_bounding_client_rect();
        let w = canvas.width() as f64;
        let h = canvas.height() as f64;
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
                let is_dead = ships
                    .get(idx)
                    .map(|s| s.status == ShipStatus::Dead)
                    .unwrap_or(true);
                if !is_dead {
                    selected.set(Some(idx));
                }
            }
            canvas_map::CanvasHit::Station(dest_idx) => {
                // Find the first non-dead, non-transit ship and fly it there
                let ships_data = state.ships.get_untracked();
                // Find ship 0 (Meridian) specifically for the single-ship game mode
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
                        let status_str = match ship.status {
                            ShipStatus::Docked => "DOCKED",
                            ShipStatus::Transit => "IN TRANSIT",
                            ShipStatus::Dead => "DECOMMISSIONED",
                        };
                        let at_station = ship
                            .current_station_idx
                            .and_then(|i| st.get(i).map(|s| s.name.clone()));
                        let status_detail = match at_station {
                            Some(name) => format!("{} at {}", status_str, name),
                            None => status_str.to_string(),
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

#[component]
fn Sidebar(state: AppState) -> impl IntoView {
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
        <div class="sidebar">
            <div class="sidebar-header">
                <span class="pulse-dot"></span>
                "Live Activity"
            </div>
            <div class="event-log">
                <For
                    each=move || entries.get()
                    key=|entry| entry.id
                    children=move |entry| {
                        view! { <LogEntryRow entry=entry highlighted=highlighted /> }
                    }
                />
            </div>
            <div class="sidebar-footer">
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
        let mut cls = "log-entry".to_string();
        if entry.is_new {
            cls.push_str(" flash");
        }
        if is_highlighted() {
            cls.push_str(" corr-highlight");
        }
        cls
    };

    let badge_class = match entry.service.as_str() {
        "fleet" => "service-badge fleet",
        "cargo" => "service-badge cargo",
        "nav" => "service-badge nav",
        "supply" => "service-badge supply",
        "station" => "service-badge station",
        _ => "service-badge",
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
            <div class="log-entry-meta">
                <span>{entry.timestamp.clone()}</span>
                <span>{format!("v{}", entry.version)}</span>
            </div>
            <div class="log-entry-body">
                <span class=badge_class>{entry.service.clone()}</span>
                <span class="log-event-name">{entry.event_name.clone()}</span>
            </div>
            <div class="log-agg-id">{agg_short}</div>
        </div>
    }
}
