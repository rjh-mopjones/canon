use std::cell::Cell;
use std::rc::Rc;

use leptos::prelude::*;
use uuid::Uuid;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

use crate::canvas_map;
use crate::gateway::gateway_base_url;
use crate::state::{AppState, DataMode, LogEntry, OversightReqStatus, OversightState, ShipStatus};

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
    // No autonomous flight — ship moves only on user command.

    view! {
        <div class="content-area">
            <div class="map-wrap">
                <MapBar state=state />
                <MapCanvas state=state />
                <StationCards state=state />
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
                    if state.data_mode.get_untracked() == DataMode::Live {
                        post_departure_to_gateway(state, 0, dest_idx);
                    } else {
                        schedule_departure(state, 0, dest_idx);
                    }
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
                                                is_current || is_in_transit || is_dead;
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
                                                        if state_btn.data_mode.get_untracked()
                                                            == DataMode::Live
                                                        {
                                                            post_departure_to_gateway(
                                                                state_btn,
                                                                sidx,
                                                                dest,
                                                            );
                                                        } else {
                                                            schedule_departure(
                                                                state_btn, sidx, dest,
                                                            );
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
