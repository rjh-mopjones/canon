//! Fetch initial state from gateway REST endpoints.
//!
//! On mount, attempts to hydrate ships, stations, oversight windows, and dead
//! letters from the gateway. Falls back silently to demo defaults when the
//! gateway is unavailable.

use leptos::prelude::*;
use serde::Deserialize;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;

use crate::gateway::gateway_base_url;
use crate::state::{
    AppState, DeadLetterEntry, OversightReqStatus, OversightState, ShipState, ShipStatus,
    StationDef, STOCK_LOW_THRESHOLD,
};

// ---------------------------------------------------------------------------
// Gateway response types (match gateway/src/types.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ShipStateResponse {
    id: Uuid,
    name: String,
    status: String,
    #[serde(default)]
    station_id: Option<Uuid>,
    fuel_pct: u32,
    aggregate_version: u64,
    last_snapshot_version: u64,
}

#[derive(Debug, Deserialize)]
struct StationStateResponse {
    id: Uuid,
    name: String,
    capacity_kg: f32,
    current_stock_kg: f32,
}

#[derive(Debug, Deserialize)]
struct OversightWindowResponse {
    #[allow(dead_code)]
    window_id: Uuid,
    handler_id: String,
    #[allow(dead_code)]
    correlation_key: Uuid,
    #[allow(dead_code)]
    ship_name: String,
    #[allow(dead_code)]
    dest_label: String,
    status: String,
    requirements: Vec<RequirementResponse>,
    #[allow(dead_code)]
    ttl_remaining_secs: u32,
    #[allow(dead_code)]
    ttl_total_secs: u32,
}

#[derive(Debug, Deserialize)]
struct RequirementResponse {
    label: String,
    met: bool,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Fetch initial state from gateway REST endpoints, filtered by session.
///
/// Hydrates ships, stations, oversight windows, and dead letters from
/// the gateway. All requests include `?session_id=` so the gateway
/// returns only entities belonging to this session.
pub fn hydrate_from_gateway(state: AppState, session_id: Uuid) {
    let base = gateway_base_url();
    let sid = session_id.to_string();

    // Hydrate stations before ships so that if the ship is already
    // docked (e.g. page reload), the station_id→position lookup works.
    // Fresh ships start in the center by design — user chooses where to fly.
    hydrate_stations_then_ships(state, base.clone(), sid.clone(), base.clone(), sid.clone());
    hydrate_oversight(state, base.clone(), sid.clone());
    hydrate_dead_letters(state, base, sid);
}

// ---------------------------------------------------------------------------
// Sequential station → ship hydration
// ---------------------------------------------------------------------------

fn hydrate_stations_then_ships(
    state: AppState,
    base_stations: String,
    sid_stations: String,
    base_ships: String,
    sid_ships: String,
) {
    spawn_local(async move {
        // Hydrate stations first (blocking) so ship position lookup works.
        do_hydrate_stations(state, &base_stations, &sid_stations).await;
        // Now hydrate ships — stations signal is populated.
        do_hydrate_ships(state, &base_ships, &sid_ships).await;
    });
}

// ---------------------------------------------------------------------------
// Individual hydration requests
// ---------------------------------------------------------------------------

async fn do_hydrate_ships(state: AppState, base: &str, sid: &str) {
    {
        let url = format!("{base}/ships?session_id={sid}");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return, // gateway unavailable -- keep demo defaults
        };

        if !resp.ok() {
            return;
        }

        let ships: Vec<ShipStateResponse> = match resp.json().await {
            Ok(s) => s,
            Err(_) => return,
        };

        if ships.is_empty() {
            return; // no ships registered yet -- keep demo defaults
        }

        // Station positions for layout (we keep the canonical positions)
        let station_positions = default_station_positions();

        let mapped: Vec<ShipState> = ships
            .into_iter()
            .map(|s| {
                let status = match s.status.as_str() {
                    "docked" | "Docked" => ShipStatus::Docked,
                    "transit" | "Transit" => ShipStatus::Transit,
                    "dead" | "Dead" => ShipStatus::Dead,
                    _ => ShipStatus::Docked,
                };

                // Try to find which station this ship is at so we can position it
                let station_idx = s.station_id.and_then(|sid| {
                    state
                        .stations
                        .with_untracked(|stations| stations.iter().position(|st| st.id == sid))
                });

                let (left, top) = station_idx
                    .and_then(|i| station_positions.get(i))
                    .copied()
                    .unwrap_or((48.0, 44.0)); // default to center if unknown

                let snapshot_every = 50u32;
                let events_since = (s.aggregate_version.saturating_sub(s.last_snapshot_version)
                    as u32)
                    % snapshot_every;

                ShipState {
                    id: s.id,
                    name: s.name,
                    status,
                    fuel_pct: s.fuel_pct as f32,
                    version: s.aggregate_version,
                    events_since_snapshot: events_since,
                    snapshot_every,
                    current_station_idx: station_idx,
                    destination_station_idx: None,
                    left_pct: left,
                    top_pct: top,
                    canvas_x: None,
                    canvas_y: None,
                    from_pct_x: None,
                    from_pct_y: None,
                    flight_start_ms: None,
                    flight_duration_ms: None,
                }
            })
            .collect();

        state.ships.set(mapped);
    }
}

async fn do_hydrate_stations(state: AppState, base: &str, sid: &str) {
    {
        let url = format!("{base}/stations?session_id={sid}");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return,
        };

        if !resp.ok() {
            return;
        }

        let mut stations: Vec<StationStateResponse> = match resp.json().await {
            Ok(s) => s,
            Err(_) => return,
        };

        if stations.is_empty() {
            return; // keep demo defaults
        }

        // Sort to match canonical order: Alpha, Beta, Gamma, Delta.
        // This ensures positions, planet colours, and supply-chain links
        // are assigned correctly regardless of API return order.
        let canonical_order = ["Alpha Depot", "Beta Relay", "Gamma Outpost", "Delta Prime"];
        stations.sort_by_key(|s| {
            canonical_order
                .iter()
                .position(|&name| name == s.name)
                .unwrap_or(usize::MAX)
        });

        // Map gateway stations onto canonical positions.
        let positions = default_station_positions();
        // Canvas rendering properties keyed by station index
        let planet_color_vars = [
            "--planet-green",
            "--planet-purple",
            "--planet-coral",
            "--planet-blue",
        ];
        let planet_radii = [32.0, 22.0, 28.0, 20.0];
        let has_rings = [false, true, false, false];
        let supplied_by_names = ["Delta Prime", "Alpha Depot", "Beta Relay", "Gamma Outpost"];

        let mapped: Vec<StationDef> = stations
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let (left, top) = positions.get(i).copied().unwrap_or((50.0, 50.0));
                // Stock comes from the pipeline — the gateway bootstrap task
                // seeds initial stock via RecordCargoReceived commands.
                let stock_pct: f64 = if s.capacity_kg > 0.0 {
                    (s.current_stock_kg as f64 / s.capacity_kg as f64 * 100.0).clamp(0.0, 100.0)
                } else {
                    0.0
                };
                let stock_low = stock_pct < STOCK_LOW_THRESHOLD;
                StationDef {
                    id: s.id,
                    name: s.name,
                    left_pct: left,
                    top_pct: top,
                    stock_low,
                    stock_pct,
                    planet_color_var: planet_color_vars.get(i).unwrap_or(&"--accent").to_string(),
                    planet_radius: planet_radii.get(i).copied().unwrap_or(20.0),
                    has_ring: has_rings.get(i).copied().unwrap_or(false),
                    capacity_kg: s.capacity_kg as f64,
                    supplied_by_name: supplied_by_names.get(i).unwrap_or(&"").to_string(),
                }
            })
            .collect();

        state.stations.set(mapped);
    }
}

fn hydrate_oversight(state: AppState, base: String, sid: String) {
    spawn_local(async move {
        let url = format!("{base}/admin/oversight/windows?session_id={sid}");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return,
        };

        if !resp.ok() {
            return;
        }

        let windows: Vec<OversightWindowResponse> = match resp.json().await {
            Ok(w) => w,
            Err(_) => return,
        };

        // Show the first pending window in the oversight strip
        if let Some(window) = windows.into_iter().find(|w| w.status == "pending") {
            let arrival = window
                .requirements
                .iter()
                .find(|r| r.label == "ShipArrivedAtStation")
                .map(|r| {
                    if r.met {
                        OversightReqStatus::Met
                    } else {
                        OversightReqStatus::Pending
                    }
                })
                .unwrap_or(OversightReqStatus::Pending);

            let manifest = window
                .requirements
                .iter()
                .find(|r| r.label == "ManifestCreated")
                .map(|r| {
                    if r.met {
                        OversightReqStatus::Met
                    } else {
                        OversightReqStatus::Pending
                    }
                })
                .unwrap_or(OversightReqStatus::Pending);

            state.oversight.set(OversightState {
                visible: true,
                handler_id: window.handler_id,
                gate_title: "Cargo Unloading Gate".into(),
                arrival_status: arrival,
                manifest_status: manifest,
            });
        }
    });
}

fn hydrate_dead_letters(state: AppState, base: String, sid: String) {
    spawn_local(async move {
        let url = format!("{base}/admin/deadletters?session_id={sid}");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return,
        };

        if !resp.ok() {
            return;
        }

        let entries: Vec<DeadLetterEntry> = match resp.json().await {
            Ok(e) => e,
            Err(_) => return,
        };

        state.dead_letters.set(entries);
    });
}

// ---------------------------------------------------------------------------
// Station position constants (canonical layout)
// ---------------------------------------------------------------------------

fn default_station_positions() -> Vec<(f64, f64)> {
    vec![
        (18.0, 26.0), // Alpha Depot
        (68.0, 14.0), // Beta Relay
        (76.0, 68.0), // Gamma Outpost
        (24.0, 74.0), // Delta Prime
    ]
}
