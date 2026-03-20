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
    AppState, DataMode, DeadLetterEntry, OversightReqStatus, OversightState, ShipState, ShipStatus,
    StationDef,
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

/// Fetch initial state from gateway REST endpoints.
/// Falls back silently when the gateway is unavailable (demo mode uses defaults).
pub fn hydrate_from_gateway(state: AppState) {
    let base = gateway_base_url();

    // Fire all four hydration requests concurrently.
    hydrate_ships(state, base.clone());
    hydrate_stations(state, base.clone());
    hydrate_oversight(state, base.clone());
    hydrate_dead_letters(state, base);
}

// ---------------------------------------------------------------------------
// Individual hydration requests
// ---------------------------------------------------------------------------

fn hydrate_ships(state: AppState, base: String) {
    spawn_local(async move {
        let url = format!("{base}/ships");
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
                let events_since =
                    ((s.aggregate_version - s.last_snapshot_version) as u32) % snapshot_every;

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
                }
            })
            .collect();

        state.ships.set(mapped);
        state.data_mode.set(DataMode::Live);
    });
}

fn hydrate_stations(state: AppState, base: String) {
    spawn_local(async move {
        let url = format!("{base}/stations");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return,
        };

        if !resp.ok() {
            return;
        }

        let stations: Vec<StationStateResponse> = match resp.json().await {
            Ok(s) => s,
            Err(_) => return,
        };

        if stations.is_empty() {
            return; // keep demo defaults
        }

        // Map gateway stations onto canonical positions.
        let positions = default_station_positions();
        let mapped: Vec<StationDef> = stations
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                let (left, top) = positions.get(i).copied().unwrap_or((50.0, 50.0));
                let stock_low = s.current_stock_kg < (s.capacity_kg * 0.2);
                StationDef {
                    id: s.id,
                    name: s.name,
                    left_pct: left,
                    top_pct: top,
                    stock_low,
                }
            })
            .collect();

        state.stations.set(mapped);
    });
}

fn hydrate_oversight(state: AppState, base: String) {
    spawn_local(async move {
        let url = format!("{base}/admin/oversight/windows");
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

fn hydrate_dead_letters(state: AppState, base: String) {
    spawn_local(async move {
        let url = format!("{base}/admin/deadletters");
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
