//! Fetch initial state from the gateway snapshot endpoint.
//!
//! On mount, attempts to hydrate the full session snapshot from the gateway.
//! Falls back silently to demo defaults when the gateway is unavailable.

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
    #[allow(dead_code)]
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

#[derive(Debug, Deserialize)]
struct GameStateResponse {
    ship: Option<ShipStateResponse>,
    stations: Vec<StationStateResponse>,
    cargo: Option<GameCargoResponse>,
    oversight: Option<OversightWindowResponse>,
    dead_letters: Vec<DeadLetterEntry>,
    events: Vec<GameEventResponse>,
}

#[derive(Debug, Deserialize)]
struct GameCargoResponse {
    manifest_id: Uuid,
    #[allow(dead_code)]
    voyage_id: Uuid,
    destination_station_id: Option<Uuid>,
    amount_pct: u32,
    #[allow(dead_code)]
    status: String,
}

#[derive(Debug, Deserialize)]
struct GameEventResponse {
    id: Uuid,
    timestamp: String,
    version: u64,
    service: String,
    event_name: String,
    aggregate_id: Uuid,
    correlation_id: Uuid,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Fetch initial session state from the gateway snapshot endpoint.
pub fn hydrate_from_gateway(state: AppState, session_id: Uuid) {
    let base = gateway_base_url();
    spawn_local(async move {
        let url = format!("{base}/game/{session_id}");
        let resp = match gloo_net::http::Request::get(&url).send().await {
            Ok(r) => r,
            Err(_) => return,
        };

        if !resp.ok() {
            return;
        }

        let snapshot: GameStateResponse = match resp.json().await {
            Ok(snapshot) => snapshot,
            Err(_) => return,
        };

        apply_snapshot(state, snapshot);
    });
}

// ---------------------------------------------------------------------------
// Snapshot mapping
// ---------------------------------------------------------------------------

fn apply_snapshot(state: AppState, snapshot: GameStateResponse) {
    let mapped_stations = map_stations(snapshot.stations);
    let mapped_ship = snapshot
        .ship
        .map(|ship| map_ship(ship, &mapped_stations))
        .into_iter()
        .collect::<Vec<_>>();
    let mapped_cargo = snapshot
        .cargo
        .and_then(|cargo| map_cargo(cargo, &mapped_stations));

    state.stations.set(mapped_stations);
    state.ships.set(mapped_ship);
    state.cargo.set(mapped_cargo.clone());
    state
        .last_manifest_id
        .set(mapped_cargo.as_ref().and_then(|cargo| cargo.manifest_id));
    state.dead_letters.set(snapshot.dead_letters);
    state.log_entries.set(
        snapshot
            .events
            .into_iter()
            .map(|event| crate::state::LogEntry {
                id: event.id,
                timestamp: event.timestamp,
                version: event.version,
                service: event.service,
                event_name: event.event_name,
                aggregate_id: event.aggregate_id,
                correlation_id: event.correlation_id,
                is_new: false,
            })
            .collect(),
    );

    if let Some(window) = snapshot.oversight {
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
    } else {
        state.oversight.set(OversightState::default());
    }
}

fn map_cargo(cargo: GameCargoResponse, stations: &[StationDef]) -> Option<crate::state::CargoLoad> {
    let destination_idx = cargo
        .destination_station_id
        .and_then(|destination_station_id| {
            stations
                .iter()
                .position(|station| station.id == destination_station_id)
        })?;

    Some(crate::state::CargoLoad {
        destination_idx,
        amount_pct: cargo.amount_pct,
        manifest_id: Some(cargo.manifest_id),
    })
}

fn map_ship(ship: ShipStateResponse, stations: &[StationDef]) -> ShipState {
    let status = match ship.status.as_str() {
        "docked" | "Docked" => ShipStatus::Docked,
        "transit" | "Transit" => ShipStatus::Transit,
        "dead" | "Dead" => ShipStatus::Dead,
        _ => ShipStatus::Docked,
    };

    let station_positions = default_station_positions();
    let station_idx = ship
        .station_id
        .and_then(|sid| stations.iter().position(|station| station.id == sid));
    let (left, top) = station_idx
        .and_then(|idx| station_positions.get(idx))
        .copied()
        .unwrap_or((48.0, 44.0));

    let snapshot_every = 50u32;
    let events_since = (ship
        .aggregate_version
        .saturating_sub(ship.last_snapshot_version) as u32)
        % snapshot_every;

    ShipState {
        id: ship.id,
        name: ship.name,
        status,
        fuel_pct: ship.fuel_pct as f32,
        version: ship.aggregate_version,
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
}

fn map_stations(mut stations: Vec<StationStateResponse>) -> Vec<StationDef> {
    if stations.is_empty() {
        return Vec::new();
    }

    let canonical_order = ["Alpha Depot", "Beta Relay", "Gamma Outpost", "Delta Prime"];
    stations.sort_by_key(|station| {
        canonical_order
            .iter()
            .position(|&name| name == station.name)
            .unwrap_or(usize::MAX)
    });

    let positions = default_station_positions();
    let planet_color_vars = [
        "--planet-green",
        "--planet-purple",
        "--planet-coral",
        "--planet-blue",
    ];
    let planet_radii = [32.0, 22.0, 28.0, 20.0];
    let has_rings = [false, true, false, false];
    let supplied_by_names = ["Delta Prime", "Alpha Depot", "Beta Relay", "Gamma Outpost"];

    stations
        .into_iter()
        .enumerate()
        .map(|(i, station)| {
            let (left, top) = positions.get(i).copied().unwrap_or((50.0, 50.0));
            let stock_pct = if station.capacity_kg > 0.0 {
                (station.current_stock_kg as f64 / station.capacity_kg as f64 * 100.0)
                    .clamp(0.0, 100.0)
            } else {
                0.0
            };

            StationDef {
                id: station.id,
                name: station.name,
                left_pct: left,
                top_pct: top,
                stock_low: stock_pct < STOCK_LOW_THRESHOLD,
                stock_pct,
                planet_color_var: planet_color_vars.get(i).unwrap_or(&"--accent").to_string(),
                planet_radius: planet_radii.get(i).copied().unwrap_or(20.0),
                has_ring: has_rings.get(i).copied().unwrap_or(false),
                capacity_kg: station.capacity_kg as f64,
                supplied_by_name: supplied_by_names.get(i).unwrap_or(&"").to_string(),
            }
        })
        .collect()
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
