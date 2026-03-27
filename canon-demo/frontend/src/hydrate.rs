//! Session snapshot fetch + application.

use leptos::prelude::*;
use serde::Deserialize;
use uuid::Uuid;
use wasm_bindgen_futures::spawn_local;

use crate::gateway::gateway_base_url;
use crate::state::{
    AppState, CargoLoad, InfraStatus, LogEntry, OversightReqStatus, OversightState, ShipState,
    ShipStatus, StationDef, STOCK_LOW_THRESHOLD,
};

const SNAPSHOT_EVERY: u32 = 50;
const TRANSIT_DURATION_MS: f64 = 4200.0;

#[derive(Debug, Clone, Deserialize)]
pub struct GameStateResponse {
    pub ship: Option<ShipStateResponse>,
    pub stations: Vec<StationStateResponse>,
    pub cargo: Option<GameCargoResponse>,
    pub oversight: Option<OversightWindowResponse>,
    pub events: Vec<GameEventResponse>,
    pub game_over: bool,
    pub infra: InfraStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShipStateResponse {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    #[serde(default)]
    pub station_id: Option<Uuid>,
    pub fuel_pct: u32,
    pub aggregate_version: u64,
    pub last_snapshot_version: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StationStateResponse {
    pub id: Uuid,
    pub name: String,
    pub stock_pct: f64,
    pub capacity_kg: f32,
    pub stock_low: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GameCargoResponse {
    pub manifest_id: Uuid,
    #[allow(dead_code)]
    pub voyage_id: Uuid,
    #[serde(default)]
    pub destination_station_id: Option<Uuid>,
    pub amount_pct: u32,
    #[allow(dead_code)]
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OversightWindowResponse {
    #[allow(dead_code)]
    pub window_id: Uuid,
    pub handler_id: String,
    #[allow(dead_code)]
    pub correlation_key: Uuid,
    #[allow(dead_code)]
    pub ship_name: String,
    #[allow(dead_code)]
    pub dest_label: String,
    #[allow(dead_code)]
    pub status: String,
    pub requirements: Vec<RequirementResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequirementResponse {
    pub label: String,
    pub met: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GameEventResponse {
    pub id: Uuid,
    pub timestamp: String,
    pub version: u64,
    pub service: String,
    pub event_name: String,
    pub aggregate_id: Uuid,
    pub correlation_id: Uuid,
}

pub fn hydrate_from_gateway(state: AppState, session_id: Uuid) {
    spawn_local(async move {
        if let Some(snapshot) = fetch_game_state(session_id).await {
            apply_snapshot(state, snapshot);
        }
    });
}

pub async fn fetch_game_state(session_id: Uuid) -> Option<GameStateResponse> {
    let url = format!("{}/game/{}", gateway_base_url(), session_id);
    let resp = gloo_net::http::Request::get(&url).send().await.ok()?;
    if !resp.ok() {
        return None;
    }
    resp.json().await.ok()
}

pub fn apply_snapshot(state: AppState, snapshot: GameStateResponse) {
    let prev_ship = state.ships.with_untracked(|ships| ships.first().cloned());
    let prev_latest_event_id = state
        .log_entries
        .with_untracked(|entries| entries.first().map(|entry| entry.id));
    let mapped_stations = map_stations(snapshot.stations);
    let mapped_ship = snapshot
        .ship
        .map(|ship| map_ship(ship, &mapped_stations, prev_ship.as_ref()))
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
    state.log_entries.set(
        snapshot
            .events
            .into_iter()
            .map(|event| LogEntry {
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
    state.infra.set(snapshot.infra);
    state.game_over.set(snapshot.game_over);

    let latest_event_changed = state
        .log_entries
        .with_untracked(|entries| entries.first().map(|entry| entry.id))
        != prev_latest_event_id;
    if latest_event_changed {
        state
            .pending_command
            .set(crate::state::PendingCommand::None);
        state.command_error.set(None);
    }

    state.oversight.set(map_oversight(snapshot.oversight));
}

fn map_oversight(oversight: Option<OversightWindowResponse>) -> OversightState {
    let Some(window) = oversight else {
        return OversightState::default();
    };

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

    OversightState {
        visible: true,
        handler_id: window.handler_id,
        gate_title: "Cargo Unloading Gate".into(),
        arrival_status: arrival,
        manifest_status: manifest,
    }
}

fn map_cargo(cargo: GameCargoResponse, stations: &[StationDef]) -> Option<CargoLoad> {
    let destination_idx = cargo
        .destination_station_id
        .and_then(|destination_station_id| {
            stations
                .iter()
                .position(|station| station.id == destination_station_id)
        })?;

    Some(CargoLoad {
        destination_idx,
        amount_pct: cargo.amount_pct,
        manifest_id: Some(cargo.manifest_id),
    })
}

fn map_ship(
    ship: ShipStateResponse,
    stations: &[StationDef],
    prev_ship: Option<&ShipState>,
) -> ShipState {
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
        .unwrap_or((50.0, 50.0));

    let mut mapped = ShipState {
        id: ship.id,
        name: ship.name,
        status,
        fuel_pct: ship.fuel_pct as f32,
        version: ship.aggregate_version,
        events_since_snapshot: (ship
            .aggregate_version
            .saturating_sub(ship.last_snapshot_version) as u32)
            % SNAPSHOT_EVERY,
        snapshot_every: SNAPSHOT_EVERY,
        current_station_idx: if status == ShipStatus::Transit {
            None
        } else {
            station_idx
        },
        destination_station_idx: if status == ShipStatus::Transit {
            station_idx
        } else {
            None
        },
        left_pct: left,
        top_pct: top,
        canvas_x: None,
        canvas_y: None,
        from_pct_x: None,
        from_pct_y: None,
        flight_start_ms: None,
        flight_duration_ms: None,
    };

    if let Some(prev) = prev_ship {
        if prev.id == mapped.id {
            match (prev.status, mapped.status) {
                (ShipStatus::Transit, ShipStatus::Transit) => {
                    mapped.left_pct = prev.left_pct;
                    mapped.top_pct = prev.top_pct;
                    mapped.from_pct_x = prev.from_pct_x.or(Some(prev.left_pct));
                    mapped.from_pct_y = prev.from_pct_y.or(Some(prev.top_pct));
                    mapped.flight_start_ms = prev.flight_start_ms;
                    mapped.flight_duration_ms = prev.flight_duration_ms;
                    mapped.canvas_x = prev.canvas_x;
                    mapped.canvas_y = prev.canvas_y;
                    mapped.destination_station_idx = mapped
                        .destination_station_idx
                        .or(prev.destination_station_idx);
                }
                (_, ShipStatus::Transit) => {
                    let now_ms = web_sys::window()
                        .and_then(|w| w.performance())
                        .map(|p| p.now())
                        .unwrap_or(0.0);
                    mapped.left_pct = prev.left_pct;
                    mapped.top_pct = prev.top_pct;
                    mapped.from_pct_x = Some(prev.left_pct);
                    mapped.from_pct_y = Some(prev.top_pct);
                    mapped.flight_start_ms = Some(now_ms);
                    mapped.flight_duration_ms = Some(TRANSIT_DURATION_MS);
                    mapped.canvas_x = None;
                    mapped.canvas_y = None;
                }
                (ShipStatus::Transit, _) => {
                    mapped.left_pct = left;
                    mapped.top_pct = top;
                }
                _ => {}
            }
        }
    }

    mapped
}

fn map_stations(stations: Vec<StationStateResponse>) -> Vec<StationDef> {
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

    let mut stations = stations;
    let canonical_order = ["Alpha Depot", "Beta Relay", "Gamma Outpost", "Delta Prime"];
    stations.sort_by_key(|s| {
        canonical_order
            .iter()
            .position(|&name| name == s.name)
            .unwrap_or(usize::MAX)
    });

    stations
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let (left, top) = positions.get(i).copied().unwrap_or((50.0, 50.0));
            StationDef {
                id: s.id,
                name: s.name,
                left_pct: left,
                top_pct: top,
                stock_low: s.stock_low || s.stock_pct < STOCK_LOW_THRESHOLD,
                stock_pct: s.stock_pct,
                planet_color_var: planet_color_vars.get(i).unwrap_or(&"--accent").to_string(),
                planet_radius: planet_radii.get(i).copied().unwrap_or(20.0),
                has_ring: has_rings.get(i).copied().unwrap_or(false),
                capacity_kg: s.capacity_kg as f64,
                supplied_by_name: supplied_by_names.get(i).unwrap_or(&"").to_string(),
            }
        })
        .collect()
}

fn default_station_positions() -> Vec<(f64, f64)> {
    vec![(18.0, 26.0), (68.0, 14.0), (76.0, 68.0), (24.0, 74.0)]
}
