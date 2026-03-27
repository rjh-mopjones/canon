//! GET /game/:session_id -- complete game state snapshot.
//!
//! This snapshot is the source of truth for both the bootstrap HTTP read path
//! and the WebSocket snapshot-push transport.

use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use canon_core::AggregateId;
use canon_event_store::EventStore;
use canon_snapshot_store::SnapshotStore;

use crate::error::GatewayError;
use crate::session::SessionIds;
use crate::state::AppState;
use crate::types::{
    GameCargoResponse, GameEventResponse, GameStateResponse, GameStationResponse,
    InfraStatusResponse, OversightWindowResponse, RequirementResponse, ShipStateResponse,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/game/:session_id", get(game_snapshot))
}

#[derive(Debug)]
pub struct BuiltGameState {
    pub snapshot: GameStateResponse,
    pub tracked_aggregate_ids: HashSet<Uuid>,
}

/// GET /game/:session_id -- return complete game state as one atomic JSON blob.
async fn game_snapshot(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<GameStateResponse>, GatewayError> {
    Ok(Json(build_game_state(&state, session_id).await?.snapshot))
}

pub async fn build_game_state(
    state: &AppState,
    session_id: Uuid,
) -> Result<BuiltGameState, GatewayError> {
    let session_ids = {
        let sessions = state.sessions.read().await;
        sessions
            .get(&session_id)
            .map(|session| session.ids.clone())
            .ok_or_else(|| GatewayError::NotFound(format!("session {session_id} not found")))?
    };

    let ship = load_ship_snapshot(state, session_ids.ship_id).await?;
    let manifest_rows = load_projection_rows(
        state.pool_for_service("cargo"),
        "manifest_read_model",
        Some(("ship_id", session_ids.ship_id)),
    )
    .await?;
    let route_rows = load_projection_rows(
        state.pool_for_service("navigation"),
        "route_read_model",
        Some(("ship_id", session_ids.ship_id)),
    )
    .await?;
    let inventory_ids = load_supply_inventory_ids(state, &session_ids.station_ids).await?;

    let mut tracked_aggregate_ids = session_ids.aggregate_id_set();
    tracked_aggregate_ids.extend(manifest_rows.iter().map(|row| row.aggregate_id));
    tracked_aggregate_ids.extend(route_rows.iter().map(|row| row.aggregate_id));
    tracked_aggregate_ids.extend(inventory_ids.iter().copied());

    let stations = load_station_snapshots(state, session_ids.station_ids).await?;
    let cargo = load_active_cargo_snapshot(&manifest_rows, ship.as_ref(), &session_ids).await?;
    let oversight = query_first_oversight_window(state, &tracked_aggregate_ids).await;
    let events = load_recent_events(
        state,
        session_ids.clone(),
        manifest_rows.iter().map(|row| row.aggregate_id).collect(),
        route_rows.iter().map(|row| row.aggregate_id).collect(),
        inventory_ids.clone(),
    )
    .await?;

    let infra = {
        let cached = state.infra_status.read().await;
        InfraStatusResponse {
            kafka: cached.kafka,
            yugabyte: cached.yugabyte,
            cassandra: cached.cassandra,
        }
    };

    let game_over = stations.iter().any(|station| station.stock_pct <= 0.0);

    Ok(BuiltGameState {
        snapshot: GameStateResponse {
            ship,
            stations,
            cargo,
            oversight,
            events,
            game_over,
            infra,
        },
        tracked_aggregate_ids,
    })
}

async fn load_ship_snapshot(
    state: &AppState,
    ship_id: Uuid,
) -> Result<Option<ShipStateResponse>, GatewayError> {
    let fleet_event_store = state.event_store_for_service("fleet");
    let fleet_snapshot_store = state.snapshot_store_for_service("fleet");

    let ship_agg_id = AggregateId::from_uuid(ship_id);
    let ship_events = fleet_event_store.load(&ship_agg_id).await?;
    let snapshot = fleet_snapshot_store.load(&ship_agg_id).await.ok().flatten();
    let last_snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64()).unwrap_or(0);

    if ship_events.is_empty() {
        return Ok(None);
    }

    let hydrated = hydrate_ship(&ship_events);
    let aggregate_version = ship_events.last().map(|e| e.version.as_u64()).unwrap_or(0);
    let correlation_id = ship_events
        .last()
        .map(|e| e.correlation_id)
        .unwrap_or_else(Uuid::new_v4);

    Ok(Some(ShipStateResponse {
        id: ship_id,
        name: hydrated.name,
        status: hydrated.status,
        station_id: hydrated.station_id,
        route_label: hydrated.route_label,
        fuel_pct: hydrated.fuel_pct,
        aggregate_version,
        last_snapshot_version,
        correlation_id,
    }))
}

async fn load_station_snapshots(
    state: &AppState,
    station_ids: [Uuid; 4],
) -> Result<Vec<GameStationResponse>, GatewayError> {
    let station_event_store = state.event_store_for_service("station");
    let mut stations = Vec::with_capacity(station_ids.len());

    for station_uuid in &station_ids {
        let agg_id = AggregateId::from_uuid(*station_uuid);
        let events = station_event_store.load(&agg_id).await?;
        let hydrated = hydrate_station(&events);
        if !hydrated.registered {
            continue;
        }

        let stock_pct = if hydrated.capacity_kg > 0.0 {
            (hydrated.current_stock_kg as f64 / hydrated.capacity_kg as f64 * 100.0)
                .clamp(0.0, 100.0)
        } else {
            0.0
        };

        stations.push(GameStationResponse {
            id: *station_uuid,
            name: hydrated.name,
            stock_pct,
            capacity_kg: hydrated.capacity_kg,
            stock_low: stock_pct < 20.0,
        });
    }

    Ok(stations)
}

async fn load_active_cargo_snapshot(
    manifest_rows: &[ProjectionRow],
    ship: Option<&ShipStateResponse>,
    session_ids: &SessionIds,
) -> Result<Option<GameCargoResponse>, GatewayError> {
    let active_manifest = manifest_rows.iter().find_map(|row| {
        let status = row.state.get("status")?.as_str()?;
        if status.eq_ignore_ascii_case("Closed") {
            return None;
        }
        Some((row.aggregate_id, status.to_owned(), row.state.clone()))
    });

    let Some((manifest_id, status, manifest_state)) = active_manifest else {
        return Ok(None);
    };

    let destination_station_id = match ship {
        Some(ship) if ship.status.eq_ignore_ascii_case("transit") => ship.station_id,
        Some(ship) => ship.station_id.and_then(|station_id| {
            session_ids
                .station_ids
                .iter()
                .position(|id| *id == station_id)
                .map(|idx| session_ids.station_ids[(idx + 1) % session_ids.station_ids.len()])
        }),
        None => None,
    };

    let voyage_id = manifest_state
        .get("voyage_id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or(manifest_id);

    Ok(Some(GameCargoResponse {
        manifest_id,
        voyage_id,
        destination_station_id,
        amount_pct: 35,
        status,
    }))
}

async fn load_recent_events(
    state: &AppState,
    session_ids: SessionIds,
    manifest_ids: Vec<Uuid>,
    route_ids: Vec<Uuid>,
    inventory_ids: Vec<Uuid>,
) -> Result<Vec<GameEventResponse>, GatewayError> {
    let mut all_events = Vec::new();

    collect_events_for_service(
        state,
        "fleet",
        std::iter::once(session_ids.ship_id),
        &mut all_events,
    )
    .await?;
    collect_events_for_service(
        state,
        "station",
        session_ids.station_ids.iter().copied(),
        &mut all_events,
    )
    .await?;
    collect_events_for_service(state, "cargo", manifest_ids, &mut all_events).await?;
    collect_events_for_service(state, "navigation", route_ids, &mut all_events).await?;
    collect_events_for_service(state, "supply", inventory_ids, &mut all_events).await?;

    all_events.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(all_events
        .into_iter()
        .take(20)
        .map(|(_, event)| event)
        .collect())
}

async fn collect_events_for_service<I>(
    state: &AppState,
    service: &str,
    aggregate_ids: I,
    out: &mut Vec<(DateTime<Utc>, GameEventResponse)>,
) -> Result<(), GatewayError>
where
    I: IntoIterator<Item = Uuid>,
{
    let event_store = state.event_store_for_service(service);
    for aggregate_id in aggregate_ids {
        let events = event_store
            .load(&AggregateId::from_uuid(aggregate_id))
            .await?;
        for event in events {
            out.push((
                event.timestamp,
                GameEventResponse {
                    id: event.event_id,
                    timestamp: event.timestamp.to_rfc3339(),
                    version: event.version.as_u64(),
                    service: service.to_owned(),
                    event_name: event.event_type,
                    aggregate_id,
                    correlation_id: event.correlation_id,
                },
            ));
        }
    }

    Ok(())
}

#[derive(sqlx::FromRow)]
struct ProjectionRow {
    aggregate_id: Uuid,
    state: serde_json::Value,
}

async fn load_projection_rows(
    pool: &sqlx::PgPool,
    projection_id: &str,
    json_filter: Option<(&str, Uuid)>,
) -> Result<Vec<ProjectionRow>, GatewayError> {
    let rows = match json_filter {
        Some((field, value)) => {
            let sql = format!(
                "SELECT aggregate_id, state FROM projections \
                 WHERE projection_id = $1 AND state->>'{field}' = $2"
            );
            sqlx::query_as::<_, ProjectionRow>(&sql)
                .bind(projection_id)
                .bind(value.to_string())
                .fetch_all(pool)
                .await?
        }
        None => {
            sqlx::query_as::<_, ProjectionRow>(
                "SELECT aggregate_id, state FROM projections WHERE projection_id = $1",
            )
            .bind(projection_id)
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows)
}

async fn load_supply_inventory_ids(
    state: &AppState,
    station_ids: &[Uuid; 4],
) -> Result<Vec<Uuid>, GatewayError> {
    #[derive(sqlx::FromRow)]
    struct InventoryRow {
        inventory_id: Uuid,
    }

    let rows = sqlx::query_as::<_, InventoryRow>(
        "SELECT inventory_id FROM supply_inventory WHERE station_id = ANY($1)",
    )
    .bind(station_ids.as_slice())
    .fetch_all(state.pool_for_service("supply"))
    .await?;

    Ok(rows.into_iter().map(|row| row.inventory_id).collect())
}

// ── Ship hydration ──────────────────────────────────────────────────────────

struct HydratedShip {
    name: String,
    status: String,
    station_id: Option<Uuid>,
    route_label: String,
    fuel_pct: u32,
}

fn hydrate_ship(events: &[canon_core::EventEnvelope]) -> HydratedShip {
    let mut name = String::new();
    let mut status = "unknown".to_owned();
    let mut station_id: Option<Uuid> = None;
    let mut route_label = String::new();
    let mut fuel_level: f32 = 100.0;
    let mut capacity: f32 = 1.0;

    for event in events {
        match event.event_type.as_str() {
            "ShipRegistered" => {
                #[derive(serde::Deserialize)]
                struct E {
                    name: String,
                    capacity_kg: f32,
                    #[serde(default)]
                    home_station: Option<Uuid>,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    name = e.name;
                    capacity = e.capacity_kg;
                    status = "docked".to_owned();
                    fuel_level = capacity;
                    station_id = e.home_station;
                }
            }
            "RouteAssigned" => {
                #[derive(serde::Deserialize)]
                struct E {
                    route_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    route_label = e.route_id.to_string();
                }
            }
            "ShipDeparted" => {
                #[derive(serde::Deserialize)]
                struct E {
                    destination: Uuid,
                    fuel_at_departure: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    status = "transit".to_owned();
                    station_id = Some(e.destination);
                    fuel_level -= e.fuel_at_departure * 0.1;
                }
            }
            "ShipDecommissioned" => {
                status = "dead".to_owned();
            }
            "ResupplyScheduled" => {
                #[derive(serde::Deserialize)]
                struct E {
                    fuel_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    fuel_level = (fuel_level + e.fuel_kg).min(capacity);
                }
            }
            "ShipDockedAtStation" => {
                #[derive(serde::Deserialize)]
                struct E {
                    station_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    status = "docked".to_owned();
                    station_id = Some(e.station_id);
                }
            }
            _ => {}
        }
    }

    let fuel_pct = if capacity > 0.0 {
        ((fuel_level / capacity) * 100.0).clamp(0.0, 100.0) as u32
    } else {
        0
    };

    HydratedShip {
        name,
        status,
        station_id,
        route_label,
        fuel_pct,
    }
}

// ── Station hydration ───────────────────────────────────────────────────────

struct HydratedStation {
    name: String,
    capacity_kg: f32,
    current_stock_kg: f32,
    registered: bool,
}

fn hydrate_station(events: &[canon_core::EventEnvelope]) -> HydratedStation {
    let mut name = String::new();
    let mut capacity_kg: f32 = 0.0;
    let mut current_stock_kg: f32 = 0.0;
    let mut registered = false;

    for event in events {
        match event.event_type.as_str() {
            "StationRegistered" => {
                #[derive(serde::Deserialize)]
                struct E {
                    name: String,
                    capacity_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    name = e.name;
                    capacity_kg = e.capacity_kg;
                    registered = true;
                }
            }
            "CargoReceived" => {
                #[derive(serde::Deserialize)]
                struct E {
                    weight_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    current_stock_kg += e.weight_kg;
                }
            }
            "CapacityUpdated" => {
                #[derive(serde::Deserialize)]
                struct E {
                    capacity_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    capacity_kg = e.capacity_kg;
                }
            }
            "StockDrained" => {
                #[derive(serde::Deserialize)]
                struct E {
                    remaining_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    current_stock_kg = e.remaining_kg;
                }
            }
            "StationOffline" => {
                current_stock_kg = 0.0;
            }
            _ => {}
        }
    }

    HydratedStation {
        name,
        capacity_kg,
        current_stock_kg,
        registered,
    }
}

// ── Oversight window query ─────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct WindowRow {
    handler_id: String,
    correlation_key: Uuid,
    window_id: Uuid,
    messages: serde_json::Value,
    status: String,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

async fn query_first_oversight_window(
    state: &AppState,
    tracked_ids: &HashSet<Uuid>,
) -> Option<OversightWindowResponse> {
    let mut candidates = Vec::new();

    for stores in state.service_stores.values() {
        let rows: Vec<WindowRow> = sqlx::query_as(
            "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
             FROM inbox_windows WHERE status = 'pending' ORDER BY created_at DESC LIMIT 20",
        )
        .fetch_all(&stores.pool)
        .await
        .ok()?;
        candidates.extend(rows);
    }

    candidates.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    candidates.into_iter().find_map(|row| {
        if !json_contains_any_uuid(&row.messages, tracked_ids) {
            return None;
        }

        let now = Utc::now();
        let ttl_total_secs = row
            .expires_at
            .map(|exp| (exp - row.created_at).num_seconds().max(0) as u32)
            .unwrap_or(0);
        let ttl_remaining_secs = row
            .expires_at
            .map(|exp| (exp - now).num_seconds().max(0) as u32)
            .unwrap_or(0);

        let messages_arr = row.messages.as_array();
        let has_event_type = |event_type: &str| -> bool {
            messages_arr
                .map(|arr| {
                    arr.iter().any(|m| {
                        m.get("event_type")
                            .and_then(|v| v.as_str())
                            .is_some_and(|t| t == event_type)
                            || m.get("message_type")
                                .and_then(|v| v.as_str())
                                .is_some_and(|t| t == event_type)
                    })
                })
                .unwrap_or(false)
        };

        Some(OversightWindowResponse {
            window_id: row.window_id,
            handler_id: row.handler_id,
            correlation_key: row.correlation_key,
            ship_name: String::new(),
            dest_label: String::new(),
            status: row.status,
            requirements: vec![
                RequirementResponse {
                    label: "ShipArrivedAtStation".to_owned(),
                    met: has_event_type("ShipArrivedAtStation"),
                },
                RequirementResponse {
                    label: "ManifestCreated".to_owned(),
                    met: has_event_type("ManifestCreated"),
                },
            ],
            ttl_remaining_secs,
            ttl_total_secs,
        })
    })
}

fn json_contains_any_uuid(value: &serde_json::Value, tracked_ids: &HashSet<Uuid>) -> bool {
    match value {
        serde_json::Value::String(text) => Uuid::parse_str(text)
            .ok()
            .is_some_and(|uuid| tracked_ids.contains(&uuid)),
        serde_json::Value::Array(items) => items
            .iter()
            .any(|item| json_contains_any_uuid(item, tracked_ids)),
        serde_json::Value::Object(map) => map
            .values()
            .any(|item| json_contains_any_uuid(item, tracked_ids)),
        _ => false,
    }
}
