//! GET /game/:session_id -- complete game state snapshot.
//!
//! Returns ship, stations, cargo, oversight, recent events, game_over flag,
//! and infra health as a single atomic JSON blob. Useful for bootstrap,
//! debug, recovery, and as the foundation for snapshot-push.

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use canon_core::AggregateId;
use canon_event_store::EventStore;
use canon_snapshot_store::SnapshotStore;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{
    EventHistoryEntry, GameStateResponse, InfraStatusResponse, OversightWindowResponse,
    RequirementResponse, ShipStateResponse, StationStateResponse,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/game/:session_id", get(game_snapshot))
}

/// GET /game/:session_id -- return complete game state as one atomic JSON blob.
async fn game_snapshot(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<GameStateResponse>, GatewayError> {
    // Look up session
    let (ship_id, station_ids) = {
        let sessions = state.sessions.read().await;
        match sessions.get(&session_id) {
            Some(session) => (session.ids.ship_id, session.ids.station_ids),
            None => {
                return Err(GatewayError::NotFound(format!(
                    "session {session_id} not found"
                )));
            }
        }
    };

    // ── Ship state ─────────────────────────────────────────────────────────
    let fleet_event_store = state.event_store_for_service("fleet");
    let fleet_snapshot_store = state.snapshot_store_for_service("fleet");

    let ship_agg_id = AggregateId::from_uuid(ship_id);
    let ship_events = fleet_event_store.load(&ship_agg_id).await?;
    let snapshot = fleet_snapshot_store.load(&ship_agg_id).await.ok().flatten();
    let last_snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64()).unwrap_or(0);

    let ship = if ship_events.is_empty() {
        None
    } else {
        let hydrated = hydrate_ship(&ship_events);
        let aggregate_version = ship_events.last().map(|e| e.version.as_u64()).unwrap_or(0);
        let correlation_id = ship_events
            .last()
            .map(|e| e.correlation_id)
            .unwrap_or_else(Uuid::new_v4);

        Some(ShipStateResponse {
            id: ship_id,
            name: hydrated.name,
            status: hydrated.status,
            station_id: hydrated.station_id,
            route_label: hydrated.route_label,
            fuel_pct: hydrated.fuel_pct,
            aggregate_version,
            last_snapshot_version,
            correlation_id,
        })
    };

    // ── Station states ─────────────────────────────────────────────────────
    let station_event_store = state.event_store_for_service("station");
    let mut stations = Vec::with_capacity(4);
    let mut game_over = false;

    for station_uuid in &station_ids {
        let agg_id = AggregateId::from_uuid(*station_uuid);
        let events = station_event_store.load(&agg_id).await?;
        let hydrated = hydrate_station(&events);

        if hydrated.registered {
            if hydrated.current_stock_kg <= 0.0 {
                game_over = true;
            }
            stations.push(StationStateResponse {
                id: *station_uuid,
                name: hydrated.name,
                capacity_kg: hydrated.capacity_kg,
                current_stock_kg: hydrated.current_stock_kg,
            });
        }
    }

    // ── Oversight windows (first pending across all services) ──────────────
    let oversight = query_first_oversight_window(&state).await;

    // ── Recent events (last 20 across ship + stations) ─────────────────────
    let mut all_events: Vec<(DateTime<Utc>, EventHistoryEntry)> = Vec::new();

    // Collect ship events
    for event in &ship_events {
        let payload: serde_json::Value =
            serde_json::from_slice(&event.payload).unwrap_or(serde_json::Value::Null);
        all_events.push((
            event.timestamp,
            EventHistoryEntry {
                event_id: event.event_id,
                version: event.version.as_u64(),
                event_type: event.event_type.clone(),
                event_version: event.event_version,
                correlation_id: event.correlation_id,
                timestamp: event.timestamp.to_rfc3339(),
                payload,
            },
        ));
    }

    // Collect station events
    for station_uuid in &station_ids {
        let agg_id = AggregateId::from_uuid(*station_uuid);
        let events = station_event_store.load(&agg_id).await?;
        for event in &events {
            let payload: serde_json::Value =
                serde_json::from_slice(&event.payload).unwrap_or(serde_json::Value::Null);
            all_events.push((
                event.timestamp,
                EventHistoryEntry {
                    event_id: event.event_id,
                    version: event.version.as_u64(),
                    event_type: event.event_type.clone(),
                    event_version: event.event_version,
                    correlation_id: event.correlation_id,
                    timestamp: event.timestamp.to_rfc3339(),
                    payload,
                },
            ));
        }
    }

    // Sort by timestamp descending, take last 20
    all_events.sort_by(|a, b| b.0.cmp(&a.0));
    let events: Vec<EventHistoryEntry> = all_events.into_iter().take(20).map(|(_, e)| e).collect();

    // ── Infra status ───────────────────────────────────────────────────────
    let infra = {
        let cached = state.infra_status.read().await;
        InfraStatusResponse {
            kafka: cached.kafka,
            yugabyte: cached.yugabyte,
            cassandra: cached.cassandra,
        }
    };

    Ok(Json(GameStateResponse {
        ship,
        stations,
        cargo: None,
        oversight,
        events,
        game_over,
        infra,
    }))
}

// ── Ship hydration (same logic as fleet routes) ────────────────────────────

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

// ── Station hydration (same logic as station routes) ───────────────────────

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

async fn query_first_oversight_window(state: &AppState) -> Option<OversightWindowResponse> {
    for stores in state.service_stores.values() {
        let row: Option<WindowRow> = sqlx::query_as(
            "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
             FROM inbox_windows WHERE status = 'pending' \
             ORDER BY created_at DESC LIMIT 1",
        )
        .fetch_optional(&stores.pool)
        .await
        .ok()
        .flatten();

        if let Some(row) = row {
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

            return Some(OversightWindowResponse {
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
            });
        }
    }

    None
}
