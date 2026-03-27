use std::collections::HashSet;

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use canon_core::AggregateId;
use canon_event_store::EventStore;
use canon_snapshot_store::SnapshotStore;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{
    DeadLetterResponse, GameCargoResponse, GameEventResponse, GameStateResponse,
    OversightWindowResponse, RequirementResponse, ShipStateResponse, StationStateResponse,
};

use super::fleet::hydrate_ship_from_events;
use super::station::hydrate_station_from_events;

pub fn router() -> Router<AppState> {
    Router::new().route("/game/:session_id", get(get_game_state))
}

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

#[derive(sqlx::FromRow)]
struct DeadLetterRow {
    id: Uuid,
    handler_id: Option<String>,
    aggregate_id: Option<Uuid>,
    error: Option<String>,
    attempts: i32,
    created_at: DateTime<Utc>,
}

async fn get_game_state(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<GameStateResponse>, GatewayError> {
    let session_ids = {
        let sessions = state.sessions.read().await;
        sessions
            .get(&session_id)
            .map(|session| {
                (
                    session.ids.clone(),
                    session.manifests.clone(),
                    session.aggregate_id_set(),
                )
            })
            .ok_or_else(|| GatewayError::NotFound(format!("session {session_id} not found")))?
    };

    let (session_ids, manifests, aggregate_ids) = session_ids;

    let ship = hydrate_session_ship(&state, session_ids.ship_id).await?;
    let stations = hydrate_session_stations(&state, &session_ids.station_ids).await?;
    let cargo = hydrate_session_cargo(&state, &session_ids, &manifests).await?;
    let oversight = load_session_oversight(&state, &aggregate_ids).await;
    let dead_letters = load_session_dead_letters(&state, &aggregate_ids).await;
    let events = load_session_events(&state, &session_ids, &manifests).await?;

    Ok(Json(GameStateResponse {
        session_id,
        ship,
        stations,
        cargo,
        oversight,
        dead_letters,
        events,
    }))
}

async fn hydrate_session_ship(
    state: &AppState,
    ship_id: Uuid,
) -> Result<Option<ShipStateResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(ship_id);
    let event_store = state.event_store_for_service("fleet");
    let snapshot_store = state.snapshot_store_for_service("fleet");
    let events = event_store.load(&agg_id).await?;

    if events.is_empty() {
        return Ok(None);
    }

    let snapshot = snapshot_store.load(&agg_id).await.ok().flatten();
    let last_snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64()).unwrap_or(0);
    let ship_state = hydrate_ship_from_events(&events, ship_id);
    let aggregate_version = events.last().map(|e| e.version.as_u64()).unwrap_or(0);
    let correlation_id = events
        .last()
        .map(|e| e.correlation_id)
        .unwrap_or_else(Uuid::new_v4);

    Ok(Some(ShipStateResponse {
        id: ship_id,
        name: ship_state.name,
        status: ship_state.status,
        station_id: ship_state.station_id,
        route_label: ship_state.route_label,
        fuel_pct: ship_state.fuel_pct,
        aggregate_version,
        last_snapshot_version,
        correlation_id,
    }))
}

async fn hydrate_session_stations(
    state: &AppState,
    station_ids: &[Uuid; 4],
) -> Result<Vec<StationStateResponse>, GatewayError> {
    let mut stations = Vec::with_capacity(station_ids.len());

    for station_id in station_ids {
        let agg_id = AggregateId::from_uuid(*station_id);
        let events = state
            .event_store_for_service("station")
            .load(&agg_id)
            .await?;
        let hydrated = hydrate_station_from_events(&events);

        if hydrated.registered {
            stations.push(StationStateResponse {
                id: *station_id,
                name: hydrated.name,
                capacity_kg: hydrated.capacity_kg,
                current_stock_kg: hydrated.current_stock_kg,
            });
        }
    }

    Ok(stations)
}

async fn load_session_oversight(
    state: &AppState,
    aggregate_ids: &HashSet<Uuid>,
) -> Option<OversightWindowResponse> {
    let mut matches: Vec<(DateTime<Utc>, OversightWindowResponse)> = Vec::new();

    for stores in state.service_stores.values() {
        let rows: Vec<WindowRow> = sqlx::query_as(
            "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
             FROM inbox_windows WHERE status = 'pending' \
             ORDER BY created_at DESC",
        )
        .fetch_all(&stores.pool)
        .await
        .unwrap_or_default();

        for row in rows {
            if !json_mentions_any_uuid(&row.messages, aggregate_ids) {
                continue;
            }

            let now = Utc::now();
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

            let ttl_total_secs = row
                .expires_at
                .map(|exp| (exp - row.created_at).num_seconds().max(0) as u32)
                .unwrap_or(0);
            let ttl_remaining_secs = row
                .expires_at
                .map(|exp| (exp - now).num_seconds().max(0) as u32)
                .unwrap_or(0);

            matches.push((
                row.created_at,
                OversightWindowResponse {
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
                },
            ));
        }
    }

    matches.sort_by(|a, b| b.0.cmp(&a.0));
    matches.into_iter().map(|(_, window)| window).next()
}

async fn load_session_dead_letters(
    state: &AppState,
    aggregate_ids: &HashSet<Uuid>,
) -> Vec<DeadLetterResponse> {
    let mut entries: Vec<(DateTime<Utc>, DeadLetterResponse)> = Vec::new();

    for (service_name, stores) in &state.service_stores {
        let rows: Vec<DeadLetterRow> = sqlx::query_as(
            "SELECT id, handler_id, aggregate_id, error, attempts, created_at \
             FROM dead_letters ORDER BY created_at DESC",
        )
        .fetch_all(&stores.pool)
        .await
        .unwrap_or_default();

        for row in rows {
            let Some(aggregate_id) = row.aggregate_id else {
                continue;
            };
            if !aggregate_ids.contains(&aggregate_id) {
                continue;
            }

            let service = row.handler_id.unwrap_or_else(|| service_name.clone());
            let created_at = row.created_at;
            entries.push((
                created_at,
                DeadLetterResponse {
                    id: row.id,
                    event_type: "unknown".to_owned(),
                    service,
                    aggregate_id: aggregate_id.to_string(),
                    error: row.error.unwrap_or_default(),
                    attempts: row.attempts as u32,
                    requeued: false,
                    created_at: created_at.to_rfc3339(),
                },
            ));
        }
    }

    entries.sort_by(|a, b| b.0.cmp(&a.0));
    entries.into_iter().map(|(_, entry)| entry).collect()
}

async fn load_session_events(
    state: &AppState,
    session_ids: &crate::session::SessionIds,
    manifests: &[crate::session::SessionManifest],
) -> Result<Vec<GameEventResponse>, GatewayError> {
    let mut entries = Vec::new();

    load_events_for_aggregate(state, "fleet", session_ids.ship_id, &mut entries).await?;

    for station_id in session_ids.station_ids {
        load_events_for_aggregate(state, "station", station_id, &mut entries).await?;
    }
    for manifest in manifests {
        load_events_for_aggregate(state, "cargo", manifest.manifest_id, &mut entries).await?;
    }

    entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(entries)
}

async fn load_events_for_aggregate(
    state: &AppState,
    service: &str,
    aggregate_id: Uuid,
    entries: &mut Vec<GameEventResponse>,
) -> Result<(), GatewayError> {
    let agg_id = AggregateId::from_uuid(aggregate_id);
    let events = state.event_store_for_service(service).load(&agg_id).await?;

    entries.extend(events.into_iter().map(|event| GameEventResponse {
        id: event.event_id,
        timestamp: event.timestamp.to_rfc3339(),
        version: event.version.as_u64(),
        service: service.to_owned(),
        event_name: event.event_type,
        aggregate_id,
        correlation_id: event.correlation_id,
    }));

    Ok(())
}

fn json_mentions_any_uuid(value: &serde_json::Value, aggregate_ids: &HashSet<Uuid>) -> bool {
    match value {
        serde_json::Value::String(raw) => Uuid::parse_str(raw)
            .map(|id| aggregate_ids.contains(&id))
            .unwrap_or(false),
        serde_json::Value::Array(items) => items
            .iter()
            .any(|item| json_mentions_any_uuid(item, aggregate_ids)),
        serde_json::Value::Object(map) => map
            .values()
            .any(|value| json_mentions_any_uuid(value, aggregate_ids)),
        _ => false,
    }
}

async fn hydrate_session_cargo(
    state: &AppState,
    session_ids: &crate::session::SessionIds,
    manifests: &[crate::session::SessionManifest],
) -> Result<Option<GameCargoResponse>, GatewayError> {
    for manifest in manifests.iter().rev() {
        let agg_id = AggregateId::from_uuid(manifest.manifest_id);
        let events = state.event_store_for_service("cargo").load(&agg_id).await?;
        if events.is_empty() {
            continue;
        }

        let hydrated = hydrate_manifest_from_events(&events);
        if hydrated.has_active_cargo {
            let destination_station_id =
                next_station_id_for_origin(session_ids, manifest.origin_station_id);
            return Ok(Some(GameCargoResponse {
                manifest_id: manifest.manifest_id,
                voyage_id: manifest.voyage_id,
                destination_station_id,
                amount_pct: 35,
                status: hydrated.status,
            }));
        }
    }

    Ok(None)
}

struct HydratedManifest {
    status: String,
    has_active_cargo: bool,
}

fn hydrate_manifest_from_events(events: &[canon_core::EventEnvelope]) -> HydratedManifest {
    let mut status = "open".to_owned();
    let mut active_items: HashSet<Uuid> = HashSet::new();

    for event in events {
        match event.event_type.as_str() {
            "ManifestCreated" => {
                status = "open".to_owned();
            }
            "CargoLoaded" => {
                #[derive(serde::Deserialize)]
                struct E {
                    item_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    active_items.insert(e.item_id);
                }
            }
            "UnloadingStarted" => {
                status = "unloading".to_owned();
            }
            "CargoUnloaded" => {
                #[derive(serde::Deserialize)]
                struct E {
                    item_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    active_items.remove(&e.item_id);
                }
            }
            "ManifestClosed" => {
                status = "closed".to_owned();
                active_items.clear();
            }
            _ => {}
        }
    }

    HydratedManifest {
        status,
        has_active_cargo: !active_items.is_empty(),
    }
}

fn next_station_id_for_origin(
    session_ids: &crate::session::SessionIds,
    origin_station_id: Option<Uuid>,
) -> Option<Uuid> {
    let origin_station_id = origin_station_id?;
    let origin_idx = session_ids
        .station_ids
        .iter()
        .position(|station_id| *station_id == origin_station_id)?;
    let dest_idx = (origin_idx + 1) % session_ids.station_ids.len();
    Some(session_ids.station_ids[dest_idx])
}
