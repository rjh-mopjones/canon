//! Debug inspection endpoints for all 5 demo aggregates.
//!
//! Provides `/debug/aggregate`, `/debug/events`, and `/debug/commands` endpoints
//! that return well-formatted JSON with decoded payloads. The aggregate endpoint
//! tries hydrating against every registered demo aggregate type (Ship, ManifestState,
//! Route, Inventory, Station) and returns the first successful hydration.
//!
//! Event and command responses reuse the canonical types from `canon_core::debug`
//! (`DebugEventResponse` and `DebugCommandResponse`). The aggregate endpoint uses
//! a gateway-specific response that extends the core type with `aggregate_type`.

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use canon_core::traits::{CommandStore, EventStore, SnapshotStore};
use canon_core::{Aggregate, AggregateId, EventEnvelope, Version};

use crate::error::GatewayError;
use crate::state::AppState;

// Re-export canon-core debug response types for events and commands.
use canon_core::debug::{DebugCommandResponse, DebugEventResponse};

/// Function pointer type for aggregate hydration attempts.
type HydrateFn = fn(&Option<canon_core::Snapshot>, &[EventEnvelope]) -> Option<serde_json::Value>;

// ── Query parameters ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateIdQuery {
    pub aggregate_id: Uuid,
}

// ── Gateway-specific aggregate response ─────────────────────────────────────
//
// Extends the core `DebugAggregateResponse` with an `aggregate_type` field
// that indicates which of the 5 demo aggregates hydrated successfully.

#[derive(Debug, Clone, Serialize)]
pub struct GatewayAggregateResponse {
    pub aggregate_id: AggregateId,
    pub aggregate_type: String,
    pub version: Version,
    pub state: serde_json::Value,
    pub snapshot_version: Option<u64>,
    pub events_replayed: u64,
}

/// Aggregate type definition with its service name and hydrate function.
struct AggregateTypeDef {
    type_name: &'static str,
    service: &'static str,
    hydrate_fn: HydrateFn,
}

/// All known aggregate types and their owning services.
const AGGREGATE_TYPES: &[AggregateTypeDef] = &[
    AggregateTypeDef {
        type_name: "Ship",
        service: "fleet",
        hydrate_fn: try_hydrate_state::<fleet_service::aggregate::Ship>,
    },
    AggregateTypeDef {
        type_name: "Manifest",
        service: "cargo",
        hydrate_fn: try_hydrate_state::<cargo_service::aggregate::ManifestState>,
    },
    AggregateTypeDef {
        type_name: "Route",
        service: "navigation",
        hydrate_fn: try_hydrate_state::<navigation_service::aggregate::Route>,
    },
    AggregateTypeDef {
        type_name: "Inventory",
        service: "supply",
        hydrate_fn: try_hydrate_state::<supply_service::aggregate::Inventory>,
    },
    AggregateTypeDef {
        type_name: "Station",
        service: "station",
        hydrate_fn: try_hydrate_state::<station_service::aggregate::Station>,
    },
];

// ── Router ──────────────────────────────────────────────────────────────────

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/debug/aggregate", get(debug_aggregate))
        .route("/debug/events", get(debug_events))
        .route("/debug/commands", get(debug_commands))
}

// ── Handlers ────────────────────────────────────────────────────────────────

/// GET /debug/aggregate?aggregateId=<uuid>
///
/// Tries hydrating the given aggregate ID against all 5 demo aggregate types,
/// searching each service's event store and snapshot store.
/// Returns the first successful hydration result, or 404 if no events exist.
async fn debug_aggregate(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<GatewayAggregateResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: inspecting aggregate");

    // Try each aggregate type's service-specific event store and snapshot store
    for agg_def in AGGREGATE_TYPES {
        let event_store = state.event_store_for_service(agg_def.service);
        let snapshot_store = state.snapshot_store_for_service(agg_def.service);

        // Load snapshot (if any)
        let snapshot = snapshot_store.load(&agg_id).await.ok().flatten();
        let snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64());
        let from_version = match &snapshot {
            Some(snap) => snap.version.next(),
            None => Version::initial(),
        };

        // Load all events from this service's event store
        let all_events = match event_store.load(&agg_id).await {
            Ok(events) => events,
            Err(_) => continue,
        };

        if all_events.is_empty() && snapshot.is_none() {
            continue;
        }

        let total_version = all_events
            .last()
            .map(|e| e.version)
            .or_else(|| snapshot.as_ref().map(|s| s.version))
            .unwrap_or_else(Version::initial);

        let post_snapshot_events: Vec<EventEnvelope> = all_events
            .into_iter()
            .filter(|e| e.version.as_u64() >= from_version.as_u64())
            .collect();

        let events_replayed = post_snapshot_events.len() as u64;

        if let Some(state_json) = (agg_def.hydrate_fn)(&snapshot, &post_snapshot_events) {
            return Ok(Json(GatewayAggregateResponse {
                aggregate_id: agg_id,
                aggregate_type: agg_def.type_name.to_string(),
                version: total_version,
                state: state_json,
                snapshot_version,
                events_replayed,
            }));
        }
    }

    Err(GatewayError::NotFound(format!(
        "no events or snapshots found for aggregate {}",
        params.aggregate_id
    )))
}

/// GET /debug/events?aggregateId=<uuid>
///
/// Returns the full event history for the given aggregate ID.
/// Searches all service event stores.
async fn debug_events(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<Vec<DebugEventResponse>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: loading events");

    // Try each service's event store until we find events
    for stores in state.service_stores.values() {
        let events = match stores.event_store.load(&agg_id).await {
            Ok(events) if !events.is_empty() => events,
            _ => continue,
        };

        let responses: Vec<DebugEventResponse> = events
            .into_iter()
            .map(|e| DebugEventResponse {
                event_id: e.event_id,
                aggregate_id: e.aggregate_id,
                version: e.version,
                event_type: e.event_type,
                event_version: e.event_version,
                payload: decode_payload(&e.payload),
                correlation_id: e.correlation_id,
                causation_id: e.causation_id,
                timestamp: e.timestamp,
            })
            .collect();

        tracing::debug!(
            aggregate_id = %params.aggregate_id,
            event_count = responses.len(),
            "debug: events loaded"
        );

        return Ok(Json(responses));
    }

    Ok(Json(Vec::new()))
}

/// GET /debug/commands?aggregateId=<uuid>
///
/// Returns the command history for the given aggregate ID.
/// Searches all service schemas.
async fn debug_commands(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<Vec<DebugCommandResponse>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: loading commands");

    // Search all service schemas for commands belonging to this aggregate
    for stores in state.service_stores.values() {
        let command_store =
            canon_command_store_yugabyte::YugabyteCommandStore::new(stores.pool.clone());
        let commands = match command_store.load_for_aggregate(&agg_id).await {
            Ok(cmds) if !cmds.is_empty() => cmds,
            _ => continue,
        };

        let responses: Vec<DebugCommandResponse> = commands
            .into_iter()
            .map(|cmd| DebugCommandResponse {
                command_id: cmd.command_id,
                aggregate_id: cmd.aggregate_id,
                command_type: cmd.command_type,
                correlation_id: cmd.correlation_id,
                causation_id: cmd.causation_id,
                timestamp: cmd.timestamp,
                payload: decode_payload(&cmd.payload),
                command_version: cmd.command_version,
            })
            .collect();

        tracing::debug!(
            aggregate_id = %params.aggregate_id,
            command_count = responses.len(),
            "debug: commands loaded"
        );

        return Ok(Json(responses));
    }

    Ok(Json(Vec::new()))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Attempt to hydrate an aggregate from a snapshot + events, returning the
/// serialized state as JSON on success. Returns `None` if hydration fails
/// (i.e., the events belong to a different aggregate type).
fn try_hydrate_state<A: Aggregate>(
    snapshot: &Option<canon_core::Snapshot>,
    events: &[EventEnvelope],
) -> Option<serde_json::Value>
where
    A::State: Default + serde::de::DeserializeOwned + serde::Serialize,
{
    let mut state: A::State = match snapshot {
        Some(snap) => serde_json::from_slice(&snap.state).ok()?,
        None => A::State::default(),
    };

    let events_owned: Vec<EventEnvelope> = events.to_vec();
    A::hydrate(&mut state, events_owned.into_iter()).ok()?;
    serde_json::to_value(&state).ok()
}

/// Attempt to decode a payload as JSON. Falls back to a string representation
/// if the payload is not valid JSON.
fn decode_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}
