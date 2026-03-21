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
/// Tries hydrating the given aggregate ID against all 5 demo aggregate types.
/// Returns the first successful hydration result, or 404 if no events exist.
async fn debug_aggregate(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<GatewayAggregateResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: inspecting aggregate");

    // Load snapshot (if any) — use the #[from] conversion directly
    let snapshot = state.snapshot_store.load(&agg_id).await?;

    let snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64());
    let from_version = match &snapshot {
        Some(snap) => snap.version.next(),
        None => Version::initial(),
    };

    // Single event store read — load all events, then derive the post-snapshot
    // subset by filtering. Avoids a redundant second Cassandra read.
    let all_events = state.event_store.load(&agg_id).await?;

    if all_events.is_empty() && snapshot.is_none() {
        return Err(GatewayError::NotFound(format!(
            "no events or snapshots found for aggregate {}",
            params.aggregate_id
        )));
    }

    let total_version = all_events
        .last()
        .map(|e| e.version)
        .or_else(|| snapshot.as_ref().map(|s| s.version))
        .unwrap_or_else(Version::initial);

    // Filter events to only those after the snapshot version.
    let post_snapshot_events: Vec<EventEnvelope> = all_events
        .into_iter()
        .filter(|e| e.version.as_u64() >= from_version.as_u64())
        .collect();

    let events_replayed = post_snapshot_events.len() as u64;

    // Try each aggregate type. The first successful hydration wins.
    // Order: Ship, ManifestState, Route, Inventory, Station.
    let types: &[(&str, HydrateFn)] = &[
        ("Ship", try_hydrate_state::<fleet_service::aggregate::Ship>),
        (
            "Manifest",
            try_hydrate_state::<cargo_service::aggregate::ManifestState>,
        ),
        (
            "Route",
            try_hydrate_state::<navigation_service::aggregate::Route>,
        ),
        (
            "Inventory",
            try_hydrate_state::<supply_service::aggregate::Inventory>,
        ),
        (
            "Station",
            try_hydrate_state::<station_service::aggregate::Station>,
        ),
    ];

    for &(type_name, hydrate_fn) in types {
        if let Some(state_json) = hydrate_fn(&snapshot, &post_snapshot_events) {
            return Ok(Json(GatewayAggregateResponse {
                aggregate_id: agg_id,
                aggregate_type: type_name.to_string(),
                version: total_version,
                state: state_json,
                snapshot_version,
                events_replayed,
            }));
        }
    }

    // If none of the aggregate types could hydrate, return with unknown type.
    tracing::warn!(
        aggregate_id = %params.aggregate_id,
        "debug: could not hydrate as any known aggregate type"
    );

    Ok(Json(GatewayAggregateResponse {
        aggregate_id: agg_id,
        aggregate_type: "unknown".to_string(),
        version: total_version,
        state: serde_json::json!({
            "error": "could not hydrate as any known aggregate type",
        }),
        snapshot_version,
        events_replayed,
    }))
}

/// GET /debug/events?aggregateId=<uuid>
///
/// Returns the full event history for the given aggregate ID with decoded payloads.
/// Uses `canon_core::DebugEventResponse` as the response type.
async fn debug_events(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<Vec<DebugEventResponse>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: loading events");

    let events = state.event_store.load(&agg_id).await?;

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

    Ok(Json(responses))
}

/// GET /debug/commands?aggregateId=<uuid>
///
/// Returns the command history for the given aggregate ID with decoded payloads.
/// Uses `canon_core::DebugCommandResponse` as the response type.
async fn debug_commands(
    State(state): State<AppState>,
    Query(params): Query<AggregateIdQuery>,
) -> Result<Json<Vec<DebugCommandResponse>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: loading commands");

    let commands = state
        .command_store
        .load_for_aggregate(&agg_id)
        .await
        .map_err(|e| GatewayError::Internal(format!("command store error: {e}")))?;

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

    Ok(Json(responses))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Attempt to hydrate an aggregate from a snapshot + events, returning the
/// serialized state as JSON on success. Returns `None` if hydration fails
/// (i.e., the events belong to a different aggregate type).
///
/// Unlike the previous version, snapshot deserialization failures are treated
/// as "wrong type" rather than silently falling back to default state.
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
///
/// Note: `canon_core::debug` has an identical private function. This local copy
/// exists because the core version is not publicly exported.
fn decode_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}
