//! Debug inspection endpoints for all 5 demo aggregates.
//!
//! Provides `/debug/aggregate`, `/debug/events`, and `/debug/commands` endpoints
//! that return well-formatted JSON with decoded payloads. The aggregate endpoint
//! tries hydrating against every registered demo aggregate type (Ship, ManifestState,
//! Route, Inventory, Station) and returns the first successful hydration.

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use canon_core::traits::{CommandStore, EventStore, SnapshotStore};
use canon_core::{Aggregate, AggregateId, EventEnvelope, Version};

use crate::error::GatewayError;
use crate::state::AppState;

// ── Query parameters ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateIdQuery {
    pub aggregate_id: Uuid,
}

// ── Response types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct DebugAggregateResponse {
    pub aggregate_id: Uuid,
    pub aggregate_type: String,
    pub version: u64,
    pub state: serde_json::Value,
    pub snapshot_version: Option<u64>,
    pub events_replayed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugEventResponse {
    pub event_id: Uuid,
    pub aggregate_id: Uuid,
    pub version: u64,
    pub event_type: String,
    pub event_version: u32,
    pub payload: serde_json::Value,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugCommandResponse {
    pub command_id: Uuid,
    pub aggregate_id: Uuid,
    pub command_type: String,
    pub command_version: u32,
    pub payload: serde_json::Value,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
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
) -> Result<Json<DebugAggregateResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(params.aggregate_id);

    tracing::debug!(aggregate_id = %params.aggregate_id, "debug: inspecting aggregate");

    // Load snapshot (if any)
    let snapshot = state
        .snapshot_store
        .load(&agg_id)
        .await
        .map_err(|e| GatewayError::Internal(format!("snapshot store error: {e}")))?;

    let snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64());
    let from_version = match &snapshot {
        Some(snap) => snap.version.next(),
        None => Version::initial(),
    };

    // Load events from the appropriate version
    let events = state
        .event_store
        .load_from_version(&agg_id, from_version)
        .await?;

    let all_events = state.event_store.load(&agg_id).await?;
    let total_version = all_events
        .last()
        .map(|e| e.version.as_u64())
        .or(snapshot_version)
        .unwrap_or(0);

    if all_events.is_empty() && snapshot.is_none() {
        return Err(GatewayError::NotFound(format!(
            "no events or snapshots found for aggregate {}",
            params.aggregate_id
        )));
    }

    let events_replayed = events.len() as u64;

    // Try each aggregate type. The first successful hydration wins.
    // Order: Ship, ManifestState, Route, Inventory, Station.
    if let Some(resp) = try_hydrate::<fleet_service::aggregate::Ship>(
        "Ship",
        &agg_id,
        &snapshot,
        &events,
        total_version,
        snapshot_version,
        events_replayed,
    ) {
        return Ok(Json(resp));
    }

    if let Some(resp) = try_hydrate::<cargo_service::aggregate::ManifestState>(
        "Manifest",
        &agg_id,
        &snapshot,
        &events,
        total_version,
        snapshot_version,
        events_replayed,
    ) {
        return Ok(Json(resp));
    }

    if let Some(resp) = try_hydrate::<navigation_service::aggregate::Route>(
        "Route",
        &agg_id,
        &snapshot,
        &events,
        total_version,
        snapshot_version,
        events_replayed,
    ) {
        return Ok(Json(resp));
    }

    if let Some(resp) = try_hydrate::<supply_service::aggregate::Inventory>(
        "Inventory",
        &agg_id,
        &snapshot,
        &events,
        total_version,
        snapshot_version,
        events_replayed,
    ) {
        return Ok(Json(resp));
    }

    if let Some(resp) = try_hydrate::<station_service::aggregate::Station>(
        "Station",
        &agg_id,
        &snapshot,
        &events,
        total_version,
        snapshot_version,
        events_replayed,
    ) {
        return Ok(Json(resp));
    }

    // If none of the aggregate types could hydrate, return the events as raw data
    // with a generic state indicating hydration failed.
    tracing::warn!(
        aggregate_id = %params.aggregate_id,
        "debug: could not hydrate as any known aggregate type"
    );

    Ok(Json(DebugAggregateResponse {
        aggregate_id: params.aggregate_id,
        aggregate_type: "unknown".to_string(),
        version: total_version,
        state: serde_json::json!({
            "error": "could not hydrate as any known aggregate type",
            "event_count": all_events.len(),
        }),
        snapshot_version,
        events_replayed,
    }))
}

/// GET /debug/events?aggregateId=<uuid>
///
/// Returns the full event history for the given aggregate ID with decoded payloads.
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
            aggregate_id: *e.aggregate_id.as_uuid(),
            version: e.version.as_u64(),
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
            aggregate_id: *cmd.aggregate_id.as_uuid(),
            command_type: cmd.command_type,
            command_version: cmd.command_version,
            payload: decode_payload(&cmd.payload),
            correlation_id: cmd.correlation_id,
            causation_id: cmd.causation_id,
            timestamp: cmd.timestamp,
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

/// Attempt to hydrate an aggregate from a snapshot + events.
///
/// Returns `Some(response)` if hydration succeeds, `None` if it fails (wrong
/// aggregate type for these events).
fn try_hydrate<A: Aggregate>(
    type_name: &str,
    agg_id: &AggregateId,
    snapshot: &Option<canon_core::Snapshot>,
    events: &[EventEnvelope],
    total_version: u64,
    snapshot_version: Option<u64>,
    events_replayed: u64,
) -> Option<DebugAggregateResponse>
where
    A::State: Default + serde::de::DeserializeOwned + serde::Serialize,
{
    let mut state: A::State = match snapshot {
        Some(snap) => serde_json::from_slice(&snap.state).unwrap_or_default(),
        None => A::State::default(),
    };

    // Clone events for hydration (events are borrowed from the caller)
    let events_owned: Vec<EventEnvelope> = events.to_vec();

    if A::hydrate(&mut state, events_owned.into_iter()).is_err() {
        return None;
    }

    let state_json = match serde_json::to_value(&state) {
        Ok(v) => v,
        Err(_) => return None,
    };

    Some(DebugAggregateResponse {
        aggregate_id: *agg_id.as_uuid(),
        aggregate_type: type_name.to_string(),
        version: total_version,
        state: state_json,
        snapshot_version,
        events_replayed,
    })
}

/// Attempt to decode a payload as JSON. Falls back to a string representation
/// if the payload is not valid JSON.
fn decode_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}
