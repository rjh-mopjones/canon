use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use canon_core::AggregateId;
use canon_event_store::EventStore;

use crate::command::{build_envelope, submit_command};
use crate::correlation::{extract_correlation_id, CORRELATION_HEADER};
use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{
    BeginUnloadingRequest, CommandAcceptedResponse, CreateManifestRequest, EventHistoryEntry,
    LoadCargoRequest,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/cargo/manifests", post(create_manifest))
        .route("/cargo/manifests/:id/load", post(load_cargo))
        .route("/cargo/manifests/:id/unload", post(begin_unloading))
        .route("/cargo/manifests/:id", get(manifest_history))
}

/// POST /cargo/manifests — CreateManifest command
async fn create_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateManifestRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);
    let envelope = build_envelope("CreateManifest", None, corr_id, &body)?;

    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: *envelope.aggregate_id.as_uuid(),
        correlation_id: corr_id,
    };

    submit_command(&state.yugabyte_pool, "Manifest", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /cargo/manifests/:id/load — LoadCargo command
async fn load_cargo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<LoadCargoRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        manifest_id: Uuid,
        item_id: Uuid,
        weight_kg: f32,
        description: String,
    }
    let payload = Payload {
        manifest_id: id,
        item_id: body.item_id,
        weight_kg: body.weight_kg,
        description: body.description,
    };

    let envelope = build_envelope("LoadCargo", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(&state.yugabyte_pool, "Manifest", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /cargo/manifests/:id/unload — BeginUnloading command
async fn begin_unloading(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<BeginUnloadingRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        manifest_id: Uuid,
        station_id: Uuid,
    }
    let payload = Payload {
        manifest_id: id,
        station_id: body.station_id,
    };

    let envelope = build_envelope("BeginUnloading", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(&state.yugabyte_pool, "Manifest", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// GET /cargo/manifests/:id — load manifest event history from Cassandra
async fn manifest_history(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<EventHistoryEntry>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(id);
    let events = state.event_store.load(&agg_id).await?;

    if events.is_empty() {
        return Err(GatewayError::NotFound(format!("manifest {id} not found")));
    }

    let entries = events
        .into_iter()
        .map(|e| {
            let payload: serde_json::Value =
                serde_json::from_slice(&e.payload).unwrap_or(serde_json::Value::Null);
            EventHistoryEntry {
                event_id: e.event_id,
                version: e.version.as_u64(),
                event_type: e.event_type,
                event_version: e.event_version,
                correlation_id: e.correlation_id,
                timestamp: e.timestamp.to_rfc3339(),
                payload,
            }
        })
        .collect();

    Ok(Json(entries))
}
