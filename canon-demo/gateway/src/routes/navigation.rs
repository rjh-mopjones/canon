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
use crate::types::{CommandAcceptedResponse, EventHistoryEntry, PlanRouteRequest};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/navigation/routes", post(plan_route))
        .route("/navigation/routes/:id", get(route_history))
}

/// POST /navigation/routes — PlanRoute command
async fn plan_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<PlanRouteRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);
    let envelope = build_envelope("PlanRoute", None, corr_id, &body)?;

    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: *envelope.aggregate_id.as_uuid(),
        correlation_id: corr_id,
    };

    submit_command(&state.yugabyte_pool, "Route", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// GET /navigation/routes/:id — load route event history from Cassandra
async fn route_history(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<EventHistoryEntry>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(id);
    let events = state.event_store.load(&agg_id).await?;

    if events.is_empty() {
        return Err(GatewayError::NotFound(format!("route {id} not found")));
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
