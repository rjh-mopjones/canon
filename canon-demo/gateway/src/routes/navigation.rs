use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};

use crate::command::{build_envelope, submit_command};
use crate::correlation::{extract_correlation_id, CORRELATION_HEADER};
use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{CommandAcceptedResponse, PlanRouteRequest};

pub fn router() -> Router<AppState> {
    Router::new().route("/navigation/routes", post(plan_route))
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
