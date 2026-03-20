use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use crate::command::{build_envelope, submit_command};
use crate::correlation::{extract_correlation_id, CORRELATION_HEADER};
use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{CommandAcceptedResponse, RegisterStationRequest, StationInventoryResponse};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stations/{id}/register", post(register_station))
        .route("/stations/{id}/inventory", get(station_inventory))
}

/// POST /stations/:id/register — RegisterStation command
async fn register_station(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RegisterStationRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);
    let envelope = build_envelope("RegisterStation", Some(id), corr_id, &body)?;

    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(&state.yugabyte_pool, "Station", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// GET /stations/:id/inventory — query station_inventory projection (read-ready)
async fn station_inventory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StationInventoryResponse>, GatewayError> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT state FROM projections \
         WHERE projection_id = 'station_inventory' AND aggregate_id = $1",
    )
    .bind(id)
    .fetch_optional(&state.yugabyte_pool)
    .await?;

    match row {
        Some((state_json,)) => {
            let response: StationInventoryResponse =
                serde_json::from_value(state_json).map_err(GatewayError::Serialization)?;
            Ok(Json(response))
        }
        None => Err(GatewayError::NotFound(format!(
            "station {id} inventory not found"
        ))),
    }
}
