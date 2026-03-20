use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use crate::command::{build_envelope, submit_command};
use crate::correlation::{extract_correlation_id, CORRELATION_HEADER};
use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{
    CommandAcceptedResponse, RecordCargoReceivedRequest, RecordDockingRequest,
    RegisterStationRequest, StationInventoryResponse, StationStateResponse,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stations", get(list_stations))
        .route("/stations/{id}/register", post(register_station))
        .route("/stations/{id}/dock", post(record_docking))
        .route("/stations/{id}/cargo", post(record_cargo_received))
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

/// POST /stations/:id/dock — RecordDocking command
async fn record_docking(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RecordDockingRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        station_id: Uuid,
        ship_id: Uuid,
    }
    let payload = Payload {
        station_id: id,
        ship_id: body.ship_id,
    };

    let envelope = build_envelope("RecordDocking", Some(id), corr_id, &payload)?;
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

/// POST /stations/:id/cargo — RecordCargoReceived command
async fn record_cargo_received(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RecordCargoReceivedRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        station_id: Uuid,
        manifest_id: Uuid,
        weight_kg: f32,
    }
    let payload = Payload {
        station_id: id,
        manifest_id: body.manifest_id,
        weight_kg: body.weight_kg,
    };

    let envelope = build_envelope("RecordCargoReceived", Some(id), corr_id, &payload)?;
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

/// GET /stations — list all stations from the station_inventory projection
///
/// Returns all stations with their current state. Used by the frontend for
/// initial hydration on mount.
async fn list_stations(
    State(state): State<AppState>,
) -> Result<Json<Vec<StationStateResponse>>, GatewayError> {
    let rows: Vec<StationInventoryResponse> = sqlx::query_as(
        "SELECT station_id, name, capacity_kg, current_stock_kg \
         FROM station_inventory ORDER BY name",
    )
    .fetch_all(&state.yugabyte_pool)
    .await?;

    let stations = rows
        .into_iter()
        .map(|row| StationStateResponse {
            id: row.station_id,
            name: row.name,
            capacity_kg: row.capacity_kg,
            current_stock_kg: row.current_stock_kg,
        })
        .collect();

    Ok(Json(stations))
}

/// GET /stations/:id/inventory — query station_inventory projection (read-ready)
///
/// Reads from the `station_inventory` projection table, which is maintained
/// by the station service's projection consumer via `canon-projection-store-yugabyte`.
async fn station_inventory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StationInventoryResponse>, GatewayError> {
    let row: Option<StationInventoryResponse> = sqlx::query_as(
        "SELECT station_id, name, capacity_kg, current_stock_kg \
         FROM station_inventory WHERE station_id = $1",
    )
    .bind(id)
    .fetch_optional(&state.yugabyte_pool)
    .await?;

    match row {
        Some(response) => Ok(Json(response)),
        None => Err(GatewayError::NotFound(format!(
            "station {id} inventory not found"
        ))),
    }
}
