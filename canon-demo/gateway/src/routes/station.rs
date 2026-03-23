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
    CommandAcceptedResponse, RecordCargoReceivedRequest, RecordDockingRequest,
    RegisterStationRequest, StationInventoryResponse, StationStateResponse,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stations", get(list_stations))
        .route("/stations/:id/register", post(register_station))
        .route("/stations/:id/dock", post(record_docking))
        .route("/stations/:id/cargo", post(record_cargo_received))
        .route("/stations/:id/inventory", get(station_inventory))
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

    submit_command(state.pool_for_service("station"), "Station", &envelope).await?;

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

    submit_command(state.pool_for_service("station"), "Station", &envelope).await?;

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

    submit_command(state.pool_for_service("station"), "Station", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// GET /stations — list all stations by hydrating from the Cassandra event store
///
/// Finds all station aggregate IDs by querying RegisterStation commands in the
/// station service's commands table, then loads events from Cassandra and
/// hydrates each station's state in-process. This is the read-through approach
/// — it bypasses the (currently unregistered) projection consumer entirely.
async fn list_stations(
    State(state): State<AppState>,
) -> Result<Json<Vec<StationStateResponse>>, GatewayError> {
    let station_pool = state.pool_for_service("station");
    let station_event_store = state.event_store_for_service("station");

    // Get the most recent 4 registered stations (latest game session).
    // Each gateway restart creates fresh aggregates — we only want the newest.
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT aggregate_id FROM commands WHERE command_type = 'RegisterStation' \
         ORDER BY created_at DESC LIMIT 4",
    )
    .fetch_all(station_pool)
    .await?;

    let mut stations = Vec::with_capacity(rows.len());

    for (agg_uuid,) in rows {
        let agg_id = AggregateId::from_uuid(agg_uuid);
        let events = station_event_store.load(&agg_id).await?;

        let hydrated = hydrate_station_from_events(&events);
        // Only include stations that have actually been registered (have at
        // least one StationRegistered event).
        if hydrated.registered {
            stations.push(StationStateResponse {
                id: agg_uuid,
                name: hydrated.name,
                capacity_kg: hydrated.capacity_kg,
                current_stock_kg: hydrated.current_stock_kg,
            });
        }
    }

    // Sort by name for stable ordering (matches previous ORDER BY name)
    stations.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Json(stations))
}

/// GET /stations/:id/inventory — hydrate station state from Cassandra event store
///
/// Read-through endpoint: loads events for the given station aggregate from
/// Cassandra and hydrates the current state. This avoids depending on the
/// projection consumer (which currently has no projections registered).
async fn station_inventory(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<StationInventoryResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(id);
    let events = state
        .event_store_for_service("station")
        .load(&agg_id)
        .await?;

    if events.is_empty() {
        return Err(GatewayError::NotFound(format!(
            "station {id} inventory not found"
        )));
    }

    let hydrated = hydrate_station_from_events(&events);
    Ok(Json(StationInventoryResponse {
        station_id: id,
        name: hydrated.name,
        capacity_kg: hydrated.capacity_kg,
        current_stock_kg: hydrated.current_stock_kg,
    }))
}

// ── Station state hydration ────────────────────────────────────────────────

struct HydratedStation {
    name: String,
    capacity_kg: f32,
    current_stock_kg: f32,
    registered: bool,
}

fn hydrate_station_from_events(events: &[canon_core::EventEnvelope]) -> HydratedStation {
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
