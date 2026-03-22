use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use canon_core::AggregateId;
use canon_event_store::EventStore;
use canon_snapshot_store::SnapshotStore;

use crate::command::{build_envelope, submit_command};
use crate::correlation::{extract_correlation_id, CORRELATION_HEADER};
use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{
    AssignRouteRequest, CommandAcceptedResponse, DepartForStationRequest, RegisterShipRequest,
    ScheduleResupplyRequest, ShipStateResponse,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/ships", get(list_ships))
        .route("/fleet/ships", post(register_ship))
        .route("/fleet/ships/:id/route", post(assign_route))
        .route("/fleet/ships/:id/depart", post(depart_for_station))
        .route("/fleet/ships/:id/resupply", post(schedule_resupply))
        .route("/fleet/ships/:id/decommission", post(decommission_ship))
        .route("/ships/:id/history", get(ship_history))
}

/// POST /fleet/ships — RegisterShip command
async fn register_ship(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RegisterShipRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);
    let envelope = build_envelope("RegisterShip", None, corr_id, &body)?;

    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: *envelope.aggregate_id.as_uuid(),
        correlation_id: corr_id,
    };

    submit_command(state.pool_for_service("fleet"), "Ship", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /fleet/ships/:id/route — AssignRoute command
async fn assign_route(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<AssignRouteRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        ship_id: Uuid,
        route_id: Uuid,
    }
    let payload = Payload {
        ship_id: id,
        route_id: body.route_id,
    };

    let envelope = build_envelope("AssignRoute", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(state.pool_for_service("fleet"), "Ship", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /fleet/ships/:id/depart — DepartForStation command
async fn depart_for_station(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<DepartForStationRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        ship_id: Uuid,
        destination: Uuid,
    }
    let payload = Payload {
        ship_id: id,
        destination: body.destination,
    };

    let envelope = build_envelope("DepartForStation", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(state.pool_for_service("fleet"), "Ship", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /fleet/ships/:id/resupply — ScheduleResupply command
async fn schedule_resupply(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ScheduleResupplyRequest>,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        ship_id: Uuid,
        fuel_kg: f32,
    }
    let payload = Payload {
        ship_id: id,
        fuel_kg: body.fuel_kg,
    };

    let envelope = build_envelope("ScheduleResupply", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(state.pool_for_service("fleet"), "Ship", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// POST /fleet/ships/:id/decommission — DecommissionShip command
async fn decommission_ship(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<CommandAcceptedResponse>), GatewayError> {
    let corr_id = extract_correlation_id(&headers);

    #[derive(serde::Serialize)]
    struct Payload {
        ship_id: Uuid,
    }
    let payload = Payload { ship_id: id };

    let envelope = build_envelope("DecommissionShip", Some(id), corr_id, &payload)?;
    let response = CommandAcceptedResponse {
        command_id: envelope.command_id,
        aggregate_id: id,
        correlation_id: corr_id,
    };

    submit_command(state.pool_for_service("fleet"), "Ship", &envelope).await?;

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CORRELATION_HEADER,
        corr_id.to_string().parse().map_err(|_| {
            GatewayError::Internal("failed to format correlation header".to_owned())
        })?,
    );

    Ok((resp_headers, Json(response)))
}

/// GET /ships — list all ships with current state (hydrated from events)
async fn list_ships(
    State(state): State<AppState>,
) -> Result<Json<Vec<ShipStateResponse>>, GatewayError> {
    // Find all ship aggregate IDs by querying RegisterShip commands (fleet schema)
    let fleet_pool = state.pool_for_service("fleet");
    let fleet_event_store = state.event_store_for_service("fleet");
    let fleet_snapshot_store = state.snapshot_store_for_service("fleet");

    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT DISTINCT aggregate_id FROM commands WHERE command_type = 'RegisterShip'",
    )
    .fetch_all(fleet_pool)
    .await?;

    let mut ships = Vec::with_capacity(rows.len());

    for (agg_uuid,) in rows {
        let agg_id = AggregateId::from_uuid(agg_uuid);
        let events = fleet_event_store.load(&agg_id).await?;

        let snapshot = fleet_snapshot_store.load(&agg_id).await.ok().flatten();
        let last_snapshot_version = snapshot.as_ref().map(|s| s.version.as_u64()).unwrap_or(0);

        let ship_state = hydrate_ship_from_events(&events, agg_uuid);
        let aggregate_version = events.last().map(|e| e.version.as_u64()).unwrap_or(0);
        let correlation_id = events
            .last()
            .map(|e| e.correlation_id)
            .unwrap_or_else(Uuid::new_v4);

        ships.push(ShipStateResponse {
            id: agg_uuid,
            name: ship_state.name,
            status: ship_state.status,
            station_id: ship_state.station_id,
            route_label: ship_state.route_label,
            fuel_pct: ship_state.fuel_pct,
            aggregate_version,
            last_snapshot_version,
            correlation_id,
        });
    }

    Ok(Json(ships))
}

/// GET /ships/:id/history — load event history from Cassandra
async fn ship_history(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<crate::types::EventHistoryEntry>>, GatewayError> {
    let agg_id = AggregateId::from_uuid(id);
    let events = state.event_store_for_service("fleet").load(&agg_id).await?;

    if events.is_empty() {
        return Err(GatewayError::NotFound(format!("ship {id} not found")));
    }

    let entries = events
        .into_iter()
        .map(|e| {
            let payload: serde_json::Value =
                serde_json::from_slice(&e.payload).unwrap_or(serde_json::Value::Null);
            crate::types::EventHistoryEntry {
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

// ── Ship state hydration ────────────────────────────────────────────────────

struct HydratedShip {
    name: String,
    status: String,
    station_id: Option<Uuid>,
    route_label: String,
    fuel_pct: u32,
}

fn hydrate_ship_from_events(events: &[canon_core::EventEnvelope], _agg_id: Uuid) -> HydratedShip {
    let mut name = String::new();
    let mut status = "unknown".to_owned();
    let mut station_id: Option<Uuid> = None;
    let mut route_label = String::new();
    let mut fuel_level: f32 = 100.0;
    let mut capacity: f32 = 1.0;

    for event in events {
        match event.event_type.as_str() {
            "ShipRegistered" => {
                #[derive(serde::Deserialize)]
                struct E {
                    name: String,
                    capacity_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    name = e.name;
                    capacity = e.capacity_kg;
                    status = "docked".to_owned();
                    fuel_level = capacity;
                }
            }
            "RouteAssigned" => {
                #[derive(serde::Deserialize)]
                struct E {
                    route_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    route_label = e.route_id.to_string();
                }
            }
            "ShipDeparted" => {
                #[derive(serde::Deserialize)]
                struct E {
                    destination: Uuid,
                    fuel_at_departure: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    status = "transit".to_owned();
                    station_id = Some(e.destination);
                    fuel_level -= e.fuel_at_departure * 0.1;
                }
            }
            "ShipDecommissioned" => {
                status = "dead".to_owned();
            }
            "ResupplyScheduled" => {
                #[derive(serde::Deserialize)]
                struct E {
                    fuel_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&event.payload) {
                    fuel_level = (fuel_level + e.fuel_kg).min(capacity);
                }
            }
            _ => {}
        }
    }

    let fuel_pct = if capacity > 0.0 {
        ((fuel_level / capacity) * 100.0).clamp(0.0, 100.0) as u32
    } else {
        0
    };

    HydratedShip {
        name,
        status,
        station_id,
        route_label,
        fuel_pct,
    }
}
