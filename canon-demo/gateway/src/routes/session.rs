//! POST /sessions — create a new game session with fresh aggregate IDs.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::info;

use crate::session::{self, LiveSession};
use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/sessions", post(create_session))
}

#[derive(serde::Serialize)]
pub struct CreateSessionResponse {
    pub session_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
    pub stations: Vec<SessionStationInfo>,
}

#[derive(serde::Serialize)]
pub struct SessionStationInfo {
    pub id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub initial_stock_pct: f64,
}

async fn create_session(
    State(state): State<AppState>,
) -> Result<Json<CreateSessionResponse>, crate::error::GatewayError> {
    let station_pool = state.pool_for_service("station").clone();
    let fleet_pool = state.pool_for_service("fleet").clone();

    // Bootstrap fresh aggregates
    let ids = session::bootstrap_session(&station_pool, &fleet_pool).await;

    // Spawn per-session drain task
    let drain_handle = session::spawn_session_drain(station_pool, ids.station_ids);

    // Build response
    let stations: Vec<SessionStationInfo> = session::BOOTSTRAP_STATIONS
        .iter()
        .enumerate()
        .map(|(i, bs)| SessionStationInfo {
            id: ids.station_ids[i],
            name: bs.name.to_owned(),
            capacity_kg: bs.capacity_kg as f32,
            initial_stock_pct: bs.initial_stock_pct,
        })
        .collect();

    let response = CreateSessionResponse {
        session_id: ids.session_id,
        ship_id: ids.ship_id,
        stations,
    };

    // Store session — single write lock for both limit check and insert to
    // avoid TOCTOU race where two concurrent requests both pass the read
    // check and then both insert.
    {
        let mut sessions = state.sessions.write().await;
        if sessions.len() >= 20 {
            // Abort the drain task we just spawned since we're rejecting.
            drain_handle.abort();
            return Err(crate::error::GatewayError::Internal(
                "session limit reached (max 20 concurrent sessions)".to_owned(),
            ));
        }
        let session = LiveSession {
            ids: ids.clone(),
            drain_handle: Some(drain_handle),
            ws_connected: Arc::new(AtomicBool::new(false)),
        };
        sessions.insert(ids.session_id, session);
    }

    info!(session_id = %ids.session_id, "session created");

    Ok(Json(response))
}
