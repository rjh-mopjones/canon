//! POST /sessions — create a new game session with fresh aggregate IDs.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::projection::GameProjection;
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

    // Generate aggregate IDs first. Commands are NOT submitted yet.
    let ids = session::allocate_session_ids();

    // Seed projection with capacity + zero stock so the drain task has
    // something to observe, and the Kafka consumer has a projection to
    // write events into.
    let projection = Arc::new(tokio::sync::RwLock::new(GameProjection::seeded(
        ids.clone(),
        crate::session::BOOTSTRAP_STATIONS,
    )));

    let drain_handle =
        session::spawn_session_drain(station_pool.clone(), ids.station_ids, projection.clone());

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

    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Insert session into the store BEFORE submitting bootstrap commands —
    // the Kafka consumer only applies events to sessions it can find in the
    // store. Previously, commands were submitted first and the resulting
    // ShipRegistered event reached the consumer before the session was
    // visible, so the event was dropped and the projection stayed ship-less.
    {
        let mut sessions = state.sessions.write().await;
        if sessions.len() >= 20 {
            drain_handle.abort();
            return Err(crate::error::GatewayError::Internal(
                "session limit reached (max 20 concurrent sessions)".to_owned(),
            ));
        }
        let session = LiveSession {
            ids: ids.clone(),
            drain_handle: Some(drain_handle),
            projection,
            last_polled_at: AtomicU64::new(now_millis),
            game_over: AtomicBool::new(false),
        };
        sessions.insert(ids.session_id, session);
    }

    // Session is now visible to the Kafka consumer. Submit bootstrap commands
    // in the background so this endpoint returns quickly; events will populate
    // the projection as they flow through the pipeline.
    let ids_bg = ids.clone();
    let station_pool_bg = station_pool.clone();
    let fleet_pool_bg = fleet_pool.clone();
    tokio::spawn(async move {
        session::submit_bootstrap_commands(&station_pool_bg, &fleet_pool_bg, &ids_bg).await;
    });

    info!(session_id = %ids.session_id, "session created");

    Ok(Json(response))
}
