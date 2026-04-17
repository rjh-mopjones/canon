//! GET /game/:session_id -- complete game state snapshot.
//!
//! Reads from the in-memory game projection (sub-ms). The projection is
//! maintained incrementally by Kafka consumers — no DB queries on the hot path.
//! Oversight is the one exception: queried from inbox_windows on each poll.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use tracing::warn;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{GameStateResponse, OversightWindowResponse, RequirementResponse};

pub fn router() -> Router<AppState> {
    Router::new().route("/game/:session_id", get(game_snapshot))
}

/// GET /game/:session_id -- return game state from the in-memory projection.
async fn game_snapshot(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<GameStateResponse>, GatewayError> {
    // Clone the projection Arc and update last_polled_at, then release the
    // session store lock before doing any DB queries.
    let (projection, tracked_ids) = {
        let sessions = state.sessions.read().await;
        let session = sessions
            .get(&session_id)
            .ok_or_else(|| GatewayError::NotFound(format!("session {session_id} not found")))?;

        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        session.last_polled_at.store(now_millis, Ordering::Relaxed);

        let proj = session.projection.read().await;
        let ids = proj.tracked_ids.clone();
        drop(proj);

        (session.projection.clone(), ids)
    };

    // Query oversight OUTSIDE any projection lock — this is a DB query that
    // can take 10-50ms. Previously we held a write lock during this query,
    // which blocked Kafka consumers from applying events to the projection
    // and caused intermittent timeouts under concurrent multi-session load.
    let oversight = query_first_oversight_window(&state, &tracked_ids).await;

    // Brief write lock just to set oversight, then immediately build response
    // and release. The Kafka consumer only contends for this lock for
    // microseconds instead of the full DB query duration.
    let mut proj = projection.write().await;
    proj.oversight = oversight;
    let infra = state.infra_status.read().await;
    let response = proj.to_game_state_response(&infra);
    Ok(Json(response))
}

// ── Oversight window query ─────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct WindowRow {
    handler_id: String,
    correlation_key: Uuid,
    window_id: Uuid,
    messages: serde_json::Value,
    status: String,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

async fn query_first_oversight_window(
    state: &AppState,
    tracked_ids: &HashSet<Uuid>,
) -> Option<OversightWindowResponse> {
    let mut candidates = Vec::new();

    for stores in state.service_stores.values() {
        let rows: Vec<WindowRow> = match sqlx::query_as(
            "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
             FROM inbox_windows WHERE status = 'pending' ORDER BY created_at DESC LIMIT 20",
        )
        .fetch_all(&stores.pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to query inbox_windows for oversight");
                continue;
            }
        };
        candidates.extend(rows);
    }

    candidates.sort_by_key(|c| std::cmp::Reverse(c.created_at));

    candidates.into_iter().find_map(|row| {
        if !json_contains_any_uuid(&row.messages, tracked_ids) {
            return None;
        }

        let now = Utc::now();
        let ttl_total_secs = row
            .expires_at
            .map(|exp| (exp - row.created_at).num_seconds().max(0) as u32)
            .unwrap_or(0);
        let ttl_remaining_secs = row
            .expires_at
            .map(|exp| (exp - now).num_seconds().max(0) as u32)
            .unwrap_or(0);

        let messages_arr = row.messages.as_array();
        let has_event_type = |event_type: &str| -> bool {
            messages_arr
                .map(|arr| {
                    arr.iter().any(|m| {
                        m.get("event_type")
                            .and_then(|v| v.as_str())
                            .is_some_and(|t| t == event_type)
                            || m.get("message_type")
                                .and_then(|v| v.as_str())
                                .is_some_and(|t| t == event_type)
                    })
                })
                .unwrap_or(false)
        };

        Some(OversightWindowResponse {
            window_id: row.window_id,
            handler_id: row.handler_id,
            correlation_key: row.correlation_key,
            ship_name: String::new(),
            dest_label: String::new(),
            status: row.status,
            requirements: vec![
                RequirementResponse {
                    label: "ShipArrivedAtStation".to_owned(),
                    met: has_event_type("ShipArrivedAtStation"),
                },
                RequirementResponse {
                    label: "ManifestCreated".to_owned(),
                    met: has_event_type("ManifestCreated"),
                },
            ],
            ttl_remaining_secs,
            ttl_total_secs,
        })
    })
}

fn json_contains_any_uuid(value: &serde_json::Value, tracked_ids: &HashSet<Uuid>) -> bool {
    match value {
        serde_json::Value::String(text) => Uuid::parse_str(text)
            .ok()
            .is_some_and(|uuid| tracked_ids.contains(&uuid)),
        serde_json::Value::Array(items) => items
            .iter()
            .any(|item| json_contains_any_uuid(item, tracked_ids)),
        serde_json::Value::Object(map) => map
            .values()
            .any(|item| json_contains_any_uuid(item, tracked_ids)),
        _ => false,
    }
}
