//! GET /game/:session_id -- complete game state snapshot.
//!
//! Reads from the in-memory game projection (sub-ms). The projection is
//! maintained incrementally by Kafka consumers — no DB queries on the hot path.
//! Oversight is the one exception: queried from inbox_windows on each poll.
//!
//! Supports conditional requests: if the client sends `If-None-Match: <v>`
//! matching the current projection version, the response is `304 Not Modified`
//! with an empty body and no `inbox_windows` query is issued.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use uuid::Uuid;

use tracing::warn;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{OversightWindowResponse, RequirementResponse};

pub fn router() -> Router<AppState> {
    Router::new().route("/game/:session_id", get(game_snapshot))
}

/// GET /game/:session_id -- return game state from the in-memory projection.
async fn game_snapshot(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, GatewayError> {
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_etag);

    // Clone the projection Arc, update last_polled_at, and read current version
    // under a brief read lock. Release the session store + projection lock
    // before any DB work.
    let (projection, tracked_ids, current_version) = {
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
        let version = proj.version;
        drop(proj);

        (session.projection.clone(), ids, version)
    };

    // Fast path: client already has the latest projection version. Skip the
    // oversight DB query and return 304 with just an ETag.
    if if_none_match == Some(current_version) {
        return Ok(not_modified(current_version));
    }

    // Query oversight in parallel across every service pool. Each query is
    // independent; serialising them (prior behaviour) multiplied the DB cost
    // by the number of services per poll.
    let oversight = query_first_oversight_window(&state, &tracked_ids).await;

    // Brief write lock: set oversight and build the response. The version
    // read above is the lower bound — if a Kafka consumer applied an event
    // between the two locks, the response reflects the newer state (and a
    // higher ETag) which is fine.
    let mut proj = projection.write().await;
    proj.oversight = oversight;
    let etag_version = proj.version;
    let infra = state.infra_status.read().await;
    let response = proj.to_game_state_response(&infra);
    drop(proj);

    Ok(ok_with_etag(response, etag_version))
}

fn not_modified(version: u64) -> Response {
    let mut resp = StatusCode::NOT_MODIFIED.into_response();
    resp.headers_mut()
        .insert(header::ETAG, etag_header_value(version));
    resp
}

fn ok_with_etag(body: crate::types::GameStateResponse, version: u64) -> Response {
    let mut resp = Json(body).into_response();
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut()
        .insert(header::ETAG, etag_header_value(version));
    resp
}

fn etag_header_value(version: u64) -> HeaderValue {
    // Weak ETag — the body is semantically identical per-version but
    // oversight TTL seconds are time-derived and vary between polls.
    HeaderValue::from_str(&format!("W/\"{version}\"")).expect("numeric etag is always valid ascii")
}

fn parse_etag(header: &str) -> Option<u64> {
    let trimmed = header.trim().trim_start_matches("W/").trim_matches('"');
    trimmed.parse::<u64>().ok()
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
    // Fire one query per service pool in parallel. 5 services × sequential
    // ~10-50ms per query previously stacked to 50-250ms per poll; running
    // them concurrently caps the latency at the slowest single query.
    let queries = state.service_stores.values().map(|stores| {
        let pool = stores.pool.clone();
        async move {
            let rows: Result<Vec<WindowRow>, _> = sqlx::query_as(
                "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
                 FROM inbox_windows WHERE status = 'pending' ORDER BY created_at DESC LIMIT 20",
            )
            .fetch_all(&pool)
            .await;
            rows
        }
    });

    let results = join_all(queries).await;
    let mut candidates: Vec<WindowRow> = Vec::new();
    for res in results {
        match res {
            Ok(rows) => candidates.extend(rows),
            Err(e) => warn!(error = %e, "failed to query inbox_windows for oversight"),
        }
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

#[cfg(test)]
mod tests {
    use super::parse_etag;

    #[test]
    fn parses_strong_etag() {
        assert_eq!(parse_etag("\"42\""), Some(42));
    }

    #[test]
    fn parses_weak_etag() {
        assert_eq!(parse_etag("W/\"42\""), Some(42));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_etag("nope"), None);
        assert_eq!(parse_etag("\"\""), None);
    }
}
