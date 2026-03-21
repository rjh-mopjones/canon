use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{DeadLetterResponse, OversightWindowResponse, RequirementResponse};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/admin/oversight/windows", get(list_oversight_windows))
        .route("/admin/deadletters", get(list_dead_letters))
        .route("/admin/deadletters/:id/requeue", post(requeue_dead_letter))
        .route("/admin/deadletters/:id", delete(discard_dead_letter))
}

// ── Row types for sqlx::query_as ────────────────────────────────────────────

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

#[derive(sqlx::FromRow)]
struct DeadLetterRow {
    id: Uuid,
    handler_id: Option<String>,
    aggregate_id: Option<Uuid>,
    error: Option<String>,
    attempts: i32,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct RequeueRow {
    message_id: Uuid,
    handler_id: Option<String>,
    aggregate_id: Option<Uuid>,
    payload: Vec<u8>,
}

// ── Handlers ────────────────────────────────────────────────────────────────

/// GET /admin/oversight/windows — list pending inbox windows
async fn list_oversight_windows(
    State(state): State<AppState>,
) -> Result<Json<Vec<OversightWindowResponse>>, GatewayError> {
    let rows: Vec<WindowRow> = sqlx::query_as(
        "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
         FROM inbox_windows WHERE status = 'pending' \
         ORDER BY created_at DESC",
    )
    .fetch_all(&state.yugabyte_pool)
    .await?;

    let windows = rows
        .into_iter()
        .map(|row| {
            let now = Utc::now();
            let ttl_total_secs = row
                .expires_at
                .map(|exp| (exp - row.created_at).num_seconds().max(0) as u32)
                .unwrap_or(0);
            let ttl_remaining_secs = row
                .expires_at
                .map(|exp| (exp - now).num_seconds().max(0) as u32)
                .unwrap_or(0);

            // Derive requirement status from accumulated messages in the window.
            // The messages column is a JSONB array of message objects, each with a
            // "message_type" or "event_type" field indicating the kind of message.
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

            OversightWindowResponse {
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
            }
        })
        .collect();

    Ok(Json(windows))
}

/// GET /admin/deadletters — list dead letter entries
async fn list_dead_letters(
    State(state): State<AppState>,
) -> Result<Json<Vec<DeadLetterResponse>>, GatewayError> {
    let rows: Vec<DeadLetterRow> = sqlx::query_as(
        "SELECT id, handler_id, aggregate_id, error, attempts, created_at \
         FROM dead_letters ORDER BY created_at DESC",
    )
    .fetch_all(&state.yugabyte_pool)
    .await?;

    let entries = rows
        .into_iter()
        .map(|row| {
            let service = row.handler_id.unwrap_or_else(|| "unknown".to_owned());

            DeadLetterResponse {
                id: row.id,
                event_type: "unknown".to_owned(),
                service,
                aggregate_id: row.aggregate_id.map(|a| a.to_string()).unwrap_or_default(),
                error: row.error.unwrap_or_default(),
                attempts: row.attempts as u32,
                requeued: false,
                created_at: row.created_at.to_rfc3339(),
            }
        })
        .collect();

    Ok(Json(entries))
}

/// POST /admin/deadletters/:id/requeue — requeue a dead letter
///
/// Both the inbox re-insertion and dead letter deletion happen in a single
/// transaction to avoid inconsistency if the process crashes mid-operation.
async fn requeue_dead_letter(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, GatewayError> {
    let row: Option<RequeueRow> = sqlx::query_as(
        "SELECT message_id, handler_id, aggregate_id, payload \
         FROM dead_letters WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.yugabyte_pool)
    .await?;

    let row = row.ok_or_else(|| GatewayError::NotFound(format!("dead letter {id} not found")))?;

    let handler_id = row.handler_id.unwrap_or_default();

    let mut tx = state.yugabyte_pool.begin().await?;

    // Re-insert into inbox_messages with a fresh message_id so the requeued
    // message is treated as a new attempt (the original message_id may still
    // exist in inbox_messages from the failed processing run).
    let fresh_message_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
         VALUES ($1, $2, $3, 'requeued', $4) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&handler_id)
    .bind(fresh_message_id)
    .bind(row.aggregate_id)
    .bind(&row.payload)
    .execute(&mut *tx)
    .await?;

    // Clear retry attempts for the original message so the requeued copy
    // does not inherit the previous failure count.
    sqlx::query("DELETE FROM retry_attempts WHERE message_id = $1")
        .bind(row.message_id)
        .execute(&mut *tx)
        .await?;

    // Remove from dead_letters
    sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /admin/deadletters/:id — discard a dead letter
async fn discard_dead_letter(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, GatewayError> {
    let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(id)
        .execute(&state.yugabyte_pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(GatewayError::NotFound(format!(
            "dead letter {id} not found"
        )));
    }

    Ok(StatusCode::NO_CONTENT)
}
