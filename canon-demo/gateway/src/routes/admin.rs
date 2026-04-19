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
        .route("/admin/deadletters/purge", post(purge_dead_letters))
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

/// GET /admin/oversight/windows — list pending inbox windows from all services
async fn list_oversight_windows(
    State(state): State<AppState>,
) -> Result<Json<Vec<OversightWindowResponse>>, GatewayError> {
    // Collect (created_at, response) tuples so we can sort by creation time
    // across services, matching the original `ORDER BY created_at DESC`.
    let mut all_windows: Vec<(DateTime<Utc>, OversightWindowResponse)> = Vec::new();

    // Query each service's schema for pending inbox windows
    for (service_name, stores) in &state.service_stores {
        let rows: Vec<WindowRow> = sqlx::query_as(
            "SELECT handler_id, correlation_key, window_id, messages, status, expires_at, created_at \
             FROM inbox_windows WHERE status = 'pending' \
             ORDER BY created_at DESC",
        )
        .fetch_all(&stores.pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(service = %service_name, error = %e, "failed to query inbox_windows");
            Vec::new()
        });

        for row in rows {
            let now = Utc::now();
            let created_at = row.created_at;
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

            all_windows.push((
                created_at,
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
                },
            ));
        }
    }

    // Sort by most recent first (created_at DESC), matching the original SQL ordering
    all_windows.sort_by_key(|w| std::cmp::Reverse(w.0));
    let windows = all_windows.into_iter().map(|(_, w)| w).collect();

    Ok(Json(windows))
}

/// GET /admin/deadletters — list dead letter entries from all services
async fn list_dead_letters(
    State(state): State<AppState>,
) -> Result<Json<Vec<DeadLetterResponse>>, GatewayError> {
    // Collect (created_at, response) tuples so we can sort by actual DateTime
    // rather than RFC3339 strings.
    let mut all_entries: Vec<(DateTime<Utc>, DeadLetterResponse)> = Vec::new();

    for (service_name, stores) in &state.service_stores {
        let rows: Vec<DeadLetterRow> = sqlx::query_as(
            "SELECT id, handler_id, aggregate_id, error, attempts, created_at \
             FROM dead_letters ORDER BY created_at DESC",
        )
        .fetch_all(&stores.pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(service = %service_name, error = %e, "failed to query dead_letters");
            Vec::new()
        });

        for row in rows {
            let service = row.handler_id.unwrap_or_else(|| service_name.clone());
            let created_at = row.created_at;
            let error = row.error.unwrap_or_default();
            let event_type = classify_dead_letter(&error);

            all_entries.push((
                created_at,
                DeadLetterResponse {
                    id: row.id,
                    event_type,
                    service,
                    aggregate_id: row.aggregate_id.map(|a| a.to_string()).unwrap_or_default(),
                    error,
                    attempts: row.attempts as u32,
                    requeued: false,
                    created_at: created_at.to_rfc3339(),
                },
            ));
        }
    }

    // Sort by most recent first (created_at DESC)
    all_entries.sort_by_key(|e| std::cmp::Reverse(e.0));
    let entries = all_entries.into_iter().map(|(_, e)| e).collect();

    Ok(Json(entries))
}

/// POST /admin/deadletters/:id/requeue — requeue a dead letter
///
/// Searches all service schemas for the dead letter, then requeues it in
/// the same service's inbox and removes it from the dead letter table.
async fn requeue_dead_letter(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, GatewayError> {
    // Find the dead letter across all service schemas
    for stores in state.service_stores.values() {
        let row: Option<RequeueRow> = sqlx::query_as(
            "SELECT message_id, handler_id, aggregate_id, payload \
             FROM dead_letters WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&stores.pool)
        .await?;

        if let Some(row) = row {
            let handler_id = row.handler_id.unwrap_or_default();

            let mut tx = stores.pool.begin().await?;

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

            sqlx::query("DELETE FROM retry_attempts WHERE message_id = $1")
                .bind(row.message_id)
                .execute(&mut *tx)
                .await?;

            sqlx::query("DELETE FROM dead_letters WHERE id = $1")
                .bind(id)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            return Ok(StatusCode::NO_CONTENT);
        }
    }

    Err(GatewayError::NotFound(format!(
        "dead letter {id} not found"
    )))
}

/// DELETE /admin/deadletters/:id — discard a dead letter
async fn discard_dead_letter(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, GatewayError> {
    // Search all service schemas for the dead letter
    for stores in state.service_stores.values() {
        let result = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
            .bind(id)
            .execute(&stores.pool)
            .await?;

        if result.rows_affected() > 0 {
            return Ok(StatusCode::NO_CONTENT);
        }
    }

    Err(GatewayError::NotFound(format!(
        "dead letter {id} not found"
    )))
}

/// POST /admin/deadletters/purge — delete all dead letters older than `older_than_hours`
///
/// Body: `{ "older_than_hours": 24 }` (optional, default 24).
/// Returns the count deleted per service. This is the only way to clear the
/// accumulated historical dead letters without `kubectl exec` onto the DB.
#[derive(serde::Deserialize, Default)]
struct PurgeBody {
    #[serde(default)]
    older_than_hours: Option<i64>,
}

#[derive(serde::Serialize)]
struct PurgeResponse {
    deleted: u64,
    older_than_hours: i64,
}

async fn purge_dead_letters(
    State(state): State<AppState>,
    body: Option<Json<PurgeBody>>,
) -> Result<Json<PurgeResponse>, GatewayError> {
    let hours = body
        .and_then(|Json(b)| b.older_than_hours)
        .unwrap_or(24)
        .max(0);
    let cutoff = Utc::now() - chrono::Duration::hours(hours);

    let mut deleted_total: u64 = 0;
    for (service_name, stores) in &state.service_stores {
        match sqlx::query("DELETE FROM dead_letters WHERE created_at < $1")
            .bind(cutoff)
            .execute(&stores.pool)
            .await
        {
            Ok(result) => deleted_total += result.rows_affected(),
            Err(e) => {
                tracing::warn!(
                    service = %service_name,
                    error = %e,
                    "dead letter purge failed for service"
                );
            }
        }
    }

    Ok(Json(PurgeResponse {
        deleted: deleted_total,
        older_than_hours: hours,
    }))
}

/// Extract the command type from a dead letter error message.
///
/// Errors follow a stable shape, e.g.
/// `command handler failed for 'DockShip' v1: ship is not in transit` or
/// `version conflict after 3 attempts: ...`. Pulls the type out of the
/// quoted name, or classifies the category when no type is present.
fn classify_dead_letter(error: &str) -> String {
    if let Some(start) = error.find('\'') {
        if let Some(end) = error[start + 1..].find('\'') {
            return error[start + 1..start + 1 + end].to_owned();
        }
    }
    if error.contains("version conflict") {
        return "version_conflict".to_owned();
    }
    if error.contains("window_expired") {
        return "window_expired".to_owned();
    }
    "unknown".to_owned()
}

#[cfg(test)]
mod tests {
    use super::classify_dead_letter;

    #[test]
    fn classifies_command_handler_error() {
        assert_eq!(
            classify_dead_letter(
                "command handler failed for 'DockShip' v1: ship is not in transit"
            ),
            "DockShip"
        );
    }

    #[test]
    fn classifies_version_conflict() {
        assert_eq!(
            classify_dead_letter("version conflict after 3 attempts: version conflict: expected Version(0), found Version(1)"),
            "version_conflict"
        );
    }

    #[test]
    fn classifies_window_expired() {
        assert_eq!(
            classify_dead_letter("window_expired after 30m"),
            "window_expired"
        );
    }

    #[test]
    fn classifies_unknown_gracefully() {
        assert_eq!(classify_dead_letter("something exploded"), "unknown");
    }
}
