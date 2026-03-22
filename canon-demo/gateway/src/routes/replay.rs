use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use uuid::Uuid;

use canon_core::AggregateId;
use canon_event_store::EventStore;

use crate::error::GatewayError;
use crate::state::AppState;
use crate::types::{CommandDiffResponse, CommandEnvelopeResponse, CounterfactualQuery};

pub fn router() -> Router<AppState> {
    Router::new().route("/replay/counterfactual", get(counterfactual_replay))
}

#[derive(sqlx::FromRow)]
struct CommandRow {
    command_id: Uuid,
    aggregate_id: Uuid,
    command_type: String,
    command_version: i32,
    correlation_id: Uuid,
    causation_id: Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// GET /replay/counterfactual — run counterfactual replay, return CommandDiff JSON
///
/// Query params: aggregate_id, branch_version, command_type, command_payload
///
/// Searches all service schemas for the aggregate's commands and events.
async fn counterfactual_replay(
    State(state): State<AppState>,
    Query(query): Query<CounterfactualQuery>,
) -> Result<Json<CommandDiffResponse>, GatewayError> {
    let agg_id = AggregateId::from_uuid(query.aggregate_id);

    // Search all service schemas for commands and events for this aggregate
    let mut command_rows = Vec::new();
    let mut branch_events = Vec::new();

    for stores in state.service_stores.values() {
        // Try loading commands from this service's schema
        let rows: Vec<CommandRow> = sqlx::query_as(
            "SELECT command_id, aggregate_id, command_type, command_version, \
             correlation_id, causation_id, created_at \
             FROM commands WHERE aggregate_id = $1 ORDER BY created_at ASC",
        )
        .bind(query.aggregate_id)
        .fetch_all(&stores.pool)
        .await
        .unwrap_or_default();

        if !rows.is_empty() {
            command_rows = rows;
        }

        // Try loading events from this service's event store
        if let Ok(events) = stores.event_store.load(&agg_id).await {
            if !events.is_empty() {
                branch_events = events
                    .into_iter()
                    .filter(|e| e.version.as_u64() <= query.branch_version)
                    .collect();
            }
        }

        // If we found data, stop searching
        if !command_rows.is_empty() || !branch_events.is_empty() {
            break;
        }
    }

    if branch_events.is_empty() && command_rows.is_empty() {
        return Err(GatewayError::NotFound(format!(
            "aggregate {} not found",
            query.aggregate_id
        )));
    }

    // Build the diff response
    let mut unchanged = Vec::new();
    let mut removed = Vec::new();

    for row in &command_rows {
        let entry = CommandEnvelopeResponse {
            command_id: row.command_id,
            aggregate_id: row.aggregate_id,
            command_type: row.command_type.clone(),
            command_version: row.command_version as u32,
            correlation_id: row.correlation_id,
            causation_id: row.causation_id,
            timestamp: row.created_at.to_rfc3339(),
        };

        let is_at_branch = branch_events.iter().any(|e| {
            e.version.as_u64() == query.branch_version && e.causation_id == row.command_id
        });

        if is_at_branch {
            removed.push(entry);
        } else {
            unchanged.push(entry);
        }
    }

    let added = vec![CommandEnvelopeResponse {
        command_id: Uuid::new_v4(),
        aggregate_id: query.aggregate_id,
        command_type: query.command_type,
        command_version: 1,
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    }];

    Ok(Json(CommandDiffResponse {
        added,
        removed,
        unchanged,
    }))
}
