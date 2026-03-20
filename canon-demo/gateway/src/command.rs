use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope};

use crate::error::GatewayError;

/// Build a [`CommandEnvelope`] from a REST request body.
///
/// If `aggregate_id` is `None`, a new UUID is generated (used for creation commands
/// like RegisterShip where the aggregate does not yet exist).
pub fn build_envelope(
    command_type: &str,
    aggregate_id: Option<Uuid>,
    correlation_id: Uuid,
    payload: &impl serde::Serialize,
) -> Result<CommandEnvelope, GatewayError> {
    let aggregate_id = aggregate_id.unwrap_or_else(Uuid::new_v4);
    let payload_bytes = serde_json::to_vec(payload).map_err(GatewayError::Serialization)?;

    Ok(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: AggregateId::from_uuid(aggregate_id),
        command_type: command_type.to_owned(),
        correlation_id,
        causation_id: correlation_id,
        timestamp: Utc::now(),
        payload: Bytes::from(payload_bytes),
        command_version: 1,
    })
}

/// Persist a command envelope to the commands table and inbox.
///
/// Writes the command to the `commands` table for audit and to `inbox_messages`
/// for dispatch by the target service. Both writes are in the same transaction.
pub async fn submit_command(
    pool: &sqlx::PgPool,
    handler_id: &str,
    envelope: &CommandEnvelope,
) -> Result<(), GatewayError> {
    let mut tx = pool.begin().await?;

    // Write to commands table
    sqlx::query(
        "INSERT INTO commands (command_id, aggregate_id, command_type, command_version, \
         payload, correlation_id, causation_id, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (command_id) DO NOTHING",
    )
    .bind(envelope.command_id)
    .bind(envelope.aggregate_id.as_uuid())
    .bind(&envelope.command_type)
    .bind(envelope.command_version as i32)
    .bind(envelope.payload.as_ref())
    .bind(envelope.correlation_id)
    .bind(envelope.causation_id)
    .bind(envelope.timestamp)
    .execute(&mut *tx)
    .await?;

    // Write to inbox_messages for the target service's dispatcher
    let message_payload = serde_json::to_vec(&envelope).map_err(GatewayError::Serialization)?;

    sqlx::query(
        "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
         VALUES ($1, $2, $3, 'command', $4) \
         ON CONFLICT DO NOTHING",
    )
    .bind(handler_id)
    .bind(envelope.command_id)
    .bind(envelope.aggregate_id.as_uuid())
    .bind(&message_payload)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(())
}
