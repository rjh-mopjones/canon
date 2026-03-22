//! Cross-service event consumer for the supply service.
//!
//! Subscribes to `canon.station.events` and processes `StationStockLow` events
//! by submitting `RequestResupply` commands to the supply inbox.
//! This drives the cross-service flow:
//!
//! Station:StationStockLow → Supply:RequestResupply → Supply:ResupplyRequested

use bytes::Bytes;
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use canon_demo_shared::commands::RequestResupply;
use canon_demo_shared::events::StationStockLow;

#[derive(Debug, thiserror::Error)]
enum SubmitCommandError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Consume `StationStockLow` events from `canon.station.events` and submit
/// `RequestResupply` commands to the supply inbox.
pub async fn consume_station_events(
    brokers: &str,
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "canon.supply.station-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create station events consumer");
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&["canon.station.events"]) {
        error!(error = %e, "failed to subscribe to canon.station.events");
        return;
    }

    info!("subscribed to canon.station.events");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("cross-service consumer shutting down");
                break;
            }
            msg_result = consumer.recv() => {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "station events consumer error");
                        continue;
                    }
                };

                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                        continue;
                    }
                };

                // Deserialize the EventEnvelope
                let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize event envelope");
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                        continue;
                    }
                };

                // Only handle StationStockLow events
                if envelope.event_type != "StationStockLow" {
                    let _ = consumer.commit_message(&msg, CommitMode::Async);
                    continue;
                }

                // Deserialize the StationStockLow payload
                let stock_low: StationStockLow = match serde_json::from_slice(&envelope.payload) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize StationStockLow payload");
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                        continue;
                    }
                };

                info!(
                    station_id = %stock_low.station_id,
                    current_stock_kg = stock_low.current_stock_kg,
                    threshold_kg = stock_low.threshold_kg,
                    "received StationStockLow from station, submitting RequestResupply"
                );

                let correlation_id = envelope.correlation_id;

                // Each resupply request is a new inventory aggregate
                let inventory_aggregate_id = Uuid::new_v4();

                // Submit RequestResupply command — request enough fuel to reach the threshold
                let request_resupply = RequestResupply {
                    station_id: stock_low.station_id,
                    fuel_kg: stock_low.threshold_kg,
                };

                if let Err(e) = submit_command(
                    &pool,
                    "Inventory",
                    "RequestResupply",
                    inventory_aggregate_id,
                    correlation_id,
                    &request_resupply,
                ).await {
                    error!(error = %e, "failed to submit RequestResupply command");
                    continue;
                }

                let _ = consumer.commit_message(&msg, CommitMode::Async);
            }
        }
    }
}

async fn submit_command<T: serde::Serialize>(
    pool: &PgPool,
    handler_id: &str,
    command_type: &str,
    aggregate_id: Uuid,
    correlation_id: Uuid,
    command: &T,
) -> Result<(), SubmitCommandError> {
    let command_id = Uuid::new_v4();

    let command_payload = serde_json::to_vec(command)
        .map_err(|e| SubmitCommandError::Serialization(e.to_string()))?;

    let envelope = CommandEnvelope {
        command_id,
        aggregate_id: AggregateId::from_uuid(aggregate_id),
        command_type: command_type.to_string(),
        correlation_id,
        causation_id: correlation_id,
        timestamp: Utc::now(),
        payload: Bytes::from(command_payload),
        command_version: 1,
    };

    let envelope_json = serde_json::to_vec(&envelope)
        .map_err(|e| SubmitCommandError::Serialization(e.to_string()))?;

    // Write command + inbox entry in a single ACID transaction so that
    // a partial failure cannot leave a command without an inbox entry.
    let mut tx = pool.begin().await?;

    sqlx::query(
        "INSERT INTO commands (command_id, aggregate_id, command_type, command_version, \
         payload, correlation_id, causation_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT DO NOTHING",
    )
    .bind(command_id)
    .bind(aggregate_id)
    .bind(command_type)
    .bind(1_i32)
    .bind(&envelope_json)
    .bind(correlation_id)
    .bind(correlation_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO inbox_messages (handler_id, message_id, aggregate_id, message_type, payload) \
         VALUES ($1, $2, $3, 'command', $4) \
         ON CONFLICT DO NOTHING",
    )
    .bind(handler_id)
    .bind(command_id)
    .bind(aggregate_id)
    .bind(&envelope_json)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    info!(
        command_type = command_type,
        command_id = %command_id,
        "submitted cross-service command to inbox"
    );

    Ok(())
}
