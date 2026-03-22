//! Cross-service event consumer for the station service.
//!
//! Subscribes to `canon.navigation.events` and processes `ShipArrivedAtStation`
//! events by submitting `RecordDocking` commands to the station inbox.
//! This drives the cross-service flow:
//!
//! Navigation:ShipArrivedAtStation → Station:RecordDocking → Station:ShipDocked

use bytes::Bytes;
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use canon_demo_shared::commands::RecordDocking;
use canon_demo_shared::events::ShipArrivedAtStation;

#[derive(Debug, thiserror::Error)]
enum SubmitCommandError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Consume `ShipArrivedAtStation` events from `canon.navigation.events` and submit
/// `RecordDocking` commands to the station inbox.
pub async fn consume_navigation_events(
    brokers: &str,
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "canon.station.navigation-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create navigation events consumer");
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&["canon.navigation.events"]) {
        error!(error = %e, "failed to subscribe to canon.navigation.events");
        return;
    }

    info!("subscribed to canon.navigation.events");

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
                        warn!(error = %e, "navigation events consumer error");
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

                // Only handle ShipArrivedAtStation events
                if envelope.event_type != "ShipArrivedAtStation" {
                    let _ = consumer.commit_message(&msg, CommitMode::Async);
                    continue;
                }

                // Deserialize the ShipArrivedAtStation payload
                let arrived: ShipArrivedAtStation = match serde_json::from_slice(&envelope.payload) {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize ShipArrivedAtStation payload");
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                        continue;
                    }
                };

                info!(
                    ship_id = %arrived.ship_id,
                    station_id = %arrived.station_id,
                    "received ShipArrivedAtStation from navigation, submitting RecordDocking"
                );

                let correlation_id = envelope.correlation_id;

                // Submit RecordDocking command — aggregate_id is the station being docked at
                let record_docking = RecordDocking {
                    station_id: arrived.station_id,
                    ship_id: arrived.ship_id,
                };

                if let Err(e) = submit_command(
                    &pool,
                    "Station",
                    "RecordDocking",
                    arrived.station_id,
                    correlation_id,
                    &record_docking,
                ).await {
                    error!(error = %e, "failed to submit RecordDocking command");
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
