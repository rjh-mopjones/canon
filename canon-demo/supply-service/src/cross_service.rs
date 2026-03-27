//! Cross-service event consumer for the supply service.
//!
//! Subscribes to `canon.station.events` and processes `StationStockLow` events
//! by submitting `RequestResupply` commands to the supply inbox.
//! This drives the cross-service flow:
//!
//! Station:StationStockLow -> Supply:RequestResupply -> Supply:ResupplyRequested

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, DispatcherNotifySender, EventEnvelope};
use supply_service::aggregate::RequestResupply;
use supply_service::inbound::InboundStationStockLow;

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
    shutdown: tokio::sync::watch::Receiver<bool>,
    topic_prefix: &str,
    dispatcher_notify: DispatcherNotifySender,
) {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();
    let topic = format!("{topic_prefix}.station.events");

    let client = match ClientBuilder::new(broker_list).build().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create rskafka client for station events");
            return;
        }
    };

    let partition_client = match client
        .partition_client(&topic, 0, UnknownTopicHandling::Retry)
        .await
    {
        Ok(pc) => Arc::new(pc),
        Err(e) => {
            error!(error = %e, topic = %topic, "failed to create partition client");
            return;
        }
    };

    info!(topic = %topic, "subscribed to station events (rskafka)");

    let consumer_id = format!("supply:cross:{topic}");
    let persisted = canon_demo_shared::offsets::load_offset(&pool, &consumer_id).await;
    info!(consumer = %consumer_id, offset = ?persisted, "loaded persisted offset");
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    loop {
        if *shutdown.borrow() {
            info!("cross-service consumer shutting down");
            break;
        }

        let records = match partition_client
            .fetch_records(next_offset, 1..1_048_576, 1_000)
            .await
        {
            Ok((records, _watermark)) => records,
            Err(e) => {
                warn!(error = %e, "station events fetch error, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        if records.is_empty() {
            continue;
        }

        for record_and_offset in &records {
            next_offset = record_and_offset.offset + 1;

            let payload = match record_and_offset.record.value.as_ref() {
                Some(p) => p,
                None => continue,
            };

            let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize event envelope");
                    continue;
                }
            };

            if envelope.event_type != "StationStockLow" {
                continue;
            }

            let stock_low: InboundStationStockLow = match serde_json::from_slice(&envelope.payload)
            {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize StationStockLow payload");
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

            // Derive a deterministic inventory aggregate_id from the source event,
            // so replayed events produce the same inventory aggregate.
            let inventory_aggregate_id = canon_demo_shared::deterministic_command_id(
                envelope.event_id,
                "InventoryAggregate",
            );

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
                envelope.event_id,
                &request_resupply,
            )
            .await
            {
                error!(error = %e, "failed to submit RequestResupply command");
                continue;
            }
            let _ = dispatcher_notify.try_send(());
        }

        // Persist offset after processing the batch
        if !records.is_empty() {
            canon_demo_shared::offsets::save_offset(&pool, &consumer_id, &topic, next_offset - 1)
                .await;
        }
    }
}

async fn submit_command<T: serde::Serialize>(
    pool: &PgPool,
    handler_id: &str,
    command_type: &str,
    aggregate_id: Uuid,
    correlation_id: Uuid,
    source_event_id: Uuid,
    command: &T,
) -> Result<(), SubmitCommandError> {
    // Deterministic command_id derived from (source_event_id, command_type).
    // If the same Kafka event is consumed twice (e.g., offset loss on restart),
    // the second insert will hit ON CONFLICT DO NOTHING and be safely ignored.
    let command_id = canon_demo_shared::deterministic_command_id(source_event_id, command_type);

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
        source_event_id = %source_event_id,
        "submitted cross-service command to inbox"
    );

    Ok(())
}
