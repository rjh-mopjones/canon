//! Cross-service event consumer for the station service.
//!
//! Subscribes to `canon.navigation.events` and processes `ShipArrivedAtStation`
//! events by submitting `RecordDocking` commands to the station inbox.
//! This drives the cross-service flow:
//!
//! Navigation:ShipArrivedAtStation -> Station:RecordDocking -> Station:ShipDocked

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, DispatcherNotifySender, EventEnvelope};
use station_service::commands::RecordDocking;
use station_service::events::StockDrained;
use station_service::inbound::InboundShipArrivedAtStation;

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
    shutdown: tokio::sync::watch::Receiver<bool>,
    topic_prefix: &str,
    dispatcher_notify: DispatcherNotifySender,
) {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();
    let topic = format!("{topic_prefix}.navigation.events");

    let client = match ClientBuilder::new(broker_list).build().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create rskafka client for navigation events");
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

    info!(topic = %topic, "subscribed to navigation events (rskafka)");

    let consumer_id = format!("station:cross:{topic}");
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
                warn!(error = %e, "navigation events fetch error, retrying");
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

            if envelope.event_type != "ShipArrivedAtStation" {
                continue;
            }

            let arrived: InboundShipArrivedAtStation =
                match serde_json::from_slice(&envelope.payload) {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize ShipArrivedAtStation payload");
                        continue;
                    }
                };

            info!(
                ship_id = %arrived.ship_id,
                station_id = %arrived.station_id,
                "received ShipArrivedAtStation from navigation, submitting RecordDocking"
            );

            let correlation_id = envelope.correlation_id;

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
                envelope.event_id,
                &record_docking,
            )
            .await
            {
                error!(error = %e, "failed to submit RecordDocking command");
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
        "submitted cross-service command to inbox"
    );

    Ok(())
}

/// Consume `StockDrained` events from `canon.station.events` (own published events)
/// and submit `CheckStockLevel` + `CheckStationOffline` commands for each station.
/// This drives the supply cascade (stock < 20% triggers resupply) and game-over
/// detection (stock == 0 triggers offline).
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
            error!(error = %e, "failed to create rskafka client for station events (self)");
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

    info!(topic = %topic, "subscribed to station events (self-consumer for stock checks)");

    let consumer_id = format!("station:cross:{topic}");
    let persisted = canon_demo_shared::offsets::load_offset(&pool, &consumer_id).await;
    info!(consumer = %consumer_id, offset = ?persisted, "loaded persisted offset");
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    loop {
        if *shutdown.borrow() {
            info!("station self-consumer shutting down");
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
                    warn!(error = %e, "failed to deserialize event envelope from station events");
                    continue;
                }
            };

            if envelope.event_type != "StockDrained" {
                continue;
            }

            let drained: StockDrained = match serde_json::from_slice(&envelope.payload) {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize StockDrained payload");
                    continue;
                }
            };

            let correlation_id = envelope.correlation_id;

            #[derive(serde::Serialize)]
            struct CheckStockPayload {
                station_id: Uuid,
            }

            // Pre-filter: only submit CheckStockLevel when stock is getting
            // low enough that the command handler would actually produce a
            // StationStockLow event. This avoids ~95% of dead letters from
            // expected rejections when stock is above threshold.
            if drained.remaining_kg < 1100.0 {
                let check_stock = CheckStockPayload {
                    station_id: drained.station_id,
                };

                if let Err(e) = submit_command(
                    &pool,
                    "Station",
                    "CheckStockLevel",
                    drained.station_id,
                    correlation_id,
                    envelope.event_id,
                    &check_stock,
                )
                .await
                {
                    tracing::debug!(
                        error = %e,
                        station_id = %drained.station_id,
                        remaining_kg = drained.remaining_kg,
                        "CheckStockLevel command rejected"
                    );
                } else {
                    let _ = dispatcher_notify.try_send(());
                }
            }

            // Pre-filter: only submit CheckStationOffline when stock has
            // actually hit zero. Avoids dead letters from rejections.
            if drained.remaining_kg <= 0.0 {
                let check_offline = CheckStockPayload {
                    station_id: drained.station_id,
                };

                if let Err(e) = submit_command(
                    &pool,
                    "Station",
                    "CheckStationOffline",
                    drained.station_id,
                    correlation_id,
                    envelope.event_id,
                    &check_offline,
                )
                .await
                {
                    tracing::debug!(
                        error = %e,
                        station_id = %drained.station_id,
                        "CheckStationOffline command rejected"
                    );
                } else {
                    let _ = dispatcher_notify.try_send(());
                }
            }
        }

        // Persist offset after processing the batch
        if !records.is_empty() {
            canon_demo_shared::offsets::save_offset(&pool, &consumer_id, &topic, next_offset - 1)
                .await;
        }
    }
}
