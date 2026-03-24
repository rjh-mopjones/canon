//! Cross-service event consumer for the fleet service.
//!
//! Subscribes to `canon.supply.events` and processes `ResupplyDispatched` events
//! by submitting `ScheduleResupply` commands to the fleet inbox.
//! This drives the cross-service flow:
//!
//! Supply:ResupplyDispatched → Fleet:ScheduleResupply → Fleet:ResupplyScheduled

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use canon_demo_shared::commands::{DockShip, ScheduleResupply};
use canon_demo_shared::events::{ResupplyDispatched, ShipArrivedAtStation};

#[derive(Debug, thiserror::Error)]
enum SubmitCommandError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Consume `ResupplyDispatched` events from `canon.supply.events` and submit
/// `ScheduleResupply` commands to the fleet inbox.
pub async fn consume_supply_events(
    brokers: &str,
    pool: PgPool,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();

    let client = match ClientBuilder::new(broker_list).build().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create rskafka client for supply events");
            return;
        }
    };

    let partition_client = match client
        .partition_client("canon.supply.events", 0, UnknownTopicHandling::Retry)
        .await
    {
        Ok(pc) => Arc::new(pc),
        Err(e) => {
            error!(error = %e, "failed to create partition client for canon.supply.events");
            return;
        }
    };

    info!("subscribed to canon.supply.events (rskafka)");

    let persisted =
        canon_demo_shared::offsets::load_offset(&pool, "fleet:cross:canon.supply.events").await;
    info!(consumer = "fleet:cross:canon.supply.events", offset = ?persisted, "loaded persisted offset");
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
                warn!(error = %e, "supply events fetch error, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        if records.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }

        for record_and_offset in &records {
            next_offset = record_and_offset.offset + 1;

            let payload = match record_and_offset.record.value.as_ref() {
                Some(p) => p,
                None => continue,
            };

            // Deserialize the EventEnvelope
            let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize event envelope");
                    continue;
                }
            };

            // Only handle ResupplyDispatched events
            if envelope.event_type != "ResupplyDispatched" {
                continue;
            }

            // Deserialize the ResupplyDispatched payload
            let dispatched: ResupplyDispatched = match serde_json::from_slice(&envelope.payload) {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize ResupplyDispatched payload");
                    continue;
                }
            };

            info!(
                ship_id = %dispatched.ship_id,
                fuel_kg = dispatched.fuel_kg,
                "received ResupplyDispatched from supply, submitting ScheduleResupply"
            );

            let correlation_id = envelope.correlation_id;

            // Submit ScheduleResupply command -- aggregate_id is the ship being resupplied
            let schedule_resupply = ScheduleResupply {
                ship_id: dispatched.ship_id,
                fuel_kg: dispatched.fuel_kg,
            };

            if let Err(e) = submit_command(
                &pool,
                "Ship",
                "ScheduleResupply",
                dispatched.ship_id,
                correlation_id,
                &schedule_resupply,
            )
            .await
            {
                error!(error = %e, "failed to submit ScheduleResupply command");
                continue;
            }
        }

        // Persist offset after processing the batch
        if !records.is_empty() {
            canon_demo_shared::offsets::save_offset(
                &pool,
                "fleet:cross:canon.supply.events",
                "canon.supply.events",
                next_offset - 1,
            )
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

/// Consume `ShipArrivedAtStation` events from `canon.navigation.events` and submit
/// `DockShip` commands to the fleet inbox, transitioning the ship back to Docked.
pub async fn consume_navigation_events(
    brokers: &str,
    pool: PgPool,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();

    let client = match ClientBuilder::new(broker_list).build().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create rskafka client for navigation events");
            return;
        }
    };

    let partition_client = match client
        .partition_client("canon.navigation.events", 0, UnknownTopicHandling::Retry)
        .await
    {
        Ok(pc) => Arc::new(pc),
        Err(e) => {
            error!(error = %e, "failed to create partition client for canon.navigation.events");
            return;
        }
    };

    info!("subscribed to canon.navigation.events (rskafka) for fleet-service");

    let persisted =
        canon_demo_shared::offsets::load_offset(&pool, "fleet:cross:canon.navigation.events").await;
    info!(consumer = "fleet:cross:canon.navigation.events", offset = ?persisted, "loaded persisted offset");
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    loop {
        if *shutdown.borrow() {
            info!("navigation events consumer shutting down");
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
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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

            let arrived: ShipArrivedAtStation = match serde_json::from_slice(&envelope.payload) {
                Ok(a) => a,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize ShipArrivedAtStation payload");
                    continue;
                }
            };

            info!(
                ship_id = %arrived.ship_id,
                station_id = %arrived.station_id,
                "received ShipArrivedAtStation from navigation, submitting DockShip"
            );

            let correlation_id = envelope.correlation_id;

            let dock_ship = DockShip {
                ship_id: arrived.ship_id,
                station_id: arrived.station_id,
            };

            if let Err(e) = submit_command(
                &pool,
                "Ship",
                "DockShip",
                arrived.ship_id,
                correlation_id,
                &dock_ship,
            )
            .await
            {
                error!(error = %e, "failed to submit DockShip command");
                continue;
            }
        }

        // Persist offset after processing the batch
        if !records.is_empty() {
            canon_demo_shared::offsets::save_offset(
                &pool,
                "fleet:cross:canon.navigation.events",
                "canon.navigation.events",
                next_offset - 1,
            )
            .await;
        }
    }
}
