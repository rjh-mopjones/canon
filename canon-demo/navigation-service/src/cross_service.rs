//! Cross-service event consumers for the navigation service.
//!
//! Two consumers drive the cross-service flow:
//!
//! 1. `consume_fleet_events` -- subscribes to `canon.fleet.events`, processes
//!    `ShipDeparted` events by submitting a `PlanRoute` command.
//!
//! 2. `consume_navigation_events` -- subscribes to `canon.navigation.events`
//!    (the navigation service's own published topic), listens for `RoutePlanned`
//!    events and submits `RecordArrival` once the route aggregate exists.
//!
//! Flow: Fleet:ShipDeparted -> Nav:PlanRoute -> Nav:RoutePlanned -> Nav:RecordArrival -> Nav:ShipArrivedAtStation

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, DispatcherNotifySender, EventEnvelope};
use navigation_service::commands::{PlanRoute, RecordArrival};
use navigation_service::events::RoutePlanned;
use navigation_service::inbound::InboundShipDeparted;

#[derive(Debug, thiserror::Error)]
enum SubmitCommandError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Consume `ShipDeparted` events from `canon.fleet.events` and submit
/// navigation commands to the inbox.
pub async fn consume_fleet_events(
    brokers: &str,
    pool: PgPool,
    shutdown: tokio::sync::watch::Receiver<bool>,
    topic_prefix: &str,
    dispatcher_notify: DispatcherNotifySender,
) {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();
    let topic = format!("{topic_prefix}.fleet.events");

    let client = match ClientBuilder::new(broker_list).build().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create rskafka client for fleet events");
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

    info!(topic = %topic, "subscribed to fleet events (rskafka)");

    let consumer_id = format!("navigation:cross:{topic}");
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
                warn!(error = %e, "fleet events fetch error, retrying");
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

            // Only handle ShipDeparted events
            if envelope.event_type != "ShipDeparted" {
                continue;
            }

            let departed: InboundShipDeparted = match serde_json::from_slice(&envelope.payload) {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize ShipDeparted payload");
                    continue;
                }
            };

            info!(
                ship_id = %departed.ship_id,
                destination = %departed.destination,
                "received ShipDeparted from fleet, submitting PlanRoute"
            );

            let correlation_id = envelope.correlation_id;
            // Deterministic aggregate ID so Kafka replays produce the same route.
            let route_aggregate_id =
                canon_demo_shared::deterministic_command_id(envelope.event_id, "RouteAggregate");

            let plan_route = PlanRoute {
                route_id: route_aggregate_id,
                ship_id: departed.ship_id,
                waypoints: vec![departed.destination],
            };

            if let Err(e) = submit_command(
                &pool,
                "Route",
                "PlanRoute",
                route_aggregate_id,
                correlation_id,
                envelope.event_id,
                &plan_route,
            )
            .await
            {
                error!(error = %e, "failed to submit PlanRoute command");
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

/// Consume `RoutePlanned` events from `canon.navigation.events` (our own
/// published topic) and submit `RecordArrival` once the route aggregate exists.
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
            error!(error = %e, "failed to create rskafka client for navigation self-consumer");
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

    info!(topic = %topic, "subscribed to navigation events (self-consumer for RecordArrival, rskafka)");

    let consumer_id = format!("navigation:cross:{topic}");
    let persisted = canon_demo_shared::offsets::load_offset(&pool, &consumer_id).await;
    info!(consumer = %consumer_id, offset = ?persisted, "loaded persisted offset");
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    loop {
        if *shutdown.borrow() {
            info!("navigation self-consumer shutting down");
            break;
        }

        let records = match partition_client
            .fetch_records(next_offset, 1..1_048_576, 1_000)
            .await
        {
            Ok((records, _watermark)) => records,
            Err(e) => {
                warn!(error = %e, "navigation self-consumer fetch error, retrying");
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
                    warn!(error = %e, "failed to deserialize event envelope (self-consumer)");
                    continue;
                }
            };

            // Only handle RoutePlanned events
            if envelope.event_type != "RoutePlanned" {
                continue;
            }

            let route_planned: RoutePlanned = match serde_json::from_slice(&envelope.payload) {
                Ok(rp) => rp,
                Err(e) => {
                    warn!(error = %e, "failed to deserialize RoutePlanned payload");
                    continue;
                }
            };

            let station_id = match route_planned.waypoints.last() {
                Some(id) => *id,
                None => {
                    warn!(route_id = %route_planned.route_id, "RoutePlanned has no waypoints, skipping RecordArrival");
                    continue;
                }
            };

            let route_aggregate_id = *envelope.aggregate_id.as_uuid();
            let correlation_id = envelope.correlation_id;

            info!(
                route_id = %route_planned.route_id,
                station_id = %station_id,
                "received RoutePlanned, submitting RecordArrival"
            );

            let record_arrival = RecordArrival {
                route_id: route_aggregate_id,
                station_id,
            };

            if let Err(e) = submit_command(
                &pool,
                "Route",
                "RecordArrival",
                route_aggregate_id,
                correlation_id,
                envelope.event_id,
                &record_arrival,
            )
            .await
            {
                error!(error = %e, "failed to submit RecordArrival command");
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
