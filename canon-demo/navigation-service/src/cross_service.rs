//! Cross-service event consumer for the navigation service.
//!
//! Subscribes to `canon.fleet.events` and processes `ShipDeparted` events by
//! submitting `PlanRoute` + `RecordArrival` commands to the navigation inbox.
//! This drives the full cross-service flow:
//!
//! Fleet:ShipDeparted → Nav:PlanRoute → Nav:RoutePlanned
//!                    → Nav:RecordArrival → Nav:ShipArrivedAtStation

use bytes::Bytes;
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use canon_demo_shared::commands::{PlanRoute, RecordArrival};
use canon_demo_shared::events::ShipDeparted;

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
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "canon.navigation.fleet-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create fleet events consumer");
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&["canon.fleet.events"]) {
        error!(error = %e, "failed to subscribe to canon.fleet.events");
        return;
    }

    info!("subscribed to canon.fleet.events");

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
                        warn!(error = %e, "fleet events consumer error");
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

                // Only handle ShipDeparted events
                if envelope.event_type != "ShipDeparted" {
                    let _ = consumer.commit_message(&msg, CommitMode::Async);
                    continue;
                }

                // Deserialize the ShipDeparted payload
                let departed: ShipDeparted = match serde_json::from_slice(&envelope.payload) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(error = %e, "failed to deserialize ShipDeparted payload");
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                        continue;
                    }
                };

                info!(
                    ship_id = %departed.ship_id,
                    destination = %departed.destination,
                    "received ShipDeparted from fleet, submitting PlanRoute + RecordArrival"
                );

                let correlation_id = envelope.correlation_id;
                // Use a NEW UUID for the route aggregate — NOT the station's UUID.
                // The station and route are different aggregates and must have
                // different aggregate IDs to avoid event store collisions.
                let route_aggregate_id = Uuid::new_v4();

                // Submit PlanRoute command to inbox
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
                    &plan_route,
                ).await {
                    error!(error = %e, "failed to submit PlanRoute command");
                    continue;
                }

                // Submit RecordArrival command to inbox (same route aggregate)
                let record_arrival = RecordArrival {
                    route_id: route_aggregate_id,
                    station_id: departed.destination,
                };

                if let Err(e) = submit_command(
                    &pool,
                    "Route",
                    "RecordArrival",
                    route_aggregate_id,
                    correlation_id,
                    &record_arrival,
                ).await {
                    error!(error = %e, "failed to submit RecordArrival command");
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
