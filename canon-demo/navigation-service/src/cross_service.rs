//! Cross-service event consumers for the navigation service.
//!
//! Two consumers drive the cross-service flow:
//!
//! 1. `consume_fleet_events` — subscribes to `canon.fleet.events`, processes
//!    `ShipDeparted` events by submitting a `PlanRoute` command.
//!
//! 2. `consume_navigation_events` — subscribes to `canon.navigation.events`
//!    (the navigation service's own published topic), listens for `RoutePlanned`
//!    events and submits `RecordArrival` once the route aggregate exists.
//!
//! Flow: Fleet:ShipDeparted → Nav:PlanRoute → Nav:RoutePlanned → Nav:RecordArrival → Nav:ShipArrivedAtStation

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use samsa::prelude::{BrokerAddress, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use canon_core::{AggregateId, CommandEnvelope, EventEnvelope};
use canon_demo_shared::commands::{PlanRoute, RecordArrival};
use canon_demo_shared::events::{RoutePlanned, ShipDeparted};

#[derive(Debug, thiserror::Error)]
enum SubmitCommandError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

fn parse_brokers(brokers: &str) -> Vec<BrokerAddress> {
    brokers
        .split(',')
        .filter_map(|addr| {
            let addr = addr.trim();
            let (host, port_str) = addr.rsplit_once(':')?;
            let port = port_str.parse::<u16>().ok()?;
            Some(BrokerAddress {
                host: host.to_owned(),
                port,
            })
        })
        .collect()
}

/// Consume `ShipDeparted` events from `canon.fleet.events` and submit
/// navigation commands to the inbox.
pub async fn consume_fleet_events(
    brokers: &str,
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let addrs = parse_brokers(brokers);
    let topic = "canon.fleet.events";

    let assignment = TopicPartitionsBuilder::new()
        .assign(topic.to_owned(), vec![0])
        .build();

    let mut consumer = match ConsumerGroupBuilder::<TcpConnection>::new(
        addrs,
        "canon.navigation.fleet-consumer".to_owned(),
        assignment,
    )
    .await
    {
        Ok(builder) => match builder.build().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to build fleet events consumer group");
                return;
            }
        },
        Err(e) => {
            error!(error = %e, "failed to create fleet events consumer");
            return;
        }
    };

    info!("subscribed to canon.fleet.events");

    let stream = consumer.into_stream();
    tokio::pin!(stream);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("cross-service consumer shutting down");
                break;
            }
            batch_opt = stream.next() => {
                let batch = match batch_opt {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        warn!(error = %e, "consumer group error");
                        continue;
                    }
                    None => break,
                };

                for msg in batch {
                    if msg.value.is_empty() {
                        continue;
                    }

                    let envelope: EventEnvelope = match serde_json::from_slice(&msg.value) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(error = %e, "failed to deserialize event envelope");
                            continue;
                        }
                    };

                    if envelope.event_type != "ShipDeparted" {
                        continue;
                    }

                    let departed: ShipDeparted = match serde_json::from_slice(&envelope.payload) {
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
                    let route_aggregate_id = Uuid::new_v4();

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
                }
            }
        }
    }
}

/// Consume `RoutePlanned` events from `canon.navigation.events` (our own
/// published topic) and submit `RecordArrival` once the route aggregate exists.
pub async fn consume_navigation_events(
    brokers: &str,
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let addrs = parse_brokers(brokers);
    let topic = "canon.navigation.events";

    let assignment = TopicPartitionsBuilder::new()
        .assign(topic.to_owned(), vec![0])
        .build();

    let mut consumer = match ConsumerGroupBuilder::<TcpConnection>::new(
        addrs,
        "canon.navigation.self-consumer".to_owned(),
        assignment,
    )
    .await
    {
        Ok(builder) => match builder.build().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to build navigation self-consumer group");
                return;
            }
        },
        Err(e) => {
            error!(error = %e, "failed to create navigation self-consumer");
            return;
        }
    };

    info!("subscribed to canon.navigation.events (self-consumer for RecordArrival)");

    let stream = consumer.into_stream();
    tokio::pin!(stream);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!("navigation self-consumer shutting down");
                break;
            }
            batch_opt = stream.next() => {
                let batch = match batch_opt {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        warn!(error = %e, "consumer group error");
                        continue;
                    }
                    None => break,
                };

                for msg in batch {
                    if msg.value.is_empty() {
                        continue;
                    }

                    let envelope: EventEnvelope = match serde_json::from_slice(&msg.value) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!(error = %e, "failed to deserialize event envelope (self-consumer)");
                            continue;
                        }
                    };

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
                        &record_arrival,
                    ).await {
                        error!(error = %e, "failed to submit RecordArrival command");
                        continue;
                    }
                }
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
