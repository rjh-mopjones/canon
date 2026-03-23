//! Cross-service event consumer for the supply service.
//!
//! Subscribes to `canon.station.events` and processes `StationStockLow` events
//! by submitting `RequestResupply` commands to the supply inbox.

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use samsa::prelude::{BrokerAddress, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
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

/// Consume `StationStockLow` events from `canon.station.events` and submit
/// `RequestResupply` commands to the supply inbox.
pub async fn consume_station_events(
    brokers: &str,
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let addrs = parse_brokers(brokers);
    let topic = "canon.station.events";

    let assignment = TopicPartitionsBuilder::new()
        .assign(topic.to_owned(), vec![0])
        .build();

    let mut consumer = match ConsumerGroupBuilder::<TcpConnection>::new(
        addrs,
        "canon.supply.station-consumer".to_owned(),
        assignment,
    )
    .await
    {
        Ok(builder) => match builder.build().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "failed to build station events consumer group");
                return;
            }
        },
        Err(e) => {
            error!(error = %e, "failed to create station events consumer");
            return;
        }
    };

    info!("subscribed to canon.station.events");

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

                    if envelope.event_type != "StationStockLow" {
                        continue;
                    }

                    let stock_low: StationStockLow = match serde_json::from_slice(&envelope.payload) {
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
                    let inventory_aggregate_id = Uuid::new_v4();

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
