use std::sync::Arc;

use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use canon_core::EventEnvelope;

use crate::types::WsEnvelope;

/// Errors that can occur in the gateway Kafka consumer.
#[derive(Debug, thiserror::Error)]
pub enum KafkaConsumerError {
    #[error("kafka error: {0}")]
    Kafka(String),
}

/// Service names for building topic-to-service mappings.
const SERVICES: &[&str] = &["fleet", "cargo", "navigation", "supply", "station"];

/// Spawn one Kafka polling task per topic. Each task deserialises incoming
/// events, wraps them in [`WsEnvelope::Event`], and broadcasts via the
/// provided channel.
///
/// Uses rskafka with in-memory offset tracking. On gateway restart,
/// consumption resumes from offset 0 -- downstream WebSocket clients
/// receive events idempotently. Per-session WS filtering handles
/// routing events to the correct browser tab.
///
/// The `topic_prefix` controls topic naming (default "canon", staging uses
/// "canon.staging") so staging and prod can share the same Kafka cluster.
pub fn spawn_kafka_consumers(
    brokers: &str,
    event_tx: broadcast::Sender<String>,
    offset_pool: sqlx::PgPool,
    topic_prefix: &str,
) {
    for service in SERVICES {
        let brokers = brokers.to_owned();
        let topic = format!("{topic_prefix}.{service}.events");
        let service = (*service).to_owned();
        let tx = event_tx.clone();
        let pool = offset_pool.clone();

        tokio::spawn(async move {
            if let Err(e) = consume_topic(&brokers, &topic, &service, &tx, &pool).await {
                error!(topic = %topic, error = %e, "kafka consumer failed to start");
            }
        });
    }
}

async fn consume_topic(
    brokers: &str,
    topic: &str,
    service: &str,
    tx: &broadcast::Sender<String>,
    pool: &sqlx::PgPool,
) -> Result<(), KafkaConsumerError> {
    let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();

    let client = ClientBuilder::new(broker_list)
        .build()
        .await
        .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?;

    let partition_client = Arc::new(
        client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?,
    );

    let consumer_id = format!("gateway:{topic}");
    let persisted = canon_demo_shared::offsets::load_offset(pool, &consumer_id).await;
    let mut next_offset: i64 = persisted.map(|o| o + 1).unwrap_or(0);

    info!(topic = %topic, offset = next_offset, "gateway kafka consumer started (rskafka)");

    loop {
        match partition_client
            .fetch_records(next_offset, 1..1_048_576, 1_000)
            .await
        {
            Ok((records, _watermark)) => {
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
                            warn!(error = %e, topic = %topic, "failed to deserialise event");
                            continue;
                        }
                    };

                    // Include event payload for event types that carry data needed by the
                    // frontend (e.g. StockDrained carries remaining_kg for stock display).
                    let event_payload = match envelope.event_type.as_str() {
                        "StockDrained"
                        | "ShipArrivedAtStation"
                        | "ShipDockedAtStation"
                        | "CargoLoaded"
                        | "ManifestCreated" => serde_json::from_slice(&envelope.payload).ok(),
                        _ => None,
                    };

                    let ws_msg = WsEnvelope::Event {
                        event_id: envelope.event_id,
                        correlation_id: envelope.correlation_id,
                        timestamp: envelope.timestamp.to_rfc3339(),
                        version: envelope.version.as_u64(),
                        service: service.to_owned(),
                        event_type: envelope.event_type.clone(),
                        aggregate_id: envelope.aggregate_id.as_uuid().to_string(),
                        payload: event_payload,
                    };

                    match serde_json::to_string(&ws_msg) {
                        Ok(json) => {
                            // Ignore send errors -- means no subscribers are connected
                            let _ = tx.send(json);
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to serialise WsEnvelope");
                        }
                    }
                }

                // Persist offset after each batch
                canon_demo_shared::offsets::save_offset(pool, &consumer_id, topic, next_offset - 1)
                    .await;
            }
            Err(e) => {
                warn!(error = %e, topic = %topic, "kafka fetch failed, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Spawn a background task that broadcasts `WsEnvelope::InfraStatus` every 10 seconds.
///
/// Checks YugabyteDB (via pool), Cassandra (via event store), and Kafka connectivity.
pub fn spawn_infra_status_broadcaster(
    event_tx: broadcast::Sender<String>,
    yugabyte_pool: sqlx::PgPool,
    cassandra_nodes: String,
    kafka_brokers: String,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
            interval.tick().await;

            let yugabyte_ok = sqlx::query("SELECT 1")
                .execute(&yugabyte_pool)
                .await
                .is_ok();

            // Simple TCP check for Cassandra
            let cassandra_ok = {
                let addr = cassandra_nodes
                    .split(',')
                    .next()
                    .unwrap_or("cassandra:9042")
                    .trim();
                tokio::net::TcpStream::connect(addr).await.is_ok()
            };

            // Simple TCP check for Kafka
            let kafka_ok = {
                let addr = kafka_brokers
                    .split(',')
                    .next()
                    .unwrap_or("kafka:9092")
                    .trim();
                tokio::net::TcpStream::connect(addr).await.is_ok()
            };

            let status = WsEnvelope::InfraStatus {
                kafka_ok,
                yugabyte_ok,
                cassandra_ok,
            };

            if let Ok(json) = serde_json::to_string(&status) {
                let _ = event_tx.send(json);
            }
        }
    });
}
