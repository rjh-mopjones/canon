use futures::StreamExt;
use samsa::prelude::{BrokerAddress, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
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

/// Topic-to-service mapping for WsEnvelope tagging.
const TOPIC_SERVICE_MAP: &[(&str, &str)] = &[
    ("canon.fleet.events", "fleet"),
    ("canon.cargo.events", "cargo"),
    ("canon.navigation.events", "navigation"),
    ("canon.supply.events", "supply"),
    ("canon.station.events", "station"),
];

/// Parse a comma-separated broker string into samsa `BrokerAddress` list.
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

/// Spawn one Kafka consumer per topic.
pub fn spawn_kafka_consumers(brokers: &str, event_tx: broadcast::Sender<String>) {
    for (topic, service) in TOPIC_SERVICE_MAP {
        let brokers = brokers.to_owned();
        let topic = (*topic).to_owned();
        let service = (*service).to_owned();
        let tx = event_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = consume_topic(&brokers, &topic, &service, &tx).await {
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
) -> Result<(), KafkaConsumerError> {
    let addrs = parse_brokers(brokers);

    let assignment = TopicPartitionsBuilder::new()
        .assign(topic.to_owned(), vec![0])
        .build();

    let mut consumer =
        ConsumerGroupBuilder::<TcpConnection>::new(addrs, "gateway-ws".to_owned(), assignment)
            .await
            .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?
            .build()
            .await
            .map_err(|e| KafkaConsumerError::Kafka(e.to_string()))?;

    info!(topic = %topic, "gateway kafka consumer started");

    let stream = consumer.into_stream();
    tokio::pin!(stream);

    while let Some(Ok(batch)) = stream.next().await {
        for msg in batch {
            if msg.value.is_empty() {
                continue;
            }

            let envelope: EventEnvelope = match serde_json::from_slice(&msg.value) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, topic = %topic, "failed to deserialise event");
                    continue;
                }
            };

            // Include event payload for event types that carry data needed by the
            // frontend (e.g. StockDrained carries remaining_kg for stock display).
            let event_payload = match envelope.event_type.as_str() {
                "StockDrained" => serde_json::from_slice(&envelope.payload).ok(),
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
                    let _ = tx.send(json);
                }
                Err(e) => {
                    warn!(error = %e, "failed to serialise WsEnvelope");
                }
            }
        }
    }

    Ok(())
}

/// Spawn a background task that broadcasts `WsEnvelope::InfraStatus` every 10 seconds.
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
