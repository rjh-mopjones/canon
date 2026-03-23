use std::sync::Arc;

use async_trait::async_trait;
use samsa::prelude::{BrokerAddress, ProduceMessage, Producer, ProducerBuilder, TcpConnection};
use tracing::{debug, error, info, warn};

use canon_core::traits::Publisher;
use canon_core::EventEnvelope;
use canon_publisher::PublisherError;

#[derive(Debug, thiserror::Error)]
pub enum KafkaPublisherError {
    #[error("kafka producer creation failed: {0}")]
    ProducerCreation(String),

    #[error("kafka produce failed: {0}")]
    Produce(String),

    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl From<KafkaPublisherError> for PublisherError {
    fn from(e: KafkaPublisherError) -> Self {
        PublisherError::Publish(Box::new(e))
    }
}

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

/// Kafka-backed event publisher for cross-service event distribution.
///
/// Publishes confirmed events to `canon.{service_name}.events` topics.
/// Uses `aggregate_id` as the partition key to preserve per-aggregate ordering.
pub struct KafkaPublisher {
    producer: Arc<Producer>,
    service_name: String,
}

impl KafkaPublisher {
    /// Create a new `KafkaPublisher`.
    pub async fn new(brokers: &str, service_name: &str) -> Result<Self, KafkaPublisherError> {
        let addrs = parse_brokers(brokers);
        let topic = format!("canon.{service_name}.events");

        let producer = ProducerBuilder::<TcpConnection>::new(addrs, vec![topic])
            .await
            .map_err(|e| KafkaPublisherError::ProducerCreation(e.to_string()))?
            .required_acks(-1) // acks=all
            .clone()
            .build()
            .await;

        info!(
            service = service_name,
            brokers = brokers,
            "kafka publisher created"
        );

        Ok(Self {
            producer: Arc::new(producer),
            service_name: service_name.to_owned(),
        })
    }

    /// Create from the `KAFKA_BROKERS` environment variable.
    pub async fn from_env(service_name: &str) -> Result<Self, KafkaPublisherError> {
        let brokers = match std::env::var("KAFKA_BROKERS") {
            Ok(b) => b,
            Err(_) => {
                warn!("KAFKA_BROKERS not set, falling back to localhost:9092");
                "localhost:9092".into()
            }
        };
        Self::new(&brokers, service_name).await
    }

    /// Returns the external topic name for this service.
    pub fn topic(&self) -> String {
        format!("canon.{}.events", self.service_name)
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    type Error = PublisherError;

    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), Self::Error> {
        let payload = serde_json::to_vec(&envelope).map_err(KafkaPublisherError::Serialization)?;
        let key = envelope.aggregate_id.as_uuid().to_string();

        let msg = ProduceMessage {
            topic: topic.to_owned(),
            partition_id: 0,
            key: Some(bytes::Bytes::from(key)),
            value: Some(bytes::Bytes::from(payload)),
            headers: vec![],
        };

        self.producer.produce(msg).await;

        debug!(
            event_id = %envelope.event_id,
            aggregate_id = %envelope.aggregate_id.as_uuid(),
            topic = topic,
            "event published to kafka"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_format() {
        // Just test the topic name construction
        assert_eq!(format!("canon.{}.events", "fleet"), "canon.fleet.events");
    }

    #[test]
    fn parse_brokers_works() {
        let addrs = parse_brokers("localhost:9092");
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].host, "localhost");
        assert_eq!(addrs[0].port, 9092);
    }
}
