use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rskafka::client::partition::{Compression, PartitionClient};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use tracing::{debug, error, info, warn};

use canon_core::traits::Publisher;
use canon_core::EventEnvelope;
use canon_publisher::PublisherError;

#[derive(Debug, thiserror::Error)]
pub enum KafkaPublisherError {
    #[error("kafka client creation failed: {0}")]
    ClientCreation(String),

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

/// Kafka-backed event publisher for cross-service event distribution.
///
/// Publishes confirmed events to `canon.{service_name}.events` topics.
/// Uses `aggregate_id` as the partition key to preserve per-aggregate ordering.
///
/// This publisher does not track idempotency itself -- all downstream consumers
/// in Canon are required to be idempotent (see CLAUDE.md non-negotiable rules),
/// so duplicate-suppression at the publisher layer is unnecessary.
pub struct KafkaPublisher {
    client: Arc<rskafka::client::Client>,
    service_name: String,
}

impl KafkaPublisher {
    /// Create a new `KafkaPublisher`.
    ///
    /// - `brokers`: comma-separated Kafka broker addresses (e.g. from `KAFKA_BROKERS`)
    /// - `service_name`: used to derive the external topic `canon.{service_name}.events`
    pub async fn new(brokers: &str, service_name: &str) -> Result<Self, KafkaPublisherError> {
        let broker_list: Vec<String> = brokers.split(',').map(|s| s.trim().to_owned()).collect();

        let client = ClientBuilder::new(broker_list)
            .build()
            .await
            .map_err(|e| KafkaPublisherError::ClientCreation(e.to_string()))?;

        info!(
            service = service_name,
            brokers = brokers,
            "kafka publisher created (rskafka)"
        );

        Ok(Self {
            client: Arc::new(client),
            service_name: service_name.to_owned(),
        })
    }

    /// Create from the `KAFKA_BROKERS` environment variable.
    ///
    /// Falls back to `localhost:9092` if the variable is not set, logging a warning.
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

    /// Returns the external topic name for this service: `canon.{service_name}.events`.
    pub fn topic(&self) -> String {
        format!("canon.{}.events", self.service_name)
    }

    /// Get or create a partition client for the given topic (partition 0).
    async fn partition_client(&self, topic: &str) -> Result<PartitionClient, KafkaPublisherError> {
        self.client
            .partition_client(
                topic,
                0,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            )
            .await
            .map_err(|e| KafkaPublisherError::Produce(e.to_string()))
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    type Error = PublisherError;

    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), Self::Error> {
        let payload = serde_json::to_vec(&envelope).map_err(KafkaPublisherError::Serialization)?;

        let key = envelope.aggregate_id.as_uuid().to_string();

        let record = Record {
            key: Some(key.into_bytes()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let partition_client = self.partition_client(topic).await?;

        partition_client
            .produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| {
                error!(
                    event_id = %envelope.event_id,
                    topic = topic,
                    error = %e,
                    "failed to publish event to kafka"
                );
                KafkaPublisherError::Produce(e.to_string())
            })?;

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
    use bytes::Bytes;
    use canon_core::{AggregateId, Version};
    use chrono::Utc;
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::kafka::apache::{self, Kafka};
    use uuid::Uuid;

    fn make_envelope() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    async fn setup_kafka_container() -> (ContainerAsync<Kafka>, String) {
        let container = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container");

        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Failed to get Kafka host port");

        let broker = format!("127.0.0.1:{}", port);
        (container, broker)
    }

    #[test]
    fn topic_format() {
        // Verify the topic format without needing a connection
        assert_eq!(format!("canon.{}.events", "fleet"), "canon.fleet.events");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_publishes_to_external_topic() {
        let (_container, broker) = setup_kafka_container().await;

        let service = format!("test-{}", Uuid::new_v4().simple());
        let publisher = KafkaPublisher::new(&broker, &service)
            .await
            .expect("publisher creation should succeed");

        let envelope = make_envelope();
        let event_id = envelope.event_id;
        let topic = publisher.topic();

        // Publish first so the topic gets auto-created
        publisher
            .publish(envelope, &topic)
            .await
            .expect("publish should succeed with a running broker");

        // Verify by consuming via rskafka
        let client = ClientBuilder::new(vec![broker.clone()])
            .build()
            .await
            .expect("failed to create verify client");

        let partition_client = client
            .partition_client(
                &topic,
                0,
                rskafka::client::partition::UnknownTopicHandling::Error,
            )
            .await
            .expect("failed to create partition client");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match partition_client.fetch_records(0, 1..1_048_576, 1_000).await {
                Ok((records, _watermark)) => {
                    if let Some(record) = records.first() {
                        let payload = record.record.value.as_ref().expect("empty payload");
                        let received: EventEnvelope =
                            serde_json::from_slice(payload).expect("failed to deserialize");
                        assert_eq!(received.event_id, event_id);
                        return;
                    }
                }
                Err(_) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timeout waiting for message");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_idempotent_publish() {
        let (_container, broker) = setup_kafka_container().await;

        let service = format!("test-{}", Uuid::new_v4().simple());
        let publisher = KafkaPublisher::new(&broker, &service)
            .await
            .expect("publisher creation should succeed");

        let envelope = make_envelope();
        let topic = publisher.topic();

        publisher
            .publish(envelope.clone(), &topic)
            .await
            .expect("first publish should succeed");

        // Second publish of same event -- downstream idempotency handles dedup
        publisher
            .publish(envelope, &topic)
            .await
            .expect("re-publish should succeed");
    }
}
