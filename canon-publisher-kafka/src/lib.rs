use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use canon_core::EventEnvelope;
use canon_publisher::{EventPublisher, PublisherError};

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

/// Kafka-backed event publisher for cross-service event distribution.
///
/// Publishes confirmed events to `canon.{service_name}.events` topics.
/// Uses `aggregate_id` as the partition key to preserve per-aggregate ordering.
///
/// Idempotency is tracked via an in-memory set of published `event_id`s.
/// In production, this could be backed by a persistent store, but the
/// combination of Kafka's at-least-once delivery and downstream idempotent
/// consumers provides end-to-end exactly-once semantics.
pub struct KafkaPublisher {
    producer: FutureProducer,
    service_name: String,
    published_ids: Arc<Mutex<HashSet<uuid::Uuid>>>,
    produce_timeout: Duration,
}

impl KafkaPublisher {
    /// Create a new `KafkaPublisher`.
    ///
    /// - `brokers`: comma-separated Kafka broker addresses (e.g. from `KAFKA_BROKERS`)
    /// - `service_name`: used to derive the external topic `canon.{service_name}.events`
    pub fn new(brokers: &str, service_name: &str) -> Result<Self, KafkaPublisherError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "all")
            .create()
            .map_err(|e| KafkaPublisherError::ProducerCreation(e.to_string()))?;

        info!(
            service = service_name,
            brokers = brokers,
            "kafka publisher created"
        );

        Ok(Self {
            producer,
            service_name: service_name.to_owned(),
            published_ids: Arc::new(Mutex::new(HashSet::new())),
            produce_timeout: Duration::from_secs(5),
        })
    }

    /// Create from the `KAFKA_BROKERS` environment variable.
    pub fn from_env(service_name: &str) -> Result<Self, KafkaPublisherError> {
        let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
        Self::new(&brokers, service_name)
    }

    /// Returns the external topic name for this service: `canon.{service_name}.events`
    pub fn topic(&self) -> String {
        format!("canon.{}.events", self.service_name)
    }
}

#[async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish(
        &self,
        envelope: &EventEnvelope,
        topic: &str,
    ) -> Result<(), PublisherError> {
        // Idempotency check: skip if already published
        {
            let mut ids = self.published_ids.lock().await;
            if ids.contains(&envelope.event_id) {
                debug!(
                    event_id = %envelope.event_id,
                    topic = topic,
                    "skipping already-published event"
                );
                return Ok(());
            }
            ids.insert(envelope.event_id);
        }

        let payload = serde_json::to_vec(envelope)
            .map_err(KafkaPublisherError::Serialization)?;

        let key = envelope.aggregate_id.as_uuid().to_string();

        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&payload);

        self.producer
            .send(record, self.produce_timeout)
            .await
            .map_err(|(e, _)| {
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

    #[test]
    fn topic_format() {
        // Verify topic derivation without needing a real Kafka cluster
        let brokers = "localhost:19092";
        let publisher = KafkaPublisher::new(brokers, "fleet")
            .expect("producer creation should succeed even without a running broker");
        assert_eq!(publisher.topic(), "canon.fleet.events");
    }

    #[tokio::test]
    async fn test_publishes_to_external_topic() {
        let brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());

        let publisher = match KafkaPublisher::new(&brokers, "fleet") {
            Ok(p) => p,
            Err(e) => {
                eprintln!("skipping integration test (no kafka): {e}");
                return;
            }
        };

        let envelope = make_envelope();
        let topic = publisher.topic();

        // Publish should succeed (or be skipped if broker not available)
        let result = publisher.publish(&envelope, &topic).await;
        // In CI without Kafka this may fail; the test validates the happy path
        if let Err(e) = &result {
            eprintln!("publish failed (expected without running broker): {e}");
            return;
        }

        // Verify the event_id was tracked for idempotency
        let ids = publisher.published_ids.lock().await;
        assert!(ids.contains(&envelope.event_id));
    }

    #[tokio::test]
    async fn test_idempotent_publish() {
        let brokers =
            std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".into());

        let publisher = match KafkaPublisher::new(&brokers, "fleet") {
            Ok(p) => p,
            Err(e) => {
                eprintln!("skipping integration test (no kafka): {e}");
                return;
            }
        };

        let envelope = make_envelope();
        let topic = publisher.topic();

        // First publish
        let r1 = publisher.publish(&envelope, &topic).await;
        if r1.is_err() {
            eprintln!("skipping — broker not available");
            return;
        }

        // Second publish of same event — should be a no-op (idempotent)
        let r2 = publisher.publish(&envelope, &topic).await;
        assert!(r2.is_ok(), "idempotent re-publish should succeed");

        // The id set should still contain exactly one entry for this event
        let ids = publisher.published_ids.lock().await;
        assert!(ids.contains(&envelope.event_id));
    }
}
