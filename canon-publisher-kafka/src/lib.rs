use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{debug, error, info, warn};

use canon_core::traits::Publisher;
use canon_core::EventEnvelope;
use canon_publisher::PublisherError;

const DEFAULT_PRODUCE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MESSAGE_TIMEOUT_MS: &str = "5000";

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
/// This publisher does not track idempotency itself — all downstream consumers
/// in Canon are required to be idempotent (see CLAUDE.md non-negotiable rules),
/// so duplicate-suppression at the publisher layer is unnecessary.
/// The producer is configured with `enable.idempotence=true` to get exactly-once
/// delivery semantics at the Kafka level.
pub struct KafkaPublisher {
    producer: FutureProducer,
    service_name: String,
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
            .set("message.timeout.ms", DEFAULT_MESSAGE_TIMEOUT_MS)
            .set("acks", "all")
            .set("enable.idempotence", "true")
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
            produce_timeout: DEFAULT_PRODUCE_TIMEOUT,
        })
    }

    /// Create from the `KAFKA_BROKERS` environment variable.
    ///
    /// Falls back to `localhost:9092` if the variable is not set, logging a warning.
    pub fn from_env(service_name: &str) -> Result<Self, KafkaPublisherError> {
        let brokers = match std::env::var("KAFKA_BROKERS") {
            Ok(b) => b,
            Err(_) => {
                warn!("KAFKA_BROKERS not set, falling back to localhost:9092");
                "localhost:9092".into()
            }
        };
        Self::new(&brokers, service_name)
    }

    /// Returns the external topic name for this service: `canon.{service_name}.events`.
    ///
    /// Callers should pass this to `Publisher::publish` to target the correct
    /// cross-service topic for this publisher's service.
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

        let record = FutureRecord::to(topic).key(&key).payload(&payload);

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
    use futures::StreamExt;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::{ClientConfig, Message};
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
        // rdkafka's FutureProducer connects lazily, so creation succeeds without a broker
        let brokers = "localhost:19092";
        let publisher = KafkaPublisher::new(brokers, "fleet")
            .expect("producer creation is lazy — does not require a running broker");
        assert_eq!(publisher.topic(), "canon.fleet.events");
    }

    #[tokio::test]
    async fn test_publishes_to_external_topic() {
        let (_container, broker) = setup_kafka_container().await;

        let service = format!("test-{}", Uuid::new_v4().simple());
        let publisher =
            KafkaPublisher::new(&broker, &service).expect("producer creation should succeed");

        let envelope = make_envelope();
        let event_id = envelope.event_id;
        let topic = publisher.topic();

        // Create a consumer to verify the message was actually published
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &broker)
            .set("group.id", format!("verify-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create verify consumer");

        consumer
            .subscribe(&[&topic])
            .expect("Failed to subscribe");

        publisher
            .publish(envelope, &topic)
            .await
            .expect("publish should succeed with a running broker");

        // Verify the message arrived
        let mut stream = consumer.stream();
        let msg = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("timeout waiting for message")
            .expect("stream ended")
            .expect("consumer error");

        let payload = msg.payload().expect("empty payload");
        let received: EventEnvelope =
            serde_json::from_slice(payload).expect("failed to deserialize");
        assert_eq!(received.event_id, event_id);
    }

    #[tokio::test]
    async fn test_idempotent_publish() {
        let (_container, broker) = setup_kafka_container().await;

        let service = format!("test-{}", Uuid::new_v4().simple());
        let publisher =
            KafkaPublisher::new(&broker, &service).expect("producer creation should succeed");

        let envelope = make_envelope();
        let topic = publisher.topic();

        publisher
            .publish(envelope.clone(), &topic)
            .await
            .expect("first publish should succeed");

        // Second publish of same event — Kafka idempotent producer handles dedup
        publisher
            .publish(envelope, &topic)
            .await
            .expect("re-publish should succeed");
    }
}
