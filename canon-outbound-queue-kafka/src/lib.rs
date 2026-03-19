use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use canon_core::EventEnvelope;
use canon_outbound_queue::{OutboundQueue, OutboundQueueError};

/// Configuration for the Kafka-backed outbound queue.
#[derive(Debug, Clone)]
pub struct KafkaOutboundQueueConfig {
    /// Kafka broker addresses (comma-separated).
    pub brokers: String,
    /// Topic name, e.g. `canon.fleet.outbound`.
    pub topic: String,
    /// Consumer group ID. Each downstream consumer (event store, projection,
    /// publisher) should use a distinct group ID.
    pub group_id: String,
    /// Session timeout for the consumer group (default: 6000ms).
    pub session_timeout_ms: u32,
    /// Whether to enable auto-commit (default: false — manual commit required).
    pub enable_auto_commit: bool,
}

impl KafkaOutboundQueueConfig {
    /// Create a config from the `KAFKA_BROKERS` environment variable.
    pub fn from_env(topic: String, group_id: String) -> Result<Self, OutboundQueueError> {
        let brokers = std::env::var("KAFKA_BROKERS").map_err(|e| {
            OutboundQueueError::Queue(
                format!("KAFKA_BROKERS env var not set: {e}").into(),
            )
        })?;
        Ok(Self {
            brokers,
            topic,
            group_id,
            session_timeout_ms: 6000,
            enable_auto_commit: false,
        })
    }
}

/// Kafka-backed implementation of [`OutboundQueue`].
///
/// The producer publishes `EventEnvelope` payloads to a Kafka topic, partitioned
/// by `aggregate_id`. The consumer reads from that topic using a configurable
/// consumer group, with manual offset commit after confirmed downstream write.
pub struct KafkaOutboundQueue {
    producer: FutureProducer,
    consumer: Mutex<StreamConsumer>,
    topic: String,
}

impl KafkaOutboundQueue {
    /// Build a new Kafka outbound queue from the given configuration.
    pub fn new(config: &KafkaOutboundQueueConfig) -> Result<Self, OutboundQueueError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", config.enable_auto_commit.to_string())
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        consumer
            .subscribe(&[&config.topic])
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(
            topic = %config.topic,
            group_id = %config.group_id,
            "Kafka outbound queue initialised"
        );

        Ok(Self {
            producer,
            consumer: Mutex::new(consumer),
            topic: config.topic.clone(),
        })
    }
}

#[async_trait]
impl OutboundQueue for KafkaOutboundQueue {
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        let key = envelope.aggregate_id.as_uuid().to_string();
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        self.producer
            .send(
                FutureRecord::to(&self.topic)
                    .key(&key)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(
            event_id = %envelope.event_id,
            aggregate_id = %key,
            "Published event to outbound queue"
        );

        Ok(())
    }

    async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        let consumer = self.consumer.lock().await;

        match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let payload = msg.payload().ok_or_else(|| {
                    OutboundQueueError::Queue("received message with empty payload".into())
                })?;
                let envelope: EventEnvelope = serde_json::from_slice(payload)
                    .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;
                Ok(Some(envelope))
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Kafka consumer error");
                Err(OutboundQueueError::Queue(Box::new(e)))
            }
            // Timeout — no message available
            Err(_) => Ok(None),
        }
    }

    async fn commit(&self) -> Result<(), OutboundQueueError> {
        let consumer = self.consumer.lock().await;
        consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;
        debug!("Committed consumer offsets");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
