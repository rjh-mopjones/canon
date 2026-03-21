use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use canon_core::outbox::{OutboxProcessorError, OutboxPublisher};
use canon_core::EventEnvelope;
use canon_outbound_queue::{OutboundQueue, OutboundQueueError};

/// Configuration for the Kafka-backed outbound queue producer.
#[derive(Debug, Clone)]
pub struct KafkaOutboundProducerConfig {
    /// Kafka broker addresses (comma-separated).
    pub brokers: String,
    /// Topic name, e.g. `canon.fleet.outbound`.
    pub topic: String,
}

/// Configuration for the Kafka-backed outbound queue consumer.
#[derive(Debug, Clone)]
pub struct KafkaOutboundConsumerConfig {
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
    /// Timeout in milliseconds when polling for new messages (default: 100).
    pub receive_timeout_ms: u32,
}

impl Default for KafkaOutboundConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: String::new(),
            topic: String::new(),
            group_id: String::new(),
            session_timeout_ms: 6000,
            enable_auto_commit: false,
            receive_timeout_ms: 100,
        }
    }
}

/// Configuration for the combined Kafka outbound queue (producer + consumer).
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
    /// Timeout in milliseconds when polling for new messages (default: 100).
    pub receive_timeout_ms: u32,
}

impl KafkaOutboundQueueConfig {
    /// Create a config from the `KAFKA_BROKERS` environment variable.
    pub fn from_env(topic: String, group_id: String) -> Result<Self, OutboundQueueError> {
        let brokers = std::env::var("KAFKA_BROKERS").map_err(|e| {
            OutboundQueueError::Queue(format!("KAFKA_BROKERS env var not set: {e}").into())
        })?;
        Ok(Self {
            brokers,
            topic,
            group_id,
            session_timeout_ms: 6000,
            enable_auto_commit: false,
            receive_timeout_ms: 100,
        })
    }
}

/// Tracks the topic, partition, and offset of the last received Kafka message,
/// so we can commit exactly that position.
#[derive(Debug, Clone)]
struct LastReceivedPosition {
    topic: String,
    partition: i32,
    offset: i64,
}

// ---------------------------------------------------------------------------
// KafkaOutboundProducer
// ---------------------------------------------------------------------------

/// Kafka-backed producer for the outbound queue.
///
/// Publishes `EventEnvelope` payloads to a Kafka topic, partitioned by
/// `aggregate_id`.
pub struct KafkaOutboundProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaOutboundProducer {
    /// Build a new Kafka outbound producer.
    pub fn new(config: &KafkaOutboundProducerConfig) -> Result<Self, OutboundQueueError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(topic = %config.topic, "Kafka outbound producer initialised");

        Ok(Self {
            producer,
            topic: config.topic.clone(),
        })
    }

    /// Publish an event envelope to the outbound queue.
    pub async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        self.publish_to_kafka(envelope).await
    }

    /// Internal helper that performs the actual Kafka send. Used by both the
    /// inherent `publish` method and the `OutboxPublisher` trait impl.
    async fn publish_to_kafka(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        let key = envelope.aggregate_id.as_uuid().to_string();
        let payload =
            serde_json::to_vec(&envelope).map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        self.producer
            .send(
                FutureRecord::to(&self.topic).key(&key).payload(&payload),
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
}

#[async_trait]
impl OutboxPublisher for KafkaOutboundProducer {
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboxProcessorError> {
        self.publish_to_kafka(envelope)
            .await
            .map_err(|e| OutboxProcessorError::PublishFailed {
                entry_id: uuid::Uuid::nil(),
                reason: e.to_string(),
            })
    }
}

// ---------------------------------------------------------------------------
// KafkaOutboundConsumer
// ---------------------------------------------------------------------------

/// Kafka-backed consumer for the outbound queue.
///
/// Reads `EventEnvelope` payloads from a Kafka topic using a configurable
/// consumer group, with manual per-message offset commit.
pub struct KafkaOutboundConsumer {
    consumer: Mutex<StreamConsumer>,
    receive_timeout_ms: u32,
    last_received: Mutex<Option<LastReceivedPosition>>,
}

impl KafkaOutboundConsumer {
    /// Build a new Kafka outbound consumer.
    pub fn new(config: &KafkaOutboundConsumerConfig) -> Result<Self, OutboundQueueError> {
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
            receive_timeout_ms = config.receive_timeout_ms,
            "Kafka outbound consumer initialised"
        );

        Ok(Self {
            consumer: Mutex::new(consumer),
            receive_timeout_ms: config.receive_timeout_ms,
            last_received: Mutex::new(None),
        })
    }

    /// Receive the next event envelope from the consumer group.
    /// Returns `None` if no messages are available within the configured timeout.
    pub async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        let consumer = self.consumer.lock().await;
        let timeout = Duration::from_millis(self.receive_timeout_ms as u64);

        match tokio::time::timeout(timeout, consumer.recv()).await {
            Ok(Ok(msg)) => {
                // Store position for per-message commit
                let position = LastReceivedPosition {
                    topic: msg.topic().to_owned(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                };
                {
                    let mut last = self.last_received.lock().await;
                    *last = Some(position);
                }

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

    /// Commit the offset of the last received message.
    ///
    /// Uses per-message commit via `TopicPartitionList` rather than committing
    /// the entire consumer state. The committed offset is `last_offset + 1`
    /// because Kafka interprets the committed offset as the *next* message to
    /// consume.
    pub async fn commit(&self) -> Result<(), OutboundQueueError> {
        let position = {
            let last = self.last_received.lock().await;
            last.clone()
        };

        let position = match position {
            Some(p) => p,
            None => {
                debug!("No message to commit — skipping");
                return Ok(());
            }
        };

        let consumer = self.consumer.lock().await;
        let mut tpl = TopicPartitionList::new();
        // Kafka convention: committed offset = last consumed offset + 1
        tpl.add_partition_offset(
            &position.topic,
            position.partition,
            Offset::Offset(position.offset + 1),
        )
        .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(
            topic = %position.topic,
            partition = position.partition,
            offset = position.offset,
            "Committed consumer offset (per-message)"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KafkaOutboundQueue — convenience wrapper
// ---------------------------------------------------------------------------

/// Combined Kafka-backed implementation of [`OutboundQueue`].
///
/// Wraps both a [`KafkaOutboundProducer`] and a [`KafkaOutboundConsumer`],
/// delegating `publish()` to the producer and `receive()`/`commit()` to the
/// consumer. This is the primary type for callers that need both sides.
pub struct KafkaOutboundQueue {
    producer: KafkaOutboundProducer,
    consumer: KafkaOutboundConsumer,
}

impl KafkaOutboundQueue {
    /// Build a new Kafka outbound queue from the given configuration.
    pub fn new(config: &KafkaOutboundQueueConfig) -> Result<Self, OutboundQueueError> {
        let producer_config = KafkaOutboundProducerConfig {
            brokers: config.brokers.clone(),
            topic: config.topic.clone(),
        };
        let consumer_config = KafkaOutboundConsumerConfig {
            brokers: config.brokers.clone(),
            topic: config.topic.clone(),
            group_id: config.group_id.clone(),
            session_timeout_ms: config.session_timeout_ms,
            enable_auto_commit: config.enable_auto_commit,
            receive_timeout_ms: config.receive_timeout_ms,
        };

        let producer = KafkaOutboundProducer::new(&producer_config)?;
        let consumer = KafkaOutboundConsumer::new(&consumer_config)?;

        Ok(Self { producer, consumer })
    }

    /// Access the underlying producer.
    pub fn producer(&self) -> &KafkaOutboundProducer {
        &self.producer
    }

    /// Access the underlying consumer.
    pub fn consumer(&self) -> &KafkaOutboundConsumer {
        &self.consumer
    }
}

#[async_trait]
impl OutboundQueue for KafkaOutboundQueue {
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        self.producer.publish(envelope).await
    }

    async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        self.consumer.receive().await
    }

    async fn commit(&self) -> Result<(), OutboundQueueError> {
        self.consumer.commit().await
    }
}

#[cfg(test)]
mod tests;
