use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use canon_core::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};
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
    /// Consumer group ID. Kept for API compatibility but not used by rskafka
    /// (no consumer groups). Each consumer tracks offset in-memory.
    pub group_id: String,
    /// Session timeout (kept for API compat, unused by rskafka).
    pub session_timeout_ms: u32,
    /// Whether to enable auto-commit (kept for API compat, unused by rskafka).
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
    /// Consumer group ID. Kept for API compatibility.
    pub group_id: String,
    /// Session timeout (kept for API compat).
    pub session_timeout_ms: u32,
    /// Whether to enable auto-commit (kept for API compat).
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

// ---------------------------------------------------------------------------
// KafkaOutboundProducer
// ---------------------------------------------------------------------------

/// Kafka-backed producer for the outbound queue.
///
/// Publishes `EventEnvelope` payloads to a Kafka topic, partitioned by
/// `aggregate_id`.
pub struct KafkaOutboundProducer {
    partition_client: Arc<PartitionClient>,
    #[allow(dead_code)]
    topic: String,
}

impl KafkaOutboundProducer {
    /// Build a new Kafka outbound producer.
    pub async fn new(config: &KafkaOutboundProducerConfig) -> Result<Self, OutboundQueueError> {
        let broker_list: Vec<String> = config
            .brokers
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let client = ClientBuilder::new(broker_list)
            .build()
            .await
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        let partition_client = client
            .partition_client(&config.topic, 0, UnknownTopicHandling::Error)
            .await
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(topic = %config.topic, "Kafka outbound producer initialised (rskafka)");

        Ok(Self {
            partition_client: Arc::new(partition_client),
            topic: config.topic.clone(),
        })
    }

    /// Publish an event envelope to the outbound queue.
    pub async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        self.publish_to_kafka(envelope).await
    }

    /// Internal helper that performs the actual Kafka send.
    async fn publish_to_kafka(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        let key = envelope.aggregate_id.as_uuid().to_string();
        let payload =
            serde_json::to_vec(&envelope).map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        let record = Record {
            key: Some(key.into_bytes()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        self.partition_client
            .produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(
            event_id = %envelope.event_id,
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
/// Reads `EventEnvelope` payloads from a Kafka topic with in-memory offset
/// tracking. Restarts from offset 0 on process restart -- application-layer
/// idempotency (Cassandra PK, inbox dedup, projection checkpoint) handles
/// duplicates.
pub struct KafkaOutboundConsumer {
    partition_client: Arc<PartitionClient>,
    receive_timeout_ms: u32,
    next_offset: Mutex<i64>,
}

impl KafkaOutboundConsumer {
    /// Build a new Kafka outbound consumer.
    pub async fn new(config: &KafkaOutboundConsumerConfig) -> Result<Self, OutboundQueueError> {
        let broker_list: Vec<String> = config
            .brokers
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let client = ClientBuilder::new(broker_list)
            .build()
            .await
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        let partition_client = client
            .partition_client(&config.topic, 0, UnknownTopicHandling::Error)
            .await
            .map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        debug!(
            topic = %config.topic,
            group_id = %config.group_id,
            receive_timeout_ms = config.receive_timeout_ms,
            "Kafka outbound consumer initialised (rskafka)"
        );

        Ok(Self {
            partition_client: Arc::new(partition_client),
            receive_timeout_ms: config.receive_timeout_ms,
            next_offset: Mutex::new(0),
        })
    }

    /// Core receive implementation that returns both the envelope and the offset.
    async fn receive_inner(&self) -> Result<Option<(EventEnvelope, i64)>, String> {
        let mut offset = self.next_offset.lock().await;
        let timeout = self.receive_timeout_ms;

        match self
            .partition_client
            .fetch_records(*offset, 1..1_048_576, timeout as i32)
            .await
        {
            Ok((records, _watermark)) => {
                if let Some(record_and_offset) = records.first() {
                    let kafka_offset = record_and_offset.offset;
                    *offset = kafka_offset + 1;

                    let payload = record_and_offset
                        .record
                        .value
                        .as_ref()
                        .ok_or_else(|| "received message with empty payload".to_owned())?;
                    let envelope: EventEnvelope = serde_json::from_slice(payload)
                        .map_err(|e| format!("deserialize error: {e}"))?;
                    Ok(Some((envelope, kafka_offset)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                warn!(error = %e, "Kafka consumer fetch error");
                Err(e.to_string())
            }
        }
    }

    /// Receive the next event envelope from the consumer.
    /// Returns `None` if no messages are available within the configured timeout.
    pub async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        self.receive_inner()
            .await
            .map(|opt| opt.map(|(envelope, _offset)| envelope))
            .map_err(|e: String| OutboundQueueError::Queue(e.into()))
    }

    /// Commit is a no-op with rskafka -- offset is tracked in-memory.
    /// Application-layer idempotency is the safety net on restart.
    pub async fn commit(&self) -> Result<(), OutboundQueueError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ConsumerReceiver impl for KafkaOutboundConsumer
// ---------------------------------------------------------------------------

/// Implements the canon-core `ConsumerReceiver` trait so that
/// `KafkaOutboundConsumer` can be used directly with `Service::start()`.
///
/// The `sequence_number` field in `ReceivedEnvelope` maps to `kafka_offset + 1`
/// (because Kafka offsets are 0-based, but sequence numbers are 1-based).
#[async_trait]
impl ConsumerReceiver for KafkaOutboundConsumer {
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
        self.receive_inner()
            .await
            .map(|opt| {
                opt.map(|(envelope, offset)| ReceivedEnvelope {
                    envelope,
                    // Kafka offsets are 0-based; sequence numbers are 1-based
                    sequence_number: (offset + 1) as u64,
                })
            })
            .map_err(ConsumerReceiverError::Receive)
    }

    async fn commit(&self) -> Result<(), ConsumerReceiverError> {
        // No-op -- in-memory offset tracking, application-layer idempotency
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KafkaOutboundQueue -- convenience wrapper
// ---------------------------------------------------------------------------

/// Combined Kafka-backed implementation of [`OutboundQueue`].
///
/// Wraps both a [`KafkaOutboundProducer`] and a [`KafkaOutboundConsumer`],
/// delegating `publish()` to the producer and `receive()`/`commit()` to the
/// consumer.
pub struct KafkaOutboundQueue {
    producer: KafkaOutboundProducer,
    consumer: KafkaOutboundConsumer,
}

impl KafkaOutboundQueue {
    /// Build a new Kafka outbound queue from the given configuration.
    pub async fn new(config: &KafkaOutboundQueueConfig) -> Result<Self, OutboundQueueError> {
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

        let producer = KafkaOutboundProducer::new(&producer_config).await?;
        let consumer = KafkaOutboundConsumer::new(&consumer_config).await?;

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
