use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use samsa::prelude::{
    BrokerAddress, ConsumeMessage, ConsumerGroupBuilder, ProduceMessage, Producer, ProducerBuilder,
    TcpConnection, TopicPartitionsBuilder,
};
use tokio::sync::Mutex;
use tracing::debug;

use canon_core::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};
use canon_core::outbox::{OutboxProcessorError, OutboxPublisher};
use canon_core::EventEnvelope;
use canon_outbound_queue::{OutboundQueue, OutboundQueueError};

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

/// Configuration for the Kafka-backed outbound queue producer.
#[derive(Debug, Clone)]
pub struct KafkaOutboundProducerConfig {
    pub brokers: String,
    pub topic: String,
}

/// Configuration for the Kafka-backed outbound queue consumer.
#[derive(Debug, Clone)]
pub struct KafkaOutboundConsumerConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub session_timeout_ms: u32,
    pub enable_auto_commit: bool,
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
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub session_timeout_ms: u32,
    pub enable_auto_commit: bool,
    pub receive_timeout_ms: u32,
}

impl KafkaOutboundQueueConfig {
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

pub struct KafkaOutboundProducer {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaOutboundProducer {
    pub async fn new(config: &KafkaOutboundProducerConfig) -> Result<Self, OutboundQueueError> {
        let addrs = parse_brokers(&config.brokers);

        let producer = ProducerBuilder::<TcpConnection>::new(addrs, vec![config.topic.clone()])
            .await
            .map_err(|e| OutboundQueueError::Queue(e.to_string().into()))?
            .required_acks(1)
            .clone()
            .build()
            .await;

        debug!(topic = %config.topic, "Kafka outbound producer initialised");

        Ok(Self {
            producer: Arc::new(producer),
            topic: config.topic.clone(),
        })
    }

    pub async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        self.publish_to_kafka(envelope).await
    }

    async fn publish_to_kafka(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        let key = envelope.aggregate_id.as_uuid().to_string();
        let payload =
            serde_json::to_vec(&envelope).map_err(|e| OutboundQueueError::Queue(Box::new(e)))?;

        let msg = ProduceMessage {
            topic: self.topic.clone(),
            partition_id: 0,
            key: Some(bytes::Bytes::from(key.clone())),
            value: Some(bytes::Bytes::from(payload)),
            headers: vec![],
        };

        self.producer.produce(msg).await;

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
/// Uses a background task that drains a samsa `ConsumerGroup` stream into a
/// channel. The `receive()` method reads from this channel with a timeout.
pub struct KafkaOutboundConsumer {
    rx: Mutex<tokio::sync::mpsc::Receiver<ConsumeMessage>>,
    receive_timeout_ms: u32,
}

impl KafkaOutboundConsumer {
    pub async fn new(config: &KafkaOutboundConsumerConfig) -> Result<Self, OutboundQueueError> {
        let addrs = parse_brokers(&config.brokers);

        let assignment = TopicPartitionsBuilder::new()
            .assign(config.topic.clone(), vec![0])
            .build();

        let consumer =
            ConsumerGroupBuilder::<TcpConnection>::new(addrs, config.group_id.clone(), assignment)
                .await
                .map_err(|e| OutboundQueueError::Queue(e.to_string().into()))?
                .build()
                .await
                .map_err(|e| OutboundQueueError::Queue(e.to_string().into()))?;

        debug!(
            topic = %config.topic,
            group_id = %config.group_id,
            receive_timeout_ms = config.receive_timeout_ms,
            "Kafka outbound consumer initialised"
        );

        // Spawn background task to drain the consumer group stream into a channel
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            let stream = consumer.into_stream();
            tokio::pin!(stream);
            while let Some(Ok(batch)) = stream.next().await {
                for msg in batch {
                    if tx.send(msg).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Self {
            rx: Mutex::new(rx),
            receive_timeout_ms: config.receive_timeout_ms,
        })
    }

    async fn receive_inner(&self) -> Result<Option<(EventEnvelope, u64)>, String> {
        let mut rx = self.rx.lock().await;
        let timeout = std::time::Duration::from_millis(self.receive_timeout_ms as u64);

        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(msg)) => {
                if msg.value.is_empty() {
                    return Ok(None);
                }
                let offset = msg.offset as u64;
                let envelope: EventEnvelope = serde_json::from_slice(&msg.value)
                    .map_err(|e| format!("deserialize error: {e}"))?;
                Ok(Some((envelope, offset)))
            }
            Ok(None) => Ok(None), // channel closed
            Err(_) => Ok(None),   // timeout
        }
    }

    async fn commit_inner(&self) -> Result<(), String> {
        // Consumer group auto-commits when the next batch is fetched.
        debug!("Offset commit requested (handled by consumer group auto-commit)");
        Ok(())
    }

    pub async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        self.receive_inner()
            .await
            .map(|opt| opt.map(|(envelope, _offset)| envelope))
            .map_err(|e: String| OutboundQueueError::Queue(e.into()))
    }

    pub async fn commit(&self) -> Result<(), OutboundQueueError> {
        self.commit_inner()
            .await
            .map_err(|e: String| OutboundQueueError::Queue(e.into()))
    }
}

#[async_trait]
impl ConsumerReceiver for KafkaOutboundConsumer {
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
        self.receive_inner()
            .await
            .map(|opt| {
                opt.map(|(envelope, offset)| ReceivedEnvelope {
                    envelope,
                    sequence_number: offset + 1,
                })
            })
            .map_err(ConsumerReceiverError::Receive)
    }

    async fn commit(&self) -> Result<(), ConsumerReceiverError> {
        self.commit_inner()
            .await
            .map_err(ConsumerReceiverError::Commit)
    }
}

// ---------------------------------------------------------------------------
// KafkaOutboundQueue — convenience wrapper
// ---------------------------------------------------------------------------

pub struct KafkaOutboundQueue {
    producer: KafkaOutboundProducer,
    consumer: KafkaOutboundConsumer,
}

impl KafkaOutboundQueue {
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

    pub fn producer(&self) -> &KafkaOutboundProducer {
        &self.producer
    }

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
