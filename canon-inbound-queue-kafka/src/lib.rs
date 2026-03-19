use async_trait::async_trait;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage};
use canon_queue::{InboundQueue, InboundQueueError};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum KafkaInboundQueueError {
    #[error("producer error: {0}")]
    Producer(#[from] rdkafka::error::KafkaError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("consumer error: {0}")]
    Consumer(rdkafka::error::KafkaError),
    #[error("empty message payload")]
    EmptyPayload,
}

impl From<KafkaInboundQueueError> for InboundQueueError {
    fn from(e: KafkaInboundQueueError) -> Self {
        InboundQueueError::Queue(Box::new(e))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum WireMessage {
    Command(CommandEnvelope),
    InternalEvent(EventEnvelope),
    ExternalEvent(EventEnvelope),
}

impl From<IncomingMessage> for WireMessage {
    fn from(msg: IncomingMessage) -> Self {
        match msg {
            IncomingMessage::Command(c) => WireMessage::Command(c),
            IncomingMessage::InternalEvent(e) => WireMessage::InternalEvent(e),
            IncomingMessage::ExternalEvent(e) => WireMessage::ExternalEvent(e),
        }
    }
}

impl From<WireMessage> for IncomingMessage {
    fn from(wire: WireMessage) -> Self {
        match wire {
            WireMessage::Command(c) => IncomingMessage::Command(c),
            WireMessage::InternalEvent(e) => IncomingMessage::InternalEvent(e),
            WireMessage::ExternalEvent(e) => IncomingMessage::ExternalEvent(e),
        }
    }
}

pub struct KafkaInboundQueue {
    producer: FutureProducer,
    consumer: Arc<Mutex<StreamConsumer>>,
    topic: String,
}

impl KafkaInboundQueue {
    pub async fn new(
        brokers: &str,
        service_name: &str,
        group_id: &str,
    ) -> Result<Self, InboundQueueError> {
        let topic = format!("canon.{service_name}.inbound");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(KafkaInboundQueueError::Producer)?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(KafkaInboundQueueError::Consumer)?;

        consumer
            .subscribe(&[&topic])
            .map_err(KafkaInboundQueueError::Consumer)?;

        Ok(Self {
            producer,
            consumer: Arc::new(Mutex::new(consumer)),
            topic,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

#[async_trait]
impl InboundQueue for KafkaInboundQueue {
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError> {
        let partition_key = aggregate_id.as_uuid().to_string();

        for msg in batch {
            let wire: WireMessage = msg.into();
            let payload = serde_json::to_vec(&wire)
                .map_err(KafkaInboundQueueError::Serialization)?;

            self.producer
                .send(
                    FutureRecord::to(&self.topic)
                        .key(&partition_key)
                        .payload(&payload),
                    Duration::from_secs(5),
                )
                .await
                .map_err(|(e, _)| KafkaInboundQueueError::Producer(e))?;
        }

        Ok(())
    }

    /// Receives a single message from the inbound topic.
    ///
    /// Returns a single-element batch (`Vec<IncomingMessage>`) per call.
    /// The `Vec` wrapper matches the trait contract shared with batch-oriented
    /// implementations (e.g. RabbitMQ). Callers that need higher throughput
    /// should call `receive()` in a tight loop.
    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        let consumer = self.consumer.lock().await;

        match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
            Ok(Ok(borrowed_msg)) => {
                let payload = borrowed_msg
                    .payload()
                    .ok_or(KafkaInboundQueueError::EmptyPayload)?;

                let wire: WireMessage = serde_json::from_slice(payload)
                    .map_err(KafkaInboundQueueError::Serialization)?;

                Ok(Some(vec![wire.into()]))
            }
            Ok(Err(e)) => Err(KafkaInboundQueueError::Consumer(e).into()),
            Err(_) => Ok(None),
        }
    }

    async fn commit(&self) -> Result<(), InboundQueueError> {
        let consumer = self.consumer.lock().await;
        consumer
            .commit_consumer_state(CommitMode::Sync)
            .map_err(KafkaInboundQueueError::Consumer)?;
        Ok(())
    }
}
