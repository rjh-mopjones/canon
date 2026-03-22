use async_trait::async_trait;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage};
use canon_inbound_queue::{InboundQueue, InboundQueueError};
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
            let payload =
                serde_json::to_vec(&wire).map_err(KafkaInboundQueueError::Serialization)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::kafka::apache::{self, Kafka};
    use uuid::Uuid;

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

    fn make_command_envelope() -> CommandEnvelope {
        let agg_id = AggregateId::new();
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: agg_id,
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{\"action\":\"test\"}"),
            command_version: 1,
        }
    }

    /// Try to receive with retries, allowing time for Kafka consumer group rebalancing.
    async fn receive_with_retry(
        queue: &KafkaInboundQueue,
        retries: u32,
    ) -> Option<Vec<IncomingMessage>> {
        for _ in 0..retries {
            if let Ok(Some(batch)) = queue.receive().await {
                return Some(batch);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        None
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_and_receive() {
        let (_container, broker) = setup_kafka_container().await;

        let service_name = format!("test-{}", Uuid::new_v4().simple());
        let group_id = format!("group-{}", Uuid::new_v4());

        let queue = KafkaInboundQueue::new(&broker, &service_name, &group_id)
            .await
            .expect("failed to create inbound queue");

        let cmd = make_command_envelope();
        let agg_id = cmd.aggregate_id.clone();
        let cmd_id = cmd.command_id;

        let msg = IncomingMessage::Command(cmd);
        queue
            .publish(vec![msg], &agg_id)
            .await
            .expect("publish failed");

        let batch = receive_with_retry(&queue, 50)
            .await
            .expect("did not receive published message");

        assert_eq!(batch.len(), 1);
        match &batch[0] {
            IncomingMessage::Command(c) => {
                assert_eq!(c.command_id, cmd_id);
                assert_eq!(c.aggregate_id, agg_id);
                assert_eq!(c.command_version, 1);
            }
            other => panic!("expected Command, got {:?}", std::mem::discriminant(other)),
        }

        queue.commit().await.expect("commit failed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_commit_advances_offset() {
        let (_container, broker) = setup_kafka_container().await;

        let service_name = format!("test-{}", Uuid::new_v4().simple());
        let group_id = format!("group-{}", Uuid::new_v4());

        let queue = KafkaInboundQueue::new(&broker, &service_name, &group_id)
            .await
            .expect("failed to create inbound queue");

        let cmd = make_command_envelope();
        let agg_id = cmd.aggregate_id.clone();

        let msg = IncomingMessage::Command(cmd);
        queue
            .publish(vec![msg], &agg_id)
            .await
            .expect("publish failed");

        // Receive and commit
        let batch = receive_with_retry(&queue, 50)
            .await
            .expect("did not receive published message");
        assert_eq!(batch.len(), 1);
        queue.commit().await.expect("commit failed");

        // After commit, receiving again should return None (no new messages)
        let result = receive_with_retry(&queue, 5).await;
        assert!(
            result.is_none(),
            "expected no more messages after commit, got {:?}",
            result
        );
    }
}
