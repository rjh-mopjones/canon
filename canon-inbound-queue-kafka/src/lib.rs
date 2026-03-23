use async_trait::async_trait;
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage};
use canon_inbound_queue::{InboundQueue, InboundQueueError};
use futures::StreamExt;
use samsa::prelude::{
    BrokerAddress, ConsumeMessage, ConsumerGroupBuilder, ProduceMessage, Producer, ProducerBuilder,
    TcpConnection, TopicPartitionsBuilder,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum KafkaInboundQueueError {
    #[error("producer error: {0}")]
    Producer(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("consumer error: {0}")]
    Consumer(String),
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

/// Parse a comma-separated broker string (e.g. "host:port,host2:port2") into samsa `BrokerAddress` list.
pub fn parse_brokers(brokers: &str) -> Vec<BrokerAddress> {
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

pub struct KafkaInboundQueue {
    producer: Arc<Producer>,
    /// Receives messages from the background consumer stream.
    rx: Mutex<tokio::sync::mpsc::Receiver<ConsumeMessage>>,
    topic: String,
}

impl KafkaInboundQueue {
    pub async fn new(
        brokers: &str,
        service_name: &str,
        group_id: &str,
    ) -> Result<Self, InboundQueueError> {
        let topic = format!("canon.{service_name}.inbound");
        let addrs = parse_brokers(brokers);

        let producer = ProducerBuilder::<TcpConnection>::new(addrs.clone(), vec![topic.clone()])
            .await
            .map_err(|e| KafkaInboundQueueError::Producer(e.to_string()))?
            .required_acks(1)
            .clone()
            .build()
            .await;

        let assignment = TopicPartitionsBuilder::new()
            .assign(topic.clone(), vec![0])
            .build();

        let consumer =
            ConsumerGroupBuilder::<TcpConnection>::new(addrs, group_id.to_owned(), assignment)
                .await
                .map_err(|e| KafkaInboundQueueError::Consumer(e.to_string()))?
                .build()
                .await
                .map_err(|e| KafkaInboundQueueError::Consumer(e.to_string()))?;

        // Spawn background task to drain the consumer stream into a channel
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
            producer: Arc::new(producer),
            rx: Mutex::new(rx),
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

            let produce_msg = ProduceMessage {
                topic: self.topic.clone(),
                partition_id: 0,
                key: Some(bytes::Bytes::from(partition_key.clone())),
                value: Some(bytes::Bytes::from(payload)),
                headers: vec![],
            };

            self.producer.produce(produce_msg).await;
        }

        Ok(())
    }

    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        let mut rx = self.rx.lock().await;

        match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
            Ok(Some(msg)) => {
                if msg.value.is_empty() {
                    return Ok(None);
                }
                let wire: WireMessage = serde_json::from_slice(&msg.value)
                    .map_err(KafkaInboundQueueError::Serialization)?;
                Ok(Some(vec![wire.into()]))
            }
            Ok(None) => Ok(None), // channel closed
            Err(_) => Ok(None),   // timeout
        }
    }

    async fn commit(&self) -> Result<(), InboundQueueError> {
        // Consumer group auto-commits offsets.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

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

    #[test]
    fn parse_brokers_works() {
        let addrs = parse_brokers("localhost:9092,kafka:9093");
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].host, "localhost");
        assert_eq!(addrs[0].port, 9092);
        assert_eq!(addrs[1].host, "kafka");
        assert_eq!(addrs[1].port, 9093);
    }

    #[test]
    fn wire_message_roundtrip() {
        let cmd = make_command_envelope();
        let wire = WireMessage::Command(cmd.clone());
        let json = serde_json::to_vec(&wire).unwrap();
        let deserialized: WireMessage = serde_json::from_slice(&json).unwrap();
        match deserialized {
            WireMessage::Command(c) => assert_eq!(c.command_id, cmd.command_id),
            _ => panic!("expected Command"),
        }
    }
}
