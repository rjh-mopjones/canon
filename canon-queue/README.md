# canon-queue

## Overview

Defines the `InboundQueue` trait, the port for the inbound messaging queue that carries assembled `IncomingMessage` batches from the inbox to the dispatcher, partitioned by `aggregate_id`. All messages for the same aggregate are routed to the same partition, ensuring ordered processing. In production this trait is implemented by `canon-inbound-queue-kafka`.

## Trait

```rust
#[async_trait]
pub trait InboundQueue: Send + Sync + 'static {
    /// Publish an assembled batch of IncomingMessages from the inbox to the inbound queue.
    /// Partitioned by aggregate_id — all messages for the same aggregate go to the same partition.
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError>;

    /// Receive the next batch. Returns None if no messages available.
    /// Consumer group ensures competing consumers across replicas.
    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError>;

    /// Commit the offset for the last received message.
    async fn commit(&self) -> Result<(), InboundQueueError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum InboundQueueError {
    #[error("inbound queue error: {0}")]
    Queue(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

- `AggregateId` from `canon-core`
- `IncomingMessage` from `canon-core`

## Usage

A downstream crate (e.g., `canon-inbound-queue-kafka`) implements the trait:

```rust
use async_trait::async_trait;
use canon_queue::{InboundQueue, InboundQueueError, AggregateId, IncomingMessage};

pub struct KafkaInboundQueue {
    // Kafka producer/consumer configuration
}

#[async_trait]
impl InboundQueue for KafkaInboundQueue {
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError> {
        // Publish to Kafka topic, keyed by aggregate_id for partition routing
        Ok(())
    }

    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        // Poll Kafka consumer group for next batch
        Ok(None)
    }

    async fn commit(&self) -> Result<(), InboundQueueError> {
        // Commit Kafka consumer offset
        Ok(())
    }
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = "0.1"
thiserror = "2"
```
