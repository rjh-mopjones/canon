# canon-queue

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-queue` defines the `InboundQueue` trait — the port for the inbound messaging queue that carries assembled, oversight-approved batches of `IncomingMessage` from the inbox to the dispatcher. Messages are partitioned by `aggregate_id` to ensure ordering per aggregate. In production this is implemented by `canon-inbound-queue-kafka`.

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

The following types are re-exported from `canon-core`:

- `AggregateId` — newtype wrapper around `Uuid` identifying an aggregate instance
- `IncomingMessage` — enum of `Command`, `InternalEvent`, or `ExternalEvent` variants

## Usage

A downstream infrastructure crate depends on the trait:

```rust
use canon_queue::{InboundQueue, InboundQueueError, AggregateId, IncomingMessage};
use async_trait::async_trait;

pub struct KafkaInboundQueue { /* ... */ }

#[async_trait]
impl InboundQueue for KafkaInboundQueue {
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError> {
        // Publish to Kafka topic partitioned by aggregate_id
        todo!()
    }

    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        // Poll Kafka consumer group
        todo!()
    }

    async fn commit(&self) -> Result<(), InboundQueueError> {
        // Commit Kafka consumer offset
        todo!()
    }
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = { workspace = true }
thiserror = { workspace = true }
```
