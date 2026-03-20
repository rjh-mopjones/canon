# canon-inbound-queue

Trait crate for the Canon inbound queue.

## Overview

Defines the `InboundQueue` trait — the contract for carrying assembled, oversight-approved
batches of `IncomingMessage` from the inbox to the dispatcher. Messages are partitioned by
`aggregate_id` to ensure ordering per aggregate. In production this is implemented by
`canon-inbound-queue-kafka`.

## Trait

```rust
#[async_trait]
pub trait InboundQueue: Send + Sync + 'static {
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError>;

    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError>;

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

- `AggregateId` — newtype wrapper around `Uuid` identifying an aggregate instance
- `IncomingMessage` — enum of `Command`, `InternalEvent`, or `ExternalEvent` variants

## Dependencies

- [`canon-core`](../canon-core) — `IncomingMessage`, `AggregateId`
