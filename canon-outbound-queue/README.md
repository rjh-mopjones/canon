# canon-outbound-queue

Trait crate for the Canon outbound queue.

## Overview

Defines the `OutboundQueue` trait — the contract for carrying committed `EventEnvelope` events
from the outbox processor to downstream consumers. Three independent consumer groups read from
the outbound queue:

1. **Event store consumer** — writes events to Cassandra (+ snapshot trigger)
2. **Projection consumer** — applies events to YugabyteDB read models
3. **Publisher consumer** — publishes events to `canon.{service}.events` for other services

Each consumer group creates its own `OutboundQueue` instance with a distinct `group_id`.

## Trait

```rust
#[async_trait]
pub trait OutboundQueue: Send + Sync + 'static {
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError>;
    async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError>;
    async fn commit(&self) -> Result<(), OutboundQueueError>;
}
```

## Dependencies

- [`canon-core`](../canon-core) — `EventEnvelope`, `AggregateId`
