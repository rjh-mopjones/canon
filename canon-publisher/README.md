# canon-publisher

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-publisher` defines the `EventPublisher` port — the trait that abstracts outbound event publishing to other services via named topics (e.g. `canon.fleet.events`). The outbox processor calls `EventPublisher::publish` after confirming an event is persisted. The concrete implementation lives in `canon-publisher-kafka`.

## Trait

```rust
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    async fn publish(&self, envelope: &EventEnvelope, topic: &str) -> Result<(), PublisherError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("publish error: {0}")]
    Publish(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

- `EventEnvelope`, `AggregateId` from `canon-core`

## Implementations

| Crate | Backend |
|---|---|
| `canon-publisher-kafka` | Apache Kafka via rdkafka |

## Dependencies

- `canon-core`
- `async-trait`
- `thiserror`
