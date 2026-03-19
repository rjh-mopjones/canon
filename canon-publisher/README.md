# canon-publisher

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-publisher` defines the `EventPublisher` port — the trait that abstracts outbound event publishing to other services via named topics (e.g. `canon.fleet.events`). The outbox processor calls `EventPublisher::publish` after confirming an event is persisted. The concrete implementation lives in `canon-publisher-kafka`.

## Trait

```rust
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    /// Publish an event to a named topic (e.g. "canon.fleet.events").
    /// Called by the outbox worker after confirming the event is persisted.
    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), PublisherError>;
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

- `EventEnvelope` — from `canon-core`

## Usage

A downstream infrastructure crate depends on the trait:

```rust
use canon_publisher::{EventPublisher, EventEnvelope, PublisherError};

pub struct KafkaPublisher { /* ... */ }

#[async_trait::async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), PublisherError> {
        // Kafka publish logic here
        Ok(())
    }
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = "0.1"
thiserror = "1"
```
