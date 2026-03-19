# canon-event-store

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-event-store` defines the `EventStore` port — the async trait that abstracts reading and writing event streams with optimistic concurrency control. Infrastructure crate `canon-event-store-cassandra` provides the production implementation backed by Apache Cassandra.

## Trait

```rust
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append(
        &self,
        aggregate_id: &AggregateId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<(), EventStoreError>;

    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, EventStoreError>;

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, EventStoreError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("version conflict: expected {expected:?}, found {found:?}")]
    VersionConflict { expected: Version, found: Version },
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

The following types are re-exported from `canon-core`:

- `AggregateId` — newtype wrapping `Uuid`, identifies an aggregate instance
- `EventEnvelope` — the envelope carrying a serialised event with metadata
- `Version` — monotonic version number for optimistic concurrency

## Usage

Downstream crates depend on the trait without coupling to a specific backend:

```rust
use canon_event_store::{EventStore, EventStoreError, AggregateId, Version};

async fn load_events(store: &impl EventStore, id: &AggregateId) -> Result<(), EventStoreError> {
    let events = store.load(id).await?;
    println!("loaded {} events", events.len());
    Ok(())
}
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
async-trait = "0.1"
thiserror = "1"
```
