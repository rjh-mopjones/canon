# canon-deadletter

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-deadletter` defines the `DeadLetterStore` port — the trait that abstracts persisting and managing messages that have exhausted their retry budget. Failed messages are stored with their error context and can be inspected, requeued for reprocessing, or permanently discarded via an admin API. The infrastructure implementation is provided by `canon-deadletter-yugabyte`.

## Trait

```rust
#[async_trait]
pub trait DeadLetterStore: Send + Sync + 'static {
    /// Persist a dead letter entry.
    async fn store(&self, letter: DeadLetter) -> Result<(), DeadLetterError>;

    /// List dead letters, optionally filtered by handler ID.
    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, DeadLetterError>;

    /// Re-enter a dead letter into the inbox for reprocessing.
    async fn requeue(&self, id: Uuid) -> Result<(), DeadLetterError>;

    /// Permanently remove a dead letter.
    async fn discard(&self, id: Uuid) -> Result<(), DeadLetterError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterError {
    #[error("dead letter error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

The following type is re-exported from `canon-core`:

- `AggregateId` — newtype wrapper around `Uuid` identifying an aggregate instance

## Usage

A downstream infrastructure crate depends on this trait:

```rust
use canon_deadletter::{DeadLetterStore, DeadLetter, DeadLetterError, AggregateId};
use async_trait::async_trait;

pub struct YugabyteDeadLetterStore { /* ... */ }

#[async_trait]
impl DeadLetterStore for YugabyteDeadLetterStore {
    async fn store(&self, letter: DeadLetter) -> Result<(), DeadLetterError> {
        // Insert dead letter row into YugabyteDB
        todo!()
    }

    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, DeadLetterError> {
        // Query dead letters from YugabyteDB
        todo!()
    }

    async fn requeue(&self, id: uuid::Uuid) -> Result<(), DeadLetterError> {
        // Re-enter message into inbox with fresh expires_at
        todo!()
    }

    async fn discard(&self, id: uuid::Uuid) -> Result<(), DeadLetterError> {
        // Delete dead letter row from YugabyteDB
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
bytes = { workspace = true }
chrono = { workspace = true }
uuid = { workspace = true }
```
