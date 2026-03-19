# canon-command-store

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-command-store` defines the `CommandStore` port — the trait that abstracts persistent storage and retrieval of commands. Commands are stored after being processed by a command handler and are used by the counterfactual replay engine to reconstruct decision history. The infrastructure implementation is provided by `canon-command-store-yugabyte`.

## Trait

```rust
#[async_trait]
pub trait CommandStore: Send + Sync + 'static {
    /// Idempotent — duplicate command_id is silently ignored.
    async fn append(&self, envelope: CommandEnvelope) -> Result<(), CommandStoreError>;

    /// Load commands for an aggregate. Both version bounds are optional.
    /// Used by the counterfactual replay engine.
    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from_version: Option<Version>,
        to_version: Option<Version>,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum CommandStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

The following types are re-exported from `canon-core`:

- `AggregateId` — newtype wrapper around `Uuid` identifying an aggregate instance
- `CommandEnvelope` — metadata wrapper for a command (command_id, aggregate_id, correlation/causation IDs, timestamp, payload, command_version)
- `Version` — monotonically increasing version number for optimistic concurrency

## Usage

A downstream infrastructure crate depends on this trait crate and provides a concrete implementation:

```rust
use async_trait::async_trait;
use canon_command_store::{CommandStore, CommandStoreError, AggregateId, CommandEnvelope, Version};

pub struct YugabyteCommandStore { /* connection pool */ }

#[async_trait]
impl CommandStore for YugabyteCommandStore {
    async fn append(&self, envelope: CommandEnvelope) -> Result<(), CommandStoreError> {
        // INSERT INTO commands (...) ON CONFLICT DO NOTHING
        todo!()
    }

    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from_version: Option<Version>,
        to_version: Option<Version>,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        // SELECT ... FROM commands WHERE aggregate_id = $1 ...
        todo!()
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
