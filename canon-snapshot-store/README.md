# canon-snapshot-store

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-snapshot-store` defines the `SnapshotStore` port — the trait that abstracts reading and writing aggregate snapshots. Snapshots allow aggregates to be hydrated from a recent checkpoint rather than replaying every event from the beginning. The infrastructure implementation is provided by `canon-snapshot-store-yugabyte`.

## Trait

```rust
#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    /// Upsert — always overwrites any existing snapshot for this aggregate.
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError>;

    /// Return the most recent snapshot, or None if none exists.
    async fn load(&self, aggregate_id: &AggregateId) -> Result<Option<Snapshot>, SnapshotStoreError>;
}
```

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

The following types are re-exported from `canon-core`:

- `AggregateId` — newtype wrapper around `Uuid` identifying an aggregate instance
- `Version` — newtype wrapper around `u64` representing aggregate version

## Usage

A downstream infrastructure crate depends on this trait:

```rust
use canon_snapshot_store::{SnapshotStore, Snapshot, SnapshotStoreError, AggregateId};
use async_trait::async_trait;

pub struct YugabyteSnapshotStore { /* ... */ }

#[async_trait]
impl SnapshotStore for YugabyteSnapshotStore {
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError> {
        // Insert or update snapshot in YugabyteDB
        todo!()
    }

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Option<Snapshot>, SnapshotStoreError> {
        // Query snapshot from YugabyteDB
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
```
