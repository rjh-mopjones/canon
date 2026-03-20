# canon-projection-store

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-projection-store` defines the `ProjectionStore` port — the persistence abstraction for projection checkpoint tracking. It allows the framework to record which event version each projection has processed, enabling resumption after restarts and coordinating projection rebuilds. The production implementation is provided by `canon-projection-store-pg`.

## Trait

```rust
#[async_trait]
pub trait ProjectionStore: Send + Sync + 'static {
    async fn upsert(&self, projection_id: &str, aggregate_id: &AggregateId, state: &[u8]) -> Result<(), ProjectionStoreError>;
    async fn load(&self, projection_id: &str, aggregate_id: &AggregateId) -> Result<Option<Vec<u8>>, ProjectionStoreError>;
    async fn update_last_version(&self, projection_id: &str, version: Version) -> Result<(), ProjectionStoreError>;
    async fn get_last_version(&self, projection_id: &str) -> Result<Version, ProjectionStoreError>;
    async fn set_rebuilding(&self, projection_id: &str, rebuilding: bool) -> Result<(), ProjectionStoreError>;
    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionStoreError>;
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Checkpoint, ProjectionStoreError>;
    async fn reset_checkpoint(&self, projection_id: &str, target: Version) -> Result<(), ProjectionStoreError>;
}
```

### Rebuild support

The `get_checkpoint` method returns a full `Checkpoint` including the `rebuilding` flag. The `reset_checkpoint` method atomically sets `last_version` to the target and `rebuilding` to `true`, used during projection rebuild to reset the consumer offset to a known-good version.

## Error types

```rust
#[derive(Debug, thiserror::Error)]
pub enum ProjectionStoreError {
    #[error("projection store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
```

## Re-exports

- `Version` — re-exported from `canon-core`

## Usage

A downstream infrastructure crate depends on this trait:

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-projection-store = { path = "../canon-projection-store" }
```

```rust
use canon_projection_store::{ProjectionStore, ProjectionStoreError, Checkpoint, AggregateId, Version};

struct MyProjectionStore { /* ... */ }

#[async_trait::async_trait]
impl ProjectionStore for MyProjectionStore {
    async fn upsert(&self, projection_id: &str, aggregate_id: &AggregateId, state: &[u8]) -> Result<(), ProjectionStoreError> {
        todo!()
    }
    async fn load(&self, projection_id: &str, aggregate_id: &AggregateId) -> Result<Option<Vec<u8>>, ProjectionStoreError> {
        todo!()
    }
    async fn update_last_version(&self, projection_id: &str, version: Version) -> Result<(), ProjectionStoreError> {
        todo!()
    }
    async fn get_last_version(&self, projection_id: &str) -> Result<Version, ProjectionStoreError> {
        todo!()
    }
    async fn set_rebuilding(&self, projection_id: &str, rebuilding: bool) -> Result<(), ProjectionStoreError> {
        todo!()
    }
    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionStoreError> {
        todo!()
    }
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Checkpoint, ProjectionStoreError> {
        todo!()
    }
    async fn reset_checkpoint(&self, projection_id: &str, target: Version) -> Result<(), ProjectionStoreError> {
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
chrono = { workspace = true }
```
