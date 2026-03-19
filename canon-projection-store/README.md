# canon-projection-store

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

`canon-projection-store` defines the `ProjectionStore` port — the persistence abstraction for projection checkpoint tracking. It allows the framework to record which event version each projection has processed, enabling resumption after restarts and coordinating projection rebuilds. The production implementation is provided by `canon-projection-store-pg`.

## Trait

```rust
#[async_trait]
pub trait ProjectionStore: Send + Sync + 'static {
    /// Returns the checkpoint for the given projection.
    /// Returns a Checkpoint with `Version::initial()` if no checkpoint exists yet.
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Checkpoint, ProjectionStoreError>;

    /// Upserts the checkpoint version for the given projection.
    async fn set_checkpoint(&self, projection_id: &str, version: Version) -> Result<(), ProjectionStoreError>;
}
```

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
use canon_projection_store::{ProjectionStore, ProjectionStoreError, Checkpoint, Version};

struct MyProjectionStore { /* ... */ }

#[async_trait::async_trait]
impl ProjectionStore for MyProjectionStore {
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Checkpoint, ProjectionStoreError> {
        // query checkpoint from database
        todo!()
    }

    async fn set_checkpoint(&self, projection_id: &str, version: Version) -> Result<(), ProjectionStoreError> {
        // upsert checkpoint in database
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
