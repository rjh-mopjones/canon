//! Thin trait crate for snapshot storage — re-exports from `canon-core`.
//!
//! Infrastructure crates (e.g. `canon-snapshot-store-yugabyte`) depend on this
//! crate and implement the [`SnapshotStore`] trait.

pub use canon_core::traits::SnapshotStore;
pub use canon_core::{AggregateId, Snapshot, Version};

/// Errors returned by [`SnapshotStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
