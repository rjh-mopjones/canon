//! Thin trait crate for event storage — re-exports from `canon-core`.
//!
//! Infrastructure crates (e.g. `canon-event-store-cassandra`) depend on this
//! crate and implement the [`EventStore`] trait.

pub use canon_core::traits::EventStore;
pub use canon_core::{AggregateId, EventEnvelope, Version};

/// Errors returned by [`EventStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("version conflict: expected {expected:?}, found {found:?}")]
    VersionConflict { expected: Version, found: Version },
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
