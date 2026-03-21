//! Thin trait crate for event publishing — re-exports from `canon-core`.
//!
//! Infrastructure crates (e.g. `canon-publisher-kafka`) depend on this
//! crate and implement the [`Publisher`] trait.

pub use canon_core::traits::Publisher;
pub use canon_core::{AggregateId, EventEnvelope};

/// Errors returned by [`Publisher`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("publish error: {0}")]
    Publish(#[from] Box<dyn std::error::Error + Send + Sync>),
}
