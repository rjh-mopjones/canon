//! Thin trait crate for dead letter storage — re-exports from `canon-core`.
//!
//! Infrastructure crates (e.g. `canon-deadletter-yugabyte`) depend on this
//! crate and implement the [`DeadLetterStore`] trait.

pub use canon_core::traits::DeadLetterStore;
pub use canon_core::{AggregateId, DeadLetter};

/// Errors returned by [`DeadLetterStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterError {
    #[error("dead letter error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
