//! Thin trait crate for command storage — re-exports from `canon-core`.
//!
//! Infrastructure crates (e.g. `canon-command-store-yugabyte`) depend on this
//! crate and implement the [`CommandStore`] trait.

pub use canon_core::traits::CommandStore;
pub use canon_core::{AggregateId, CommandEnvelope, CommandStatus, Version};

/// Errors returned by [`CommandStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum CommandStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}
