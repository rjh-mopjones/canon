use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub use canon_core::Version;

/// Checkpoint tracks the last processed event version for a projection.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub projection_id: String,
    pub last_version: Version,
    pub updated_at: DateTime<Utc>,
}

/// Errors returned by [`ProjectionStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionStoreError {
    #[error("projection store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Port for projection checkpoint persistence.
///
/// Implementations track which event version each projection has processed,
/// enabling resumption after restarts and coordinating projection rebuilds.
#[async_trait]
pub trait ProjectionStore: Send + Sync + 'static {
    /// Returns the checkpoint for the given projection.
    /// Returns a Checkpoint with `Version::initial()` if no checkpoint exists yet.
    async fn get_checkpoint(&self, projection_id: &str) -> Result<Checkpoint, ProjectionStoreError>;

    /// Upserts the checkpoint version for the given projection.
    async fn set_checkpoint(&self, projection_id: &str, version: Version) -> Result<(), ProjectionStoreError>;
}
