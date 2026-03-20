use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub use canon_core::{AggregateId, Version};

/// Checkpoint tracks the last processed event version for a projection.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub projection_id: String,
    pub last_version: Version,
    pub rebuilding: bool,
    pub updated_at: DateTime<Utc>,
}

/// Errors returned by [`ProjectionStore`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionStoreError {
    #[error("projection store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Port for projection state persistence and checkpoint tracking.
///
/// Implementations are responsible for:
/// - JSONB state upsert/load keyed by `(projection_id, aggregate_id)`
/// - `last_version` checkpoint for idempotent apply
/// - `rebuilding` flag for live rebuild flow
#[async_trait]
pub trait ProjectionStore: Send + Sync + 'static {
    /// Upsert projection state as JSONB keyed by (projection_id, aggregate_id).
    async fn upsert(
        &self,
        projection_id: &str,
        aggregate_id: &AggregateId,
        state: &[u8],
    ) -> Result<(), ProjectionStoreError>;

    /// Load JSONB state for a (projection_id, aggregate_id) key.
    async fn load(
        &self,
        projection_id: &str,
        aggregate_id: &AggregateId,
    ) -> Result<Option<Vec<u8>>, ProjectionStoreError>;

    /// Update the last-processed event version for a projection.
    async fn update_last_version(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), ProjectionStoreError>;

    /// Get the last-processed event version for a projection.
    /// Returns `Version::initial()` if no checkpoint exists yet.
    async fn get_last_version(&self, projection_id: &str) -> Result<Version, ProjectionStoreError>;

    /// Set the rebuilding flag for a projection.
    async fn set_rebuilding(
        &self,
        projection_id: &str,
        rebuilding: bool,
    ) -> Result<(), ProjectionStoreError>;

    /// Check whether a projection is currently rebuilding.
    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionStoreError>;

    /// Get the full checkpoint for a projection, including rebuilding flag.
    ///
    /// Returns a [`Checkpoint`] with `Version::initial()` and `rebuilding = false`
    /// if no checkpoint exists yet.
    async fn get_checkpoint(&self, projection_id: &str)
        -> Result<Checkpoint, ProjectionStoreError>;

    /// Reset the checkpoint for a projection to a target version.
    ///
    /// Used during projection rebuild to reset the consumer offset to a known-good
    /// version. This sets `last_version` to `target` and `rebuilding` to `true` atomically.
    async fn reset_checkpoint(
        &self,
        projection_id: &str,
        target: Version,
    ) -> Result<(), ProjectionStoreError>;
}
