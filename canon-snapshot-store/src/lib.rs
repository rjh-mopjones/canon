use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

pub use canon_core::{AggregateId, Version};

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub state: Bytes,
    pub taken_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    /// Upsert — always overwrites any existing snapshot for this aggregate.
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError>;

    /// Return the most recent snapshot, or None if none exists.
    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Option<Snapshot>, SnapshotStoreError>;
}
