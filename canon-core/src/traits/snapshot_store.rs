use async_trait::async_trait;

use crate::{AggregateId, Snapshot};

/// Trait for persisting and loading aggregate snapshots.
#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Upsert a snapshot for an aggregate. Replaces any existing snapshot.
    async fn save(&self, snapshot: Snapshot) -> Result<(), Self::Error>;

    /// Load the latest snapshot for an aggregate, or None if none exists.
    async fn load(&self, aggregate_id: &AggregateId) -> Result<Option<Snapshot>, Self::Error>;
}
