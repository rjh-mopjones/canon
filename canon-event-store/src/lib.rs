pub use canon_core::{AggregateId, EventEnvelope, Version};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("version conflict: expected {expected:?}, found {found:?}")]
    VersionConflict { expected: Version, found: Version },
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Append events for an aggregate. Returns `VersionConflict` if the stream
    /// has advanced past `expected_version`.
    async fn append(
        &self,
        aggregate_id: &AggregateId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<(), EventStoreError>;

    /// Load all events for an aggregate in ascending version order.
    async fn load(&self, aggregate_id: &AggregateId)
        -> Result<Vec<EventEnvelope>, EventStoreError>;

    /// Load events for an aggregate where version >= `from_version`.
    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, EventStoreError>;
}
