use async_trait::async_trait;

use crate::{AggregateId, EventEnvelope, Version};

/// Trait for appending and loading events with optimistic concurrency.
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Append events for an aggregate. Rejects the write if `expected_version`
    /// does not match the current stored version (optimistic concurrency).
    async fn append(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) -> Result<(), Self::Error>;

    /// Load all events for an aggregate in ascending version order.
    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error>;

    /// Load events for an aggregate where version >= from_version.
    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error>;

    /// Return the current (latest) version for an aggregate, or `Version::initial()` if empty.
    async fn current_version(&self, aggregate_id: &AggregateId) -> Result<Version, Self::Error>;
}
