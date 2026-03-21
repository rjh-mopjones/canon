use std::sync::Arc;

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

// ── Arc blanket impl ────────────────────────────────────────────────────────

/// Blanket `EventStore` implementation for `Arc<T>`.
///
/// Enables sharing an event store (e.g. `CassandraEventStore`) between the
/// `ServiceBuilder` (which consumes it) and the `Dispatcher` (which also needs
/// it) via `Arc::clone`.
#[async_trait]
impl<T: EventStore> EventStore for Arc<T> {
    type Error = T::Error;

    async fn append(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) -> Result<(), Self::Error> {
        (**self)
            .append(aggregate_id, expected_version, events)
            .await
    }

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error> {
        (**self).load(aggregate_id).await
    }

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error> {
        (**self).load_from_version(aggregate_id, from_version).await
    }

    async fn current_version(&self, aggregate_id: &AggregateId) -> Result<Version, Self::Error> {
        (**self).current_version(aggregate_id).await
    }
}
