use async_trait::async_trait;

use crate::error::EventStoreError;
use crate::memory::event_store::InMemoryEventStore;
use crate::traits::ReplayEventStore;
use crate::{AggregateId, EventEnvelope, Version};

/// In-memory implementation of `ReplayEventStore`.
///
/// Wraps an `InMemoryEventStore`, providing the same data through the
/// read-replica trait interface. In production, this would point at a
/// Cassandra read replica rather than the live event store.
#[derive(Clone)]
pub struct InMemoryReplayEventStore {
    inner: InMemoryEventStore,
}

impl InMemoryReplayEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }

    /// Create a replay event store backed by an existing `InMemoryEventStore`.
    /// Useful in tests where you want the replay store to share data with the
    /// live event store.
    pub fn from_event_store(store: InMemoryEventStore) -> Self {
        Self { inner: store }
    }

    /// Delegate to the underlying event store for test setup (appending events).
    pub fn inner(&self) -> &InMemoryEventStore {
        &self.inner
    }
}

impl Default for InMemoryReplayEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReplayEventStore for InMemoryReplayEventStore {
    type Error = EventStoreError;

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error> {
        self.inner.load(aggregate_id)
    }

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error> {
        self.inner.load_from_version(aggregate_id, from_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::initial(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn load_returns_events_from_underlying_store() {
        let event_store = InMemoryEventStore::new();
        let id = AggregateId::new();
        event_store
            .append(&id, Version::initial(), vec![make_event(&id)])
            .ok();

        let replay_store = InMemoryReplayEventStore::from_event_store(event_store);
        let events = replay_store.load(&id).await.ok();
        assert!(events.is_some());
        let events = events.unwrap_or_default();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn load_from_version_filters_correctly() {
        let event_store = InMemoryEventStore::new();
        let id = AggregateId::new();
        event_store
            .append(
                &id,
                Version::initial(),
                vec![make_event(&id), make_event(&id), make_event(&id)],
            )
            .ok();

        let replay_store = InMemoryReplayEventStore::from_event_store(event_store);
        let events = replay_store
            .load_from_version(&id, Version::from_u64(2))
            .await
            .ok()
            .unwrap_or_default();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].version.as_u64(), 2);
    }

    #[tokio::test]
    async fn new_store_returns_empty_for_unknown_aggregate() {
        let replay_store = InMemoryReplayEventStore::new();
        let id = AggregateId::new();
        let events = replay_store.load(&id).await.ok().unwrap_or_default();
        assert!(events.is_empty());
    }
}
