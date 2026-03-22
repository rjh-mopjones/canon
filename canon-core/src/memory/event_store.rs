use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::EventStoreError;
use crate::traits::EventStore;
use crate::{AggregateId, EventEnvelope, Version};

#[derive(Clone)]
pub struct InMemoryEventStore {
    inner: Arc<Mutex<HashMap<AggregateId, Vec<EventEnvelope>>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Append events for an aggregate. Rejects the write if `expected_version`
    /// does not match the current stored version (optimistic concurrency).
    pub fn append(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        mut events: Vec<EventEnvelope>,
    ) -> Result<(), EventStoreError> {
        let mut store = self.inner.lock().map_err(|_| EventStoreError::Poisoned)?;
        let stored = store.entry(aggregate_id.clone()).or_default();

        let actual = stored.last().map(|e| e.version);
        let current = actual.unwrap_or_else(Version::initial);

        if current != expected_version {
            return Err(EventStoreError::VersionConflict {
                expected: expected_version,
                actual,
            });
        }

        let mut version = expected_version;
        for event in &mut events {
            version = version.next();
            event.version = version;
        }
        stored.extend(events);
        Ok(())
    }

    /// Load all events for an aggregate in ascending version order.
    pub fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, EventStoreError> {
        let store = self.inner.lock().map_err(|_| EventStoreError::Poisoned)?;
        Ok(store.get(aggregate_id).cloned().unwrap_or_default())
    }

    /// Load events for an aggregate where version >= from_version.
    pub fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, EventStoreError> {
        let store = self.inner.lock().map_err(|_| EventStoreError::Poisoned)?;
        Ok(store
            .get(aggregate_id)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| e.version >= from_version)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Return the current (latest) version for an aggregate, or `Version::initial()` if empty.
    pub fn current_version(&self, aggregate_id: &AggregateId) -> Result<Version, EventStoreError> {
        let store = self.inner.lock().map_err(|_| EventStoreError::Poisoned)?;
        Ok(store
            .get(aggregate_id)
            .and_then(|events| events.last().map(|e| e.version))
            .unwrap_or_else(Version::initial))
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    type Error = EventStoreError;

    async fn append(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) -> Result<(), Self::Error> {
        self.append(aggregate_id, expected_version, events)
    }

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error> {
        self.load(aggregate_id)
    }

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error> {
        self.load_from_version(aggregate_id, from_version)
    }

    async fn current_version(&self, aggregate_id: &AggregateId) -> Result<Version, Self::Error> {
        self.current_version(aggregate_id)
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn append_with_correct_version_succeeds() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        let events = vec![make_event(&id), make_event(&id)];
        store.append(&id, Version::initial(), events).unwrap();

        let loaded = store.load(&id).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 1);
        assert_eq!(loaded[1].version.as_u64(), 2);
    }

    #[test]
    fn append_with_wrong_version_returns_err() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        let events = vec![make_event(&id)];
        store.append(&id, Version::initial(), events).unwrap();

        // Expected Version(0) but current is Version(1) — must not panic
        let result = store.append(&id, Version::initial(), vec![make_event(&id)]);
        assert!(matches!(
            result,
            Err(EventStoreError::VersionConflict { .. })
        ));
    }

    #[test]
    fn append_to_empty_store_with_initial_version_succeeds() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        let result = store.append(&id, Version::initial(), vec![make_event(&id)]);
        assert!(result.is_ok());

        let loaded = store.load(&id).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].version.as_u64(), 1);
    }

    #[test]
    fn load_from_version_returns_only_events_at_or_after() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        store
            .append(
                &id,
                Version::initial(),
                vec![make_event(&id), make_event(&id), make_event(&id)],
            )
            .unwrap();

        let loaded = store
            .load_from_version(&id, Version::initial().next().next())
            .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 2);
        assert_eq!(loaded[1].version.as_u64(), 3);
    }

    #[test]
    fn load_returns_empty_for_unknown_aggregate() {
        let store = InMemoryEventStore::new();
        let loaded = store.load(&AggregateId::new()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn load_from_version_returns_empty_for_unknown_aggregate() {
        let store = InMemoryEventStore::new();
        let loaded = store
            .load_from_version(&AggregateId::new(), Version::from_u64(5))
            .unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn current_version_returns_initial_for_unknown_aggregate() {
        let store = InMemoryEventStore::new();
        let version = store.current_version(&AggregateId::new()).unwrap();
        assert_eq!(version, Version::initial());
    }

    #[test]
    fn current_version_returns_latest_after_append() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        store
            .append(
                &id,
                Version::initial(),
                vec![make_event(&id), make_event(&id)],
            )
            .unwrap();

        let version = store.current_version(&id).unwrap();
        assert_eq!(version.as_u64(), 2);
    }

    #[test]
    fn default_creates_new_store() {
        let store = InMemoryEventStore::default();
        let loaded = store.load(&AggregateId::new()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn append_sequential_batches() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();

        // First batch at version 0
        store
            .append(&id, Version::initial(), vec![make_event(&id)])
            .unwrap();

        // Second batch at version 1
        store
            .append(&id, Version::from_u64(1), vec![make_event(&id)])
            .unwrap();

        let loaded = store.load(&id).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 1);
        assert_eq!(loaded[1].version.as_u64(), 2);
    }

    #[test]
    fn load_from_version_returns_all_when_from_is_first() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        store
            .append(
                &id,
                Version::initial(),
                vec![make_event(&id), make_event(&id)],
            )
            .unwrap();

        // From version 1 should return all events
        let loaded = store.load_from_version(&id, Version::from_u64(1)).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn load_from_version_returns_none_when_past_end() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        store
            .append(&id, Version::initial(), vec![make_event(&id)])
            .unwrap();

        let loaded = store
            .load_from_version(&id, Version::from_u64(999))
            .unwrap();
        assert!(loaded.is_empty());
    }

    // -- Async trait impl tests --

    #[tokio::test]
    async fn async_append_and_load() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();

        EventStore::append(&store, &id, Version::initial(), vec![make_event(&id)])
            .await
            .unwrap();

        let loaded = EventStore::load(&store, &id).await.unwrap();
        assert_eq!(loaded.len(), 1);
    }

    #[tokio::test]
    async fn async_load_from_version() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();

        EventStore::append(
            &store,
            &id,
            Version::initial(),
            vec![make_event(&id), make_event(&id)],
        )
        .await
        .unwrap();

        let loaded = EventStore::load_from_version(&store, &id, Version::from_u64(2))
            .await
            .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].version.as_u64(), 2);
    }

    #[tokio::test]
    async fn async_current_version() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();

        let v = EventStore::current_version(&store, &id).await.unwrap();
        assert_eq!(v, Version::initial());

        EventStore::append(&store, &id, Version::initial(), vec![make_event(&id)])
            .await
            .unwrap();

        let v = EventStore::current_version(&store, &id).await.unwrap();
        assert_eq!(v.as_u64(), 1);
    }
}
