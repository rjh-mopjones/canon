use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{AggregateId, EventEnvelope, Version};

#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("version conflict: expected {expected}, found {found}")]
    VersionConflict { expected: u64, found: u64 },
    #[error("lock poisoned")]
    Poisoned,
}

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

        let current_version = stored
            .last()
            .map(|e| e.version)
            .unwrap_or_else(Version::initial);

        if current_version != expected_version {
            return Err(EventStoreError::VersionConflict {
                expected: expected_version.as_u64(),
                found: current_version.as_u64(),
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
    fn append_and_load() {
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
    fn rejects_wrong_expected_version() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        let events = vec![make_event(&id)];
        store.append(&id, Version::initial(), events).unwrap();

        let result = store.append(&id, Version::initial(), vec![make_event(&id)]);
        assert!(matches!(result, Err(EventStoreError::VersionConflict { .. })));
    }

    #[test]
    fn load_from_version_filters() {
        let store = InMemoryEventStore::new();
        let id = AggregateId::new();
        store
            .append(&id, Version::initial(), vec![make_event(&id), make_event(&id), make_event(&id)])
            .unwrap();

        let loaded = store.load_from_version(&id, Version::initial().next().next()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 2);
        assert_eq!(loaded[1].version.as_u64(), 3);
    }
}
