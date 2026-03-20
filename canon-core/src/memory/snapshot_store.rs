use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{AggregateId, Snapshot};

#[derive(Debug, thiserror::Error)]
pub enum SnapshotStoreError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemorySnapshotStore {
    inner: Arc<Mutex<HashMap<AggregateId, Snapshot>>>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Upsert — always replaces the existing snapshot for this aggregate.
    pub fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError> {
        let mut store = self
            .inner
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        store.insert(snapshot.aggregate_id.clone(), snapshot);
        Ok(())
    }

    /// Load the latest snapshot for an aggregate, or None if none exists.
    pub fn load(&self, aggregate_id: &AggregateId) -> Result<Option<Snapshot>, SnapshotStoreError> {
        let store = self
            .inner
            .lock()
            .map_err(|_| SnapshotStoreError::Poisoned)?;
        Ok(store.get(aggregate_id).cloned())
    }
}

impl Default for InMemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;
    use bytes::Bytes;
    use chrono::Utc;

    #[test]
    fn save_and_load() {
        let store = InMemorySnapshotStore::new();
        let id = AggregateId::new();
        let snap = Snapshot {
            aggregate_id: id.clone(),
            version: Version::initial().next(),
            state: Bytes::from_static(b"state"),
            taken_at: Utc::now(),
        };
        store.save(snap).unwrap();

        let loaded = store.load(&id).unwrap().unwrap();
        assert_eq!(loaded.version.as_u64(), 1);
    }

    #[test]
    fn save_upserts() {
        let store = InMemorySnapshotStore::new();
        let id = AggregateId::new();
        let snap1 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::initial().next(),
            state: Bytes::from_static(b"v1"),
            taken_at: Utc::now(),
        };
        let snap2 = Snapshot {
            aggregate_id: id.clone(),
            version: Version::initial().next().next(),
            state: Bytes::from_static(b"v2"),
            taken_at: Utc::now(),
        };
        store.save(snap1).unwrap();
        store.save(snap2).unwrap();

        let loaded = store.load(&id).unwrap().unwrap();
        assert_eq!(loaded.version.as_u64(), 2);
        assert_eq!(loaded.state.as_ref(), b"v2");
    }

    #[test]
    fn load_returns_none_when_missing() {
        let store = InMemorySnapshotStore::new();
        let result = store.load(&AggregateId::new()).unwrap();
        assert!(result.is_none());
    }
}
