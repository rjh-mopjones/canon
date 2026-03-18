use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{ProjectionStore, Version};

#[derive(Debug, thiserror::Error)]
pub enum ProjectionStoreError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemoryProjectionStore {
    inner: Arc<Mutex<HashMap<String, Version>>>,
}

impl ProjectionStore for InMemoryProjectionStore {}

impl InMemoryProjectionStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return the stored checkpoint version, or Version::initial() if not set.
    pub fn get_checkpoint(
        &self,
        projection_id: &str,
    ) -> Result<Version, ProjectionStoreError> {
        let store = self.inner.lock().map_err(|_| ProjectionStoreError::Poisoned)?;
        Ok(store
            .get(projection_id)
            .copied()
            .unwrap_or_else(Version::initial))
    }

    /// Upsert the checkpoint version for a projection.
    pub fn set_checkpoint(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), ProjectionStoreError> {
        let mut store = self.inner.lock().map_err(|_| ProjectionStoreError::Poisoned)?;
        store.insert(projection_id.to_owned(), version);
        Ok(())
    }
}

impl Default for InMemoryProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_checkpoint_is_initial() {
        let store = InMemoryProjectionStore::new();
        let v = store.get_checkpoint("proj-1").unwrap();
        assert_eq!(v, Version::initial());
    }

    #[test]
    fn set_and_get_checkpoint() {
        let store = InMemoryProjectionStore::new();
        let v = Version::initial().next().next().next();
        store.set_checkpoint("proj-1", v).unwrap();
        assert_eq!(store.get_checkpoint("proj-1").unwrap(), v);
    }

    #[test]
    fn set_checkpoint_upserts() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-1", Version::initial().next())
            .unwrap();
        store
            .set_checkpoint("proj-1", Version::initial().next().next())
            .unwrap();
        assert_eq!(
            store.get_checkpoint("proj-1").unwrap().as_u64(),
            2
        );
    }
}
