use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{ProjectionRebuildError, ProjectionRebuildManager, ProjectionStore, Version};

#[derive(Debug, thiserror::Error)]
pub enum ProjectionStoreError {
    #[error("lock poisoned")]
    Poisoned,
}

/// Internal state for a single projection checkpoint.
#[derive(Debug, Clone)]
struct ProjectionCheckpoint {
    version: Version,
    rebuilding: bool,
}

impl Default for ProjectionCheckpoint {
    fn default() -> Self {
        Self {
            version: Version::initial(),
            rebuilding: false,
        }
    }
}

#[derive(Clone)]
pub struct InMemoryProjectionStore {
    inner: Arc<Mutex<HashMap<String, ProjectionCheckpoint>>>,
}

impl ProjectionStore for InMemoryProjectionStore {}

impl InMemoryProjectionStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return the stored checkpoint version, or Version::initial() if not set.
    pub fn get_checkpoint(&self, projection_id: &str) -> Result<Version, ProjectionStoreError> {
        let store = self
            .inner
            .lock()
            .map_err(|_| ProjectionStoreError::Poisoned)?;
        Ok(store
            .get(projection_id)
            .map(|cp| cp.version)
            .unwrap_or_else(Version::initial))
    }

    /// Upsert the checkpoint version for a projection.
    pub fn set_checkpoint(
        &self,
        projection_id: &str,
        version: Version,
    ) -> Result<(), ProjectionStoreError> {
        let mut store = self
            .inner
            .lock()
            .map_err(|_| ProjectionStoreError::Poisoned)?;
        store.entry(projection_id.to_owned()).or_default().version = version;
        Ok(())
    }

    /// Check whether a projection is currently rebuilding.
    pub fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionStoreError> {
        let store = self
            .inner
            .lock()
            .map_err(|_| ProjectionStoreError::Poisoned)?;
        Ok(store
            .get(projection_id)
            .map(|cp| cp.rebuilding)
            .unwrap_or(false))
    }

    /// Set the rebuilding flag for a projection.
    pub fn set_rebuilding(
        &self,
        projection_id: &str,
        rebuilding: bool,
    ) -> Result<(), ProjectionStoreError> {
        let mut store = self
            .inner
            .lock()
            .map_err(|_| ProjectionStoreError::Poisoned)?;
        store
            .entry(projection_id.to_owned())
            .or_default()
            .rebuilding = rebuilding;
        Ok(())
    }
}

impl Default for InMemoryProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory implementation of [`ProjectionRebuildManager`] for testing.
///
/// Wraps an [`InMemoryProjectionStore`] and manages the rebuild lifecycle:
/// `start_rebuild` -> `is_rebuilding` -> `complete_rebuild`.
#[derive(Clone)]
pub struct InMemoryProjectionRebuildManager {
    store: InMemoryProjectionStore,
}

impl InMemoryProjectionRebuildManager {
    pub fn new(store: InMemoryProjectionStore) -> Self {
        Self { store }
    }

    /// Returns a reference to the underlying projection store.
    pub fn store(&self) -> &InMemoryProjectionStore {
        &self.store
    }
}

#[async_trait]
impl ProjectionRebuildManager for InMemoryProjectionRebuildManager {
    async fn start_rebuild(
        &self,
        projection_id: &str,
        rebuild_from: Option<Version>,
    ) -> Result<(), ProjectionRebuildError> {
        // Check if already rebuilding
        let already = self
            .store
            .is_rebuilding(projection_id)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;
        if already {
            return Err(ProjectionRebuildError::AlreadyRebuilding {
                projection_id: projection_id.to_owned(),
            });
        }

        let target = rebuild_from.unwrap_or_else(Version::initial);

        // Validate that rebuild_from is not ahead of the current checkpoint
        let current = self
            .store
            .get_checkpoint(projection_id)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;
        if target.as_u64() > current.as_u64() {
            return Err(ProjectionRebuildError::VersionAhead {
                projection_id: projection_id.to_owned(),
                requested: target,
                current,
            });
        }

        // Set rebuilding flag and reset checkpoint
        self.store
            .set_rebuilding(projection_id, true)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;
        self.store
            .set_checkpoint(projection_id, target)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;

        Ok(())
    }

    async fn is_rebuilding(&self, projection_id: &str) -> Result<bool, ProjectionRebuildError> {
        self.store
            .is_rebuilding(projection_id)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))
    }

    async fn complete_rebuild(&self, projection_id: &str) -> Result<(), ProjectionRebuildError> {
        let rebuilding = self
            .store
            .is_rebuilding(projection_id)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;
        if !rebuilding {
            return Err(ProjectionRebuildError::NotRebuilding {
                projection_id: projection_id.to_owned(),
            });
        }
        self.store
            .set_rebuilding(projection_id, false)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))?;
        Ok(())
    }

    async fn get_checkpoint(&self, projection_id: &str) -> Result<Version, ProjectionRebuildError> {
        self.store
            .get_checkpoint(projection_id)
            .map_err(|e| ProjectionRebuildError::Store(Box::new(e)))
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
        assert_eq!(store.get_checkpoint("proj-1").unwrap().as_u64(), 2);
    }

    #[test]
    fn default_rebuilding_is_false() {
        let store = InMemoryProjectionStore::new();
        assert!(!store.is_rebuilding("proj-1").unwrap());
    }

    #[test]
    fn set_rebuilding_flag() {
        let store = InMemoryProjectionStore::new();
        store.set_rebuilding("proj-1", true).unwrap();
        assert!(store.is_rebuilding("proj-1").unwrap());
        store.set_rebuilding("proj-1", false).unwrap();
        assert!(!store.is_rebuilding("proj-1").unwrap());
    }

    #[tokio::test]
    async fn rebuild_manager_full_lifecycle() {
        let store = InMemoryProjectionStore::new();
        // Simulate a projection at version 10
        store
            .set_checkpoint("proj-1", Version::from_u64(10))
            .unwrap();

        let manager = InMemoryProjectionRebuildManager::new(store);

        // Not rebuilding initially
        assert!(!manager.is_rebuilding("proj-1").await.unwrap());

        // Start rebuild from version 5
        manager
            .start_rebuild("proj-1", Some(Version::from_u64(5)))
            .await
            .unwrap();

        // Should be rebuilding
        assert!(manager.is_rebuilding("proj-1").await.unwrap());

        // Checkpoint should be reset to 5
        assert_eq!(
            manager.get_checkpoint("proj-1").await.unwrap(),
            Version::from_u64(5)
        );

        // Cannot start another rebuild while one is in progress
        let err = manager
            .start_rebuild("proj-1", Some(Version::from_u64(3)))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ProjectionRebuildError::AlreadyRebuilding { .. }
        ));

        // Complete the rebuild
        manager.complete_rebuild("proj-1").await.unwrap();

        // Should no longer be rebuilding
        assert!(!manager.is_rebuilding("proj-1").await.unwrap());
    }

    #[tokio::test]
    async fn rebuild_manager_full_replay_from_beginning() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-1", Version::from_u64(10))
            .unwrap();

        let manager = InMemoryProjectionRebuildManager::new(store);

        // Start rebuild with None (full replay)
        manager.start_rebuild("proj-1", None).await.unwrap();
        assert!(manager.is_rebuilding("proj-1").await.unwrap());
        assert_eq!(
            manager.get_checkpoint("proj-1").await.unwrap(),
            Version::initial()
        );

        manager.complete_rebuild("proj-1").await.unwrap();
    }

    #[tokio::test]
    async fn rebuild_manager_rejects_version_ahead() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-1", Version::from_u64(5))
            .unwrap();

        let manager = InMemoryProjectionRebuildManager::new(store);

        // Requesting rebuild from version 10 when checkpoint is at 5
        let err = manager
            .start_rebuild("proj-1", Some(Version::from_u64(10)))
            .await
            .unwrap_err();
        assert!(matches!(err, ProjectionRebuildError::VersionAhead { .. }));
    }

    #[tokio::test]
    async fn rebuild_manager_rejects_complete_when_not_rebuilding() {
        let store = InMemoryProjectionStore::new();
        let manager = InMemoryProjectionRebuildManager::new(store);

        let err = manager.complete_rebuild("proj-1").await.unwrap_err();
        assert!(matches!(err, ProjectionRebuildError::NotRebuilding { .. }));
    }

    #[tokio::test]
    async fn rebuild_manager_independent_projections() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(10))
            .unwrap();
        store
            .set_checkpoint("proj-b", Version::from_u64(20))
            .unwrap();

        let manager = InMemoryProjectionRebuildManager::new(store);

        // Rebuild proj-a only
        manager
            .start_rebuild("proj-a", Some(Version::from_u64(3)))
            .await
            .unwrap();

        assert!(manager.is_rebuilding("proj-a").await.unwrap());
        assert!(!manager.is_rebuilding("proj-b").await.unwrap());

        // proj-b checkpoint untouched
        assert_eq!(
            manager.get_checkpoint("proj-b").await.unwrap(),
            Version::from_u64(20)
        );
    }
}
