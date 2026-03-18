use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::AggregateId;

#[derive(Debug, thiserror::Error)]
pub enum DeadLetterError {
    #[error("dead letter not found: {0}")]
    NotFound(Uuid),
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Debug, Clone)]
pub struct InMemoryDeadLetter {
    pub id: Uuid,
    pub message_id: Uuid,
    pub handler_id: String,
    pub aggregate_id: AggregateId,
    pub payload: Bytes,
    pub error: String,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
    pub last_attempted: DateTime<Utc>,
    pub requeue: bool,
}

#[derive(Clone)]
pub struct InMemoryDeadLetterStore {
    inner: Arc<Mutex<Vec<InMemoryDeadLetter>>>,
}

impl InMemoryDeadLetterStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Store a new dead letter entry.
    pub fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<Uuid, DeadLetterError> {
        let mut store = self.inner.lock().map_err(|_| DeadLetterError::Poisoned)?;
        let id = Uuid::new_v4();
        let now = Utc::now();
        store.push(InMemoryDeadLetter {
            id,
            message_id,
            handler_id: handler_id.to_owned(),
            aggregate_id: aggregate_id.clone(),
            payload,
            error: error.to_owned(),
            attempts: 1,
            created_at: now,
            last_attempted: now,
            requeue: false,
        });
        Ok(id)
    }

    /// List dead letters, optionally filtered by handler_id.
    pub fn list(
        &self,
        handler_id: Option<&str>,
    ) -> Result<Vec<InMemoryDeadLetter>, DeadLetterError> {
        let store = self.inner.lock().map_err(|_| DeadLetterError::Poisoned)?;
        Ok(store
            .iter()
            .filter(|dl| handler_id.is_none_or(|h| dl.handler_id == h))
            .cloned()
            .collect())
    }

    /// Mark a dead letter for requeue.
    pub fn requeue(&self, id: Uuid) -> Result<(), DeadLetterError> {
        let mut store = self.inner.lock().map_err(|_| DeadLetterError::Poisoned)?;
        let entry = store
            .iter_mut()
            .find(|dl| dl.id == id)
            .ok_or(DeadLetterError::NotFound(id))?;
        entry.requeue = true;
        Ok(())
    }

    /// Remove a dead letter entirely.
    pub fn discard(&self, id: Uuid) -> Result<(), DeadLetterError> {
        let mut store = self.inner.lock().map_err(|_| DeadLetterError::Poisoned)?;
        let pos = store
            .iter()
            .position(|dl| dl.id == id)
            .ok_or(DeadLetterError::NotFound(id))?;
        store.remove(pos);
        Ok(())
    }
}

impl Default for InMemoryDeadLetterStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_list() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();
        store
            .store(Uuid::new_v4(), "h1", &id, Bytes::from_static(b"{}"), "boom")
            .unwrap();
        store
            .store(Uuid::new_v4(), "h2", &id, Bytes::from_static(b"{}"), "crash")
            .unwrap();

        assert_eq!(store.list(None).unwrap().len(), 2);
        assert_eq!(store.list(Some("h1")).unwrap().len(), 1);
    }

    #[test]
    fn requeue_sets_flag() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();
        let dl_id = store
            .store(Uuid::new_v4(), "h1", &id, Bytes::from_static(b"{}"), "err")
            .unwrap();

        store.requeue(dl_id).unwrap();
        let entries = store.list(None).unwrap();
        assert!(entries[0].requeue);
    }

    #[test]
    fn discard_removes_entry() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();
        let dl_id = store
            .store(Uuid::new_v4(), "h1", &id, Bytes::from_static(b"{}"), "err")
            .unwrap();

        store.discard(dl_id).unwrap();
        assert!(store.list(None).unwrap().is_empty());
    }

    #[test]
    fn requeue_not_found() {
        let store = InMemoryDeadLetterStore::new();
        let result = store.requeue(Uuid::new_v4());
        assert!(matches!(result, Err(DeadLetterError::NotFound(_))));
    }

    #[test]
    fn discard_not_found() {
        let store = InMemoryDeadLetterStore::new();
        let result = store.discard(Uuid::new_v4());
        assert!(matches!(result, Err(DeadLetterError::NotFound(_))));
    }
}
