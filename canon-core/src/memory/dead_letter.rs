use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::DeadLetterError;
use crate::traits::DeadLetterStore;
use crate::{AggregateId, DeadLetter};

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
            .ok_or(DeadLetterError::NotFound { id })?;
        entry.requeue = true;
        Ok(())
    }

    /// Remove a dead letter entirely.
    pub fn discard(&self, id: Uuid) -> Result<(), DeadLetterError> {
        let mut store = self.inner.lock().map_err(|_| DeadLetterError::Poisoned)?;
        let pos = store
            .iter()
            .position(|dl| dl.id == id)
            .ok_or(DeadLetterError::NotFound { id })?;
        store.remove(pos);
        Ok(())
    }
}

#[async_trait]
impl DeadLetterStore for InMemoryDeadLetterStore {
    type Error = DeadLetterError;

    async fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<Uuid, Self::Error> {
        InMemoryDeadLetterStore::store(self, message_id, handler_id, aggregate_id, payload, error)
    }

    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, Self::Error> {
        let entries = InMemoryDeadLetterStore::list(self, handler_id)?;
        Ok(entries
            .into_iter()
            .map(|e| DeadLetter {
                id: e.id,
                message_id: e.message_id,
                handler_id: e.handler_id,
                aggregate_id: e.aggregate_id,
                payload: e.payload,
                error: e.error,
                attempts: e.attempts,
                created_at: e.created_at,
                last_attempted: e.last_attempted,
            })
            .collect())
    }

    async fn requeue(&self, id: Uuid) -> Result<(), Self::Error> {
        InMemoryDeadLetterStore::requeue(self, id)
    }

    async fn discard(&self, id: Uuid) -> Result<(), Self::Error> {
        InMemoryDeadLetterStore::discard(self, id)
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
    fn requeue_nonexistent_id_returns_err() {
        let store = InMemoryDeadLetterStore::new();
        let result = store.requeue(Uuid::new_v4());
        assert!(matches!(result, Err(DeadLetterError::NotFound { .. })));
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
    fn list_filtered_by_handler_id() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();
        store
            .store(Uuid::new_v4(), "h1", &id, Bytes::from_static(b"{}"), "boom")
            .unwrap();
        store
            .store(
                Uuid::new_v4(),
                "h2",
                &id,
                Bytes::from_static(b"{}"),
                "crash",
            )
            .unwrap();

        assert_eq!(store.list(None).unwrap().len(), 2);
        assert_eq!(store.list(Some("h1")).unwrap().len(), 1);
        assert_eq!(store.list(Some("h2")).unwrap().len(), 1);
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
    fn discard_nonexistent_id_returns_err() {
        let store = InMemoryDeadLetterStore::new();
        let result = store.discard(Uuid::new_v4());
        assert!(matches!(result, Err(DeadLetterError::NotFound { .. })));
    }

    #[tokio::test]
    async fn async_store_and_list() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();

        let dl_id = DeadLetterStore::store(
            &store,
            Uuid::new_v4(),
            "h1",
            &id,
            Bytes::from_static(b"{}"),
            "err",
        )
        .await
        .unwrap();

        let entries = DeadLetterStore::list(&store, None).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, dl_id);
        assert_eq!(entries[0].handler_id, "h1");
    }

    #[tokio::test]
    async fn async_list_filtered() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();

        DeadLetterStore::store(
            &store,
            Uuid::new_v4(),
            "h1",
            &id,
            Bytes::from_static(b"{}"),
            "e1",
        )
        .await
        .unwrap();
        DeadLetterStore::store(
            &store,
            Uuid::new_v4(),
            "h2",
            &id,
            Bytes::from_static(b"{}"),
            "e2",
        )
        .await
        .unwrap();

        let h1_entries = DeadLetterStore::list(&store, Some("h1")).await.unwrap();
        assert_eq!(h1_entries.len(), 1);
    }

    #[tokio::test]
    async fn async_requeue_and_discard() {
        let store = InMemoryDeadLetterStore::new();
        let id = AggregateId::new();

        let dl_id = DeadLetterStore::store(
            &store,
            Uuid::new_v4(),
            "h1",
            &id,
            Bytes::from_static(b"{}"),
            "err",
        )
        .await
        .unwrap();

        DeadLetterStore::requeue(&store, dl_id).await.unwrap();
        // After requeue, entry still exists but has requeue flag
        let entries = DeadLetterStore::list(&store, None).await.unwrap();
        assert_eq!(entries.len(), 1);

        DeadLetterStore::discard(&store, dl_id).await.unwrap();
        let entries = DeadLetterStore::list(&store, None).await.unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn default_creates_empty_store() {
        let store = InMemoryDeadLetterStore::default();
        assert!(store.list(None).unwrap().is_empty());
    }
}
