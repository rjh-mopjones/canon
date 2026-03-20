//! In-memory implementation of [`OutboxStore`] and [`OutboxPublisher`] for testing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::memory::outbound_queue::InMemoryOutboundQueue;
use crate::outbox::{OutboxEntry, OutboxProcessorError, OutboxPublisher, OutboxStore};
use crate::EventEnvelope;

/// Tracks delivery metadata for a stored outbox row.
#[derive(Debug, Clone)]
struct StoredEntry {
    entry: OutboxEntry,
    delivered_at: Option<DateTime<Utc>>,
}

/// In-memory implementation of [`OutboxStore`]. Maintains entries in a
/// `BTreeMap` keyed by `sequence_number` so polling always returns them in
/// order. Thread-safe via `Arc<Mutex<_>>`.
#[derive(Clone)]
pub struct InMemoryOutboxStore {
    inner: Arc<Mutex<InMemoryOutboxStoreInner>>,
}

struct InMemoryOutboxStoreInner {
    entries: BTreeMap<i64, StoredEntry>,
    next_sequence: i64,
}

impl InMemoryOutboxStore {
    /// Create a new, empty in-memory outbox store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryOutboxStoreInner {
                entries: BTreeMap::new(),
                next_sequence: 1,
            })),
        }
    }

    /// Insert a new entry into the outbox (simulates the command handler write
    /// path). Assigns the next sequence number automatically.
    pub fn insert(&self, envelope: EventEnvelope) -> Result<OutboxEntry, OutboxProcessorError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| OutboxProcessorError::PollFailed {
                reason: "lock poisoned".into(),
            })?;

        let seq = inner.next_sequence;
        inner.next_sequence += 1;

        let entry = OutboxEntry {
            id: Uuid::new_v4(),
            sequence_number: seq,
            aggregate_id: envelope.aggregate_id.clone(),
            envelope,
        };

        inner.entries.insert(
            seq,
            StoredEntry {
                entry: entry.clone(),
                delivered_at: None,
            },
        );

        Ok(entry)
    }

    /// Return the number of entries that have NOT been marked as delivered.
    pub fn undelivered_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .entries
                    .values()
                    .filter(|s| s.delivered_at.is_none())
                    .count()
            })
            .unwrap_or(0)
    }

    /// Return the number of entries that have been marked as delivered.
    pub fn delivered_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| {
                inner
                    .entries
                    .values()
                    .filter(|s| s.delivered_at.is_some())
                    .count()
            })
            .unwrap_or(0)
    }
}

impl Default for InMemoryOutboxStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OutboxStore for InMemoryOutboxStore {
    async fn poll_undelivered(
        &self,
        batch_size: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxProcessorError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| OutboxProcessorError::PollFailed {
                reason: "lock poisoned".into(),
            })?;

        let batch: Vec<OutboxEntry> = inner
            .entries
            .values()
            .filter(|s| s.delivered_at.is_none())
            .take(batch_size)
            .map(|s| s.entry.clone())
            .collect();

        Ok(batch)
    }

    async fn mark_delivered(&self, entry_id: Uuid) -> Result<(), OutboxProcessorError> {
        let mut inner =
            self.inner
                .lock()
                .map_err(|_| OutboxProcessorError::MarkDeliveredFailed {
                    entry_id,
                    reason: "lock poisoned".into(),
                })?;

        for stored in inner.entries.values_mut() {
            if stored.entry.id == entry_id {
                stored.delivered_at = Some(Utc::now());
                return Ok(());
            }
        }

        Err(OutboxProcessorError::MarkDeliveredFailed {
            entry_id,
            reason: "entry not found".into(),
        })
    }
}

/// In-memory [`OutboxPublisher`] that delegates to an [`InMemoryOutboundQueue`].
#[derive(Clone)]
pub struct InMemoryOutboxPublisher {
    outbound_queue: InMemoryOutboundQueue,
}

impl InMemoryOutboxPublisher {
    /// Create a new publisher backed by the given in-memory outbound queue.
    pub fn new(outbound_queue: InMemoryOutboundQueue) -> Self {
        Self { outbound_queue }
    }
}

#[async_trait]
impl OutboxPublisher for InMemoryOutboxPublisher {
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboxProcessorError> {
        self.outbound_queue
            .publish(envelope)
            .map_err(|e| OutboxProcessorError::PublishFailed {
                entry_id: Uuid::nil(),
                reason: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateId, Version};
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_envelope() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn insert_and_poll() {
        let store = InMemoryOutboxStore::new();
        let env1 = make_envelope();
        let env2 = make_envelope();
        store.insert(env1).expect("insert should succeed");
        store.insert(env2).expect("insert should succeed");

        let batch = store
            .poll_undelivered(10)
            .await
            .expect("poll should succeed");
        assert_eq!(batch.len(), 2);
        // Should be in sequence order.
        assert!(batch[0].sequence_number < batch[1].sequence_number);
    }

    #[tokio::test]
    async fn mark_delivered_excludes_from_poll() {
        let store = InMemoryOutboxStore::new();
        let entry = store
            .insert(make_envelope())
            .expect("insert should succeed");
        let entry_id = entry.id;

        store
            .mark_delivered(entry_id)
            .await
            .expect("mark delivered should succeed");

        let batch = store
            .poll_undelivered(10)
            .await
            .expect("poll should succeed");
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn mark_delivered_unknown_id_returns_error() {
        let store = InMemoryOutboxStore::new();
        let result = store.mark_delivered(Uuid::new_v4()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn undelivered_and_delivered_counts() {
        let store = InMemoryOutboxStore::new();
        let entry = store
            .insert(make_envelope())
            .expect("insert should succeed");
        store
            .insert(make_envelope())
            .expect("insert should succeed");

        assert_eq!(store.undelivered_count(), 2);
        assert_eq!(store.delivered_count(), 0);

        store
            .mark_delivered(entry.id)
            .await
            .expect("mark delivered should succeed");

        assert_eq!(store.undelivered_count(), 1);
        assert_eq!(store.delivered_count(), 1);
    }

    #[tokio::test]
    async fn publisher_delegates_to_outbound_queue() {
        let outbound = InMemoryOutboundQueue::new();
        let consumer = outbound
            .register_consumer()
            .expect("register should succeed");
        let publisher = InMemoryOutboxPublisher::new(outbound.clone());

        let env = make_envelope();
        let event_id = env.event_id;

        publisher
            .publish(env)
            .await
            .expect("publish should succeed");

        let received = outbound.receive(&consumer).expect("receive should succeed");
        assert!(received.is_some());
        assert_eq!(received.as_ref().map(|e| e.event_id), Some(event_id));
    }
}
