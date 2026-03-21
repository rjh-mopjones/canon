//! In-memory implementation of [`DispatcherStore`] for testing.
//!
//! Combines the in-memory event store and outbox store into a single
//! implementation of the dispatcher store trait.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use uuid::Uuid;

use crate::dispatcher::{DispatcherError, DispatcherStore, InboxCommandRow};
use crate::memory::event_store::InMemoryEventStore;
use crate::memory::outbox_store::InMemoryOutboxStore;
use crate::{AggregateId, CommandEnvelope, EventEnvelope, Version};

/// In-memory implementation of [`DispatcherStore`].
///
/// Maintains its own inbox queue and delegates event loading / outbox
/// writing to the shared in-memory event store and outbox store.
#[derive(Clone)]
pub struct InMemoryDispatcherStore {
    inbox: Arc<Mutex<VecDeque<InboxCommandRow>>>,
    event_store: InMemoryEventStore,
    outbox_store: InMemoryOutboxStore,
}

impl InMemoryDispatcherStore {
    /// Create a new in-memory dispatcher store backed by the given event
    /// store and outbox store.
    pub fn new(event_store: InMemoryEventStore, outbox_store: InMemoryOutboxStore) -> Self {
        Self {
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            event_store,
            outbox_store,
        }
    }

    /// Enqueue a command into the in-memory inbox for processing by the
    /// dispatcher. This simulates the gateway writing to `inbox_messages`.
    pub fn enqueue_command(
        &self,
        handler_id: &str,
        envelope: CommandEnvelope,
    ) -> Result<(), DispatcherError> {
        let row = InboxCommandRow {
            handler_id: handler_id.to_owned(),
            message_id: envelope.command_id,
            aggregate_id: envelope.aggregate_id.clone(),
            envelope,
        };
        self.inbox
            .lock()
            .map_err(|_| DispatcherError::PollFailed {
                reason: "lock poisoned".into(),
            })?
            .push_back(row);
        Ok(())
    }

    /// Return a reference to the underlying outbox store (for testing).
    pub fn outbox_store(&self) -> &InMemoryOutboxStore {
        &self.outbox_store
    }

    /// Return a reference to the underlying event store (for testing).
    pub fn event_store(&self) -> &InMemoryEventStore {
        &self.event_store
    }
}

#[async_trait]
impl DispatcherStore for InMemoryDispatcherStore {
    async fn poll_inbox(&self, batch_size: usize) -> Result<Vec<InboxCommandRow>, DispatcherError> {
        let mut inbox = self.inbox.lock().map_err(|_| DispatcherError::PollFailed {
            reason: "lock poisoned".into(),
        })?;
        let mut batch = Vec::new();
        for _ in 0..batch_size {
            match inbox.pop_front() {
                Some(row) => batch.push(row),
                None => break,
            }
        }
        Ok(batch)
    }

    async fn load_events(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, DispatcherError> {
        self.event_store
            .load(aggregate_id)
            .map_err(|e| DispatcherError::LoadEventsFailed {
                aggregate_id: aggregate_id.clone(),
                reason: e.to_string(),
            })
    }

    async fn current_version(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Version, DispatcherError> {
        self.event_store.current_version(aggregate_id).map_err(|e| {
            DispatcherError::LoadEventsFailed {
                aggregate_id: aggregate_id.clone(),
                reason: e.to_string(),
            }
        })
    }

    async fn write_outbox_and_mark_processed(
        &self,
        _message_id: Uuid,
        _handler_id: &str,
        envelope: EventEnvelope,
    ) -> Result<(), DispatcherError> {
        self.outbox_store.insert(envelope).map(|_| ()).map_err(|e| {
            DispatcherError::OutboxWriteFailed {
                reason: e.to_string(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;

    fn make_command_envelope(aggregate_id: &AggregateId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        }
    }

    #[tokio::test]
    async fn enqueue_and_poll_returns_command() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        let agg_id = AggregateId::new();
        let envelope = make_command_envelope(&agg_id);
        let cmd_id = envelope.command_id;

        store
            .enqueue_command("Ship", envelope)
            .expect("enqueue should succeed");

        let batch = store.poll_inbox(10).await.expect("poll should succeed");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].message_id, cmd_id);
    }

    #[tokio::test]
    async fn poll_empty_returns_empty() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        let batch = store.poll_inbox(10).await.expect("poll should succeed");
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn load_events_returns_empty_for_new_aggregate() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        let agg_id = AggregateId::new();
        let events = store
            .load_events(&agg_id)
            .await
            .expect("load should succeed");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn write_outbox_adds_entry() {
        let outbox_store = InMemoryOutboxStore::new();
        let store = InMemoryDispatcherStore::new(InMemoryEventStore::new(), outbox_store.clone());

        let agg_id = AggregateId::new();
        let envelope = EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: agg_id,
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        };

        store
            .write_outbox_and_mark_processed(Uuid::new_v4(), "Ship", envelope)
            .await
            .expect("write should succeed");

        assert_eq!(outbox_store.undelivered_count(), 1);
    }
}
