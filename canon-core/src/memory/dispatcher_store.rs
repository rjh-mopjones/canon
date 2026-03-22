//! In-memory implementation of [`DispatcherStore`] for testing.
//!
//! Combines the in-memory event store and outbox store into a single
//! implementation of the dispatcher store trait.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use uuid::Uuid;

use crate::dispatcher::{DispatcherError, DispatcherStore, InboxCommandRow};
use crate::memory::event_store::InMemoryEventStore;
use crate::memory::outbox_store::InMemoryOutboxStore;
use crate::{AggregateId, CommandEnvelope, EventEnvelope};

/// In-memory implementation of [`DispatcherStore`].
///
/// Maintains its own inbox queue and delegates event loading / outbox
/// writing to the shared in-memory event store and outbox store.
#[derive(Clone)]
pub struct InMemoryDispatcherStore {
    inbox: Arc<Mutex<VecDeque<InboxCommandRow>>>,
    event_store: InMemoryEventStore,
    outbox_store: InMemoryOutboxStore,
    retry_counts: Arc<Mutex<HashMap<Uuid, u32>>>,
    dead_letters: Arc<Mutex<Vec<DeadLetteredCommand>>>,
}

/// A command that has been dead-lettered after exceeding max retries.
#[derive(Debug, Clone)]
pub struct DeadLetteredCommand {
    /// The inbox command row that was dead-lettered.
    pub row: InboxCommandRow,
    /// The last error that caused the failure.
    pub error: String,
    /// Total number of failed attempts.
    pub attempts: u32,
}

impl InMemoryDispatcherStore {
    /// Create a new in-memory dispatcher store backed by the given event
    /// store and outbox store.
    pub fn new(event_store: InMemoryEventStore, outbox_store: InMemoryOutboxStore) -> Self {
        Self {
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            event_store,
            outbox_store,
            retry_counts: Arc::new(Mutex::new(HashMap::new())),
            dead_letters: Arc::new(Mutex::new(Vec::new())),
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

    /// Return the number of dead-lettered commands (for testing).
    pub fn dead_letter_count(&self) -> usize {
        self.dead_letters.lock().map(|dl| dl.len()).unwrap_or(0)
    }

    /// Return the retry count for a specific message (for testing).
    pub fn retry_count(&self, message_id: &Uuid) -> u32 {
        self.retry_counts
            .lock()
            .map(|rc| rc.get(message_id).copied().unwrap_or(0))
            .unwrap_or(0)
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

    async fn record_failure(
        &self,
        message_id: Uuid,
        _handler_id: &str,
        _error: &str,
    ) -> Result<u32, DispatcherError> {
        let mut counts =
            self.retry_counts
                .lock()
                .map_err(|_| DispatcherError::RetryRecordFailed {
                    message_id,
                    reason: "lock poisoned".into(),
                })?;
        let count = counts.entry(message_id).or_insert(0);
        *count += 1;
        Ok(*count)
    }

    async fn dead_letter(
        &self,
        row: &InboxCommandRow,
        error: &str,
        attempts: u32,
    ) -> Result<(), DispatcherError> {
        // Remove from inbox
        let mut inbox = self
            .inbox
            .lock()
            .map_err(|_| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: "lock poisoned".into(),
            })?;
        inbox.retain(|r| r.message_id != row.message_id);
        drop(inbox);

        // Add to dead letter store
        let mut dl = self
            .dead_letters
            .lock()
            .map_err(|_| DispatcherError::DeadLetterFailed {
                message_id: row.message_id,
                reason: "lock poisoned".into(),
            })?;
        dl.push(DeadLetteredCommand {
            row: row.clone(),
            error: error.to_owned(),
            attempts,
        });

        // Clean up retry counts
        if let Ok(mut counts) = self.retry_counts.lock() {
            counts.remove(&row.message_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatcher::InboxCommandRow;
    use crate::Version;
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

    #[tokio::test]
    async fn record_failure_increments_retry_count() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());
        let msg_id = Uuid::new_v4();

        let count1 = store
            .record_failure(msg_id, "Ship", "error")
            .await
            .expect("record should succeed");
        assert_eq!(count1, 1);
        assert_eq!(store.retry_count(&msg_id), 1);

        let count2 = store
            .record_failure(msg_id, "Ship", "error2")
            .await
            .expect("record should succeed");
        assert_eq!(count2, 2);
        assert_eq!(store.retry_count(&msg_id), 2);
    }

    #[tokio::test]
    async fn dead_letter_removes_from_inbox_and_stores() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        let agg_id = AggregateId::new();
        let envelope = make_command_envelope(&agg_id);
        let msg_id = envelope.command_id;

        store
            .enqueue_command("Ship", envelope.clone())
            .expect("enqueue should succeed");
        assert_eq!(store.dead_letter_count(), 0);

        let row = InboxCommandRow {
            handler_id: "Ship".to_owned(),
            message_id: msg_id,
            aggregate_id: agg_id,
            envelope,
        };

        store
            .dead_letter(&row, "permanent failure", 3)
            .await
            .expect("dead_letter should succeed");
        assert_eq!(store.dead_letter_count(), 1);

        // Inbox should be empty after dead lettering
        let batch = store.poll_inbox(10).await.expect("poll should succeed");
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn dead_letter_cleans_up_retry_counts() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        let agg_id = AggregateId::new();
        let envelope = make_command_envelope(&agg_id);
        let msg_id = envelope.command_id;

        // Record some failures
        store.record_failure(msg_id, "Ship", "error").await.unwrap();
        store.record_failure(msg_id, "Ship", "error").await.unwrap();
        assert_eq!(store.retry_count(&msg_id), 2);

        let row = InboxCommandRow {
            handler_id: "Ship".to_owned(),
            message_id: msg_id,
            aggregate_id: agg_id,
            envelope,
        };

        store
            .dead_letter(&row, "permanent failure", 3)
            .await
            .unwrap();
        assert_eq!(store.retry_count(&msg_id), 0); // cleaned up
    }

    #[tokio::test]
    async fn outbox_store_accessor_returns_shared_store() {
        let outbox = InMemoryOutboxStore::new();
        let store = InMemoryDispatcherStore::new(InMemoryEventStore::new(), outbox.clone());

        // Write through the store, verify via the accessor
        let envelope = EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
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
            .unwrap();
        assert_eq!(store.outbox_store().undelivered_count(), 1);
    }

    #[tokio::test]
    async fn event_store_accessor_returns_shared_store() {
        let event_store = InMemoryEventStore::new();
        let store = InMemoryDispatcherStore::new(event_store.clone(), InMemoryOutboxStore::new());

        // The accessor should return the same event store
        let agg_id = AggregateId::new();
        let events = store.load_events(&agg_id).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn retry_count_returns_zero_for_unknown_message() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());
        assert_eq!(store.retry_count(&Uuid::new_v4()), 0);
    }

    #[tokio::test]
    async fn poll_inbox_respects_batch_size() {
        let store =
            InMemoryDispatcherStore::new(InMemoryEventStore::new(), InMemoryOutboxStore::new());

        for _ in 0..5 {
            let agg_id = AggregateId::new();
            store
                .enqueue_command("Ship", make_command_envelope(&agg_id))
                .unwrap();
        }

        let batch = store.poll_inbox(3).await.unwrap();
        assert_eq!(batch.len(), 3);

        let batch2 = store.poll_inbox(10).await.unwrap();
        assert_eq!(batch2.len(), 2);
    }
}
