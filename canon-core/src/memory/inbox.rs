use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::error::InboxError;
use crate::memory::inbound_queue::InMemoryInboundQueue;
use crate::{AggregateId, IncomingMessage, Oversight};

type OversightFn = Arc<dyn Fn(&[IncomingMessage]) -> Oversight + Send + Sync>;

struct InboxState {
    dedup: HashSet<(String, Uuid)>,
    windows: HashMap<(String, AggregateId), Vec<IncomingMessage>>,
    oversight: HashMap<String, OversightFn>,
    processed_windows: HashSet<Uuid>,
}

/// In-memory inbox that faithfully reproduces the PostgreSQL inbox behaviour:
/// deduplication, windowed accumulation, and oversight-driven dispatch.
#[derive(Clone)]
pub struct InMemoryInbox {
    inner: Arc<Mutex<InboxState>>,
}

impl InMemoryInbox {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InboxState {
                dedup: HashSet::new(),
                windows: HashMap::new(),
                oversight: HashMap::new(),
                processed_windows: HashSet::new(),
            })),
        }
    }

    /// Register an oversight function for the given handler.
    pub fn register_handler<F>(&self, handler_id: &str, oversight_fn: F) -> Result<(), InboxError>
    where
        F: Fn(&[IncomingMessage]) -> Oversight + Send + Sync + 'static,
    {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        state
            .oversight
            .insert(handler_id.to_owned(), Arc::new(oversight_fn));
        Ok(())
    }

    /// Attempt to mark a window as processed (consumer-side batch idempotency).
    ///
    /// Returns `Ok(true)` if the window was newly marked (caller should process
    /// the batch), or `Ok(false)` if it was already processed (caller should
    /// skip the batch).
    ///
    /// The `handler_id` parameter matches the `Inbox` trait signature for
    /// consistency, though the in-memory implementation does not use it
    /// because `window_id` is globally unique (UUIDv4).
    pub fn try_mark_window_processed(
        &self,
        window_id: Uuid,
        _handler_id: &str,
    ) -> Result<bool, InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        Ok(state.processed_windows.insert(window_id))
    }

    /// Submit a message to the inbox for a specific handler.
    ///
    /// 1. Dedup check — if already seen, return Ok immediately
    /// 2. Insert into dedup set
    /// 3. Append message to the handler+aggregate window
    /// 4. Look up oversight fn — return Err if handler not registered
    /// 5. Evaluate oversight:
    ///    - Ready → drain window, push batch to inbound_queue
    ///    - NotReady → do nothing
    ///    - Discard → clear window without publishing
    pub fn submit(
        &self,
        handler_id: &str,
        message: IncomingMessage,
        inbound_queue: &InMemoryInboundQueue,
    ) -> Result<(), InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;

        let message_id = message.message_id();
        let dedup_key = (handler_id.to_owned(), message_id);
        if state.dedup.contains(&dedup_key) {
            return Ok(());
        }
        state.dedup.insert(dedup_key);

        let aggregate_id = message.aggregate_id().clone();
        let window_key = (handler_id.to_owned(), aggregate_id);
        state
            .windows
            .entry(window_key.clone())
            .or_default()
            .push(message);

        let oversight_fn = state
            .oversight
            .get(handler_id)
            .ok_or_else(|| InboxError::HandlerNotRegistered {
                handler_id: handler_id.to_owned(),
            })?
            .clone();

        let decision = {
            let window = state
                .windows
                .get(&window_key)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            oversight_fn(window)
        };

        match decision {
            Oversight::Ready => {
                let batch = state.windows.remove(&window_key).unwrap_or_default();
                // Release the inbox lock before pushing to the inbound queue
                drop(state);
                inbound_queue
                    .publish(batch)
                    .map_err(|_| InboxError::Poisoned)?;
            }
            Oversight::NotReady => {}
            Oversight::Discard => {
                state.windows.remove(&window_key);
            }
        }

        Ok(())
    }
}

impl Default for InMemoryInbox {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;

    fn make_command(aggregate_id: &AggregateId) -> IncomingMessage {
        IncomingMessage::Command(crate::CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        })
    }

    fn make_command_with_id(aggregate_id: &AggregateId, command_id: Uuid) -> IncomingMessage {
        IncomingMessage::Command(crate::CommandEnvelope {
            command_id,
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        })
    }

    #[test]
    fn submit_same_message_twice_deduplicates() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();
        let cmd_id = Uuid::new_v4();

        inbox.register_handler("h1", |_| Oversight::Ready).unwrap();
        inbox
            .submit("h1", make_command_with_id(&id, cmd_id), &queue)
            .unwrap();
        inbox
            .submit("h1", make_command_with_id(&id, cmd_id), &queue)
            .unwrap();

        // Only one batch should have been enqueued (the first submit)
        let batch = queue.receive().unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        // No more batches
        assert!(queue.receive().unwrap().is_none());
    }

    #[test]
    fn oversight_ready_drains_window_and_enqueues_batch() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox.register_handler("h1", |_| Oversight::Ready).unwrap();
        inbox.submit("h1", make_command(&id), &queue).unwrap();

        let batch = queue.receive().unwrap().unwrap();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn oversight_not_ready_leaves_window_intact_and_does_not_enqueue() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        inbox.submit("h1", make_command(&id), &queue).unwrap();

        assert!(queue.receive().unwrap().is_none());
    }

    #[test]
    fn oversight_discard_clears_window_without_enqueuing() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::Discard)
            .unwrap();
        inbox.submit("h1", make_command(&id), &queue).unwrap();

        assert!(queue.receive().unwrap().is_none());
    }

    #[test]
    fn submit_to_unregistered_handler_returns_err() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        let result = inbox.submit("unknown", make_command(&id), &queue);
        assert!(matches!(
            result,
            Err(InboxError::HandlerNotRegistered { .. })
        ));
    }

    #[test]
    fn try_mark_window_processed_returns_true_for_new_window() {
        let inbox = InMemoryInbox::new();
        let window_id = Uuid::new_v4();
        assert!(inbox.try_mark_window_processed(window_id, "h1").unwrap());
    }

    #[test]
    fn try_mark_window_processed_returns_false_for_duplicate() {
        let inbox = InMemoryInbox::new();
        let window_id = Uuid::new_v4();
        assert!(inbox.try_mark_window_processed(window_id, "h1").unwrap());
        assert!(!inbox.try_mark_window_processed(window_id, "h1").unwrap());
    }

    #[test]
    fn try_mark_window_processed_different_windows_are_independent() {
        let inbox = InMemoryInbox::new();
        let w1 = Uuid::new_v4();
        let w2 = Uuid::new_v4();
        assert!(inbox.try_mark_window_processed(w1, "h1").unwrap());
        assert!(inbox.try_mark_window_processed(w2, "h1").unwrap());
        assert!(!inbox.try_mark_window_processed(w1, "h1").unwrap());
        assert!(!inbox.try_mark_window_processed(w2, "h1").unwrap());
    }

    #[test]
    fn oversight_accumulates_until_ready() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |accumulated| {
                if accumulated.len() >= 2 {
                    Oversight::Ready
                } else {
                    Oversight::NotReady
                }
            })
            .unwrap();

        inbox.submit("h1", make_command(&id), &queue).unwrap();
        assert!(queue.receive().unwrap().is_none());

        inbox.submit("h1", make_command(&id), &queue).unwrap();
        let batch = queue.receive().unwrap().unwrap();
        assert_eq!(batch.len(), 2);
    }
}
