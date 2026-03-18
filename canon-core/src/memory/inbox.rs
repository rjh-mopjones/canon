use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::{AggregateId, IncomingMessage, Oversight};

#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("lock poisoned")]
    Poisoned,
}

type OversightFn = Box<dyn Fn(&[IncomingMessage]) -> Oversight + Send + Sync>;

struct InboxState {
    dedup: HashSet<(String, Uuid)>,
    windows: HashMap<(String, AggregateId), Vec<IncomingMessage>>,
    oversight: HashMap<String, OversightFn>,
    dispatched: Vec<Vec<IncomingMessage>>,
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
                dispatched: Vec::new(),
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
            .insert(handler_id.to_owned(), Box::new(oversight_fn));
        Ok(())
    }

    /// Submit a message to the inbox for a specific handler.
    ///
    /// 1. Dedup check — if already seen, return Ok immediately
    /// 2. Insert into dedup set
    /// 3. Append message to the handler+aggregate window
    /// 4. Evaluate oversight:
    ///    - Ready → drain window, store as dispatched batch
    ///    - NotReady → do nothing
    ///    - Discard → clear window without dispatching
    pub fn submit(
        &self,
        handler_id: &str,
        message: IncomingMessage,
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
        state.windows.entry(window_key.clone()).or_default().push(message);

        let decision = {
            let window = state.windows.get(&window_key).map(|v| v.as_slice()).unwrap_or(&[]);
            state
                .oversight
                .get(handler_id)
                .map(|f| f(window))
                .unwrap_or(Oversight::Ready)
        };

        match decision {
            Oversight::Ready => {
                let batch = state.windows.remove(&window_key).unwrap_or_default();
                state.dispatched.push(batch);
            }
            Oversight::NotReady => {}
            Oversight::Discard => {
                state.windows.remove(&window_key);
            }
        }

        Ok(())
    }

    /// Drain all dispatched batches. Used by the orchestrator / tests to
    /// consume ready batches for processing.
    pub fn take_dispatched(&self) -> Result<Vec<Vec<IncomingMessage>>, InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        Ok(std::mem::take(&mut state.dispatched))
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
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
        })
    }

    fn make_command_with_id(aggregate_id: &AggregateId, command_id: Uuid) -> IncomingMessage {
        IncomingMessage::Command(crate::CommandEnvelope {
            command_id,
            aggregate_id: aggregate_id.clone(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
        })
    }

    #[test]
    fn deduplicates_same_handler_and_message() {
        let inbox = InMemoryInbox::new();
        let id = AggregateId::new();
        let cmd_id = Uuid::new_v4();

        inbox.submit("h1", make_command_with_id(&id, cmd_id)).unwrap();
        inbox.submit("h1", make_command_with_id(&id, cmd_id)).unwrap();

        let batches = inbox.take_dispatched().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
    }

    #[test]
    fn dispatches_on_ready() {
        let inbox = InMemoryInbox::new();
        let id = AggregateId::new();

        inbox.submit("h1", make_command(&id)).unwrap();

        let batches = inbox.take_dispatched().unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn holds_on_not_ready() {
        let inbox = InMemoryInbox::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        inbox.submit("h1", make_command(&id)).unwrap();

        let batches = inbox.take_dispatched().unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn discards_without_dispatching() {
        let inbox = InMemoryInbox::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::Discard)
            .unwrap();
        inbox.submit("h1", make_command(&id)).unwrap();

        let batches = inbox.take_dispatched().unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn oversight_accumulates_until_ready() {
        let inbox = InMemoryInbox::new();
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

        inbox.submit("h1", make_command(&id)).unwrap();
        assert!(inbox.take_dispatched().unwrap().is_empty());

        inbox.submit("h1", make_command(&id)).unwrap();
        let batches = inbox.take_dispatched().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
    }
}
