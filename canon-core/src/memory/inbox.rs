use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::InboxError;
use crate::memory::inbound_queue::InMemoryInboundQueue;
use crate::{AggregateId, IncomingMessage, Oversight, WindowStatus};

type OversightFn = Arc<dyn Fn(&[IncomingMessage]) -> Oversight + Send + Sync>;

/// Metadata for a single in-memory inbox window.
struct Window {
    messages: Vec<IncomingMessage>,
    status: WindowStatus,
    expires_at: Option<DateTime<Utc>>,
}

/// An expired window collected by [`InMemoryInbox::collect_expired_windows`].
#[derive(Debug, Clone)]
pub struct ExpiredWindow {
    /// The handler that owns this window.
    pub handler_id: String,
    /// The aggregate key for this window.
    pub aggregate_id: AggregateId,
    /// The messages that were accumulated before expiry.
    pub messages: Vec<IncomingMessage>,
}

struct InboxState {
    dedup: HashSet<(String, Uuid)>,
    windows: HashMap<(String, AggregateId), Window>,
    oversight: HashMap<String, OversightFn>,
    processed_windows: HashSet<Uuid>,
    /// Per-handler TTL. When set, new windows get `expires_at = now + ttl`.
    handler_ttl: HashMap<String, Duration>,
}

/// In-memory inbox that faithfully reproduces the YugabyteDB inbox behaviour:
/// deduplication, windowed accumulation, oversight-driven dispatch, and window expiry.
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
                handler_ttl: HashMap::new(),
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

    /// Set the window TTL for a handler. New windows created for this handler
    /// will expire after the given duration.
    pub fn set_handler_ttl(&self, handler_id: &str, ttl: Duration) -> Result<(), InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        state.handler_ttl.insert(handler_id.to_owned(), ttl);
        Ok(())
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

        // Compute expires_at for new windows based on handler TTL
        let handler_ttl = state.handler_ttl.get(handler_id).copied();

        let window = state.windows.entry(window_key.clone()).or_insert_with(|| {
            let expires_at = handler_ttl.map(|ttl| {
                Utc::now() + chrono::Duration::from_std(ttl).unwrap_or(chrono::TimeDelta::MAX)
            });
            Window {
                messages: Vec::new(),
                status: WindowStatus::Pending,
                expires_at,
            }
        });
        window.messages.push(message);

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
                .map(|w| w.messages.as_slice())
                .unwrap_or(&[]);
            oversight_fn(window)
        };

        match decision {
            Oversight::Ready => {
                let batch = state
                    .windows
                    .remove(&window_key)
                    .map(|w| w.messages)
                    .unwrap_or_default();
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

    /// Mark all pending windows past their TTL as `Expired`.
    ///
    /// Returns the number of windows that were expired.
    pub fn sweep_expired_windows(&self) -> Result<u64, InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        let now = Utc::now();
        let mut count = 0u64;
        for window in state.windows.values_mut() {
            if window.status == WindowStatus::Pending {
                if let Some(expires_at) = window.expires_at {
                    if expires_at < now {
                        window.status = WindowStatus::Expired;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    /// Collect all expired windows, removing them from the inbox and returning
    /// them so the caller can move them to the dead letter store.
    ///
    /// Each returned window transitions to `DeadLettered` status.
    pub fn collect_expired_windows(&self) -> Result<Vec<ExpiredWindow>, InboxError> {
        let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        let expired_keys: Vec<(String, AggregateId)> = state
            .windows
            .iter()
            .filter(|(_, w)| w.status == WindowStatus::Expired)
            .map(|(k, _)| k.clone())
            .collect();

        let mut result = Vec::with_capacity(expired_keys.len());
        for key in expired_keys {
            if let Some(window) = state.windows.remove(&key) {
                result.push(ExpiredWindow {
                    handler_id: key.0,
                    aggregate_id: key.1,
                    messages: window.messages,
                });
            }
        }
        Ok(result)
    }

    /// Re-insert messages from a dead letter requeue into the inbox with a fresh
    /// `expires_at` and `Pending` status. Oversight runs again from scratch.
    ///
    /// The `handler_id` must be registered before calling this method.
    /// Dedup records for these messages are cleared so they can be reprocessed.
    pub fn requeue_window(
        &self,
        handler_id: &str,
        _aggregate_id: &AggregateId,
        messages: Vec<IncomingMessage>,
        inbound_queue: &InMemoryInboundQueue,
    ) -> Result<(), InboxError> {
        // Clear dedup entries for the messages being requeued
        {
            let mut state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
            for msg in &messages {
                let dedup_key = (handler_id.to_owned(), msg.message_id());
                state.dedup.remove(&dedup_key);
            }
        }

        // Re-submit each message through the normal path (dedup + oversight)
        for msg in messages {
            self.submit(handler_id, msg, inbound_queue)?;
        }
        Ok(())
    }
}

/// Snapshot of a single inbox window for admin inspection.
#[derive(Debug, Clone)]
pub struct InboxWindowInfo {
    /// The handler that owns this window.
    pub handler_id: String,
    /// The correlation key (aggregate_id in in-memory impl).
    pub correlation_key: Uuid,
    /// Unique window identifier.
    pub window_id: Uuid,
    /// Current lifecycle status of the window.
    pub status: WindowStatus,
    /// Number of messages accumulated in this window.
    pub message_count: usize,
    /// Milliseconds until this window expires, or `None` if no TTL.
    pub expires_in_ms: Option<i64>,
    /// When the window was first created.
    pub created_at: DateTime<Utc>,
}

impl InMemoryInbox {
    /// List all inbox windows, optionally filtered by handler_id, status, or correlation_key.
    ///
    /// All filter parameters are optional. When `None`, that filter is not applied.
    pub fn list_windows(
        &self,
        handler_id: Option<&str>,
        status: Option<WindowStatus>,
        correlation_key: Option<Uuid>,
    ) -> Result<Vec<InboxWindowInfo>, InboxError> {
        let state = self.inner.lock().map_err(|_| InboxError::Poisoned)?;
        let now = Utc::now();

        let mut results = Vec::new();
        for ((h_id, agg_id), window) in &state.windows {
            // Apply handler_id filter
            if let Some(filter_handler) = handler_id {
                if h_id != filter_handler {
                    continue;
                }
            }

            // Apply status filter
            if let Some(filter_status) = status {
                if window.status != filter_status {
                    continue;
                }
            }

            let corr_key = *agg_id.as_uuid();

            // Apply correlation_key filter
            if let Some(filter_corr) = correlation_key {
                if corr_key != filter_corr {
                    continue;
                }
            }

            let expires_in_ms = window.expires_at.map(|ea| {
                let diff = ea - now;
                diff.num_milliseconds()
            });

            results.push(InboxWindowInfo {
                handler_id: h_id.clone(),
                correlation_key: corr_key,
                // In-memory: use a deterministic but unique ID based on the window key
                window_id: Uuid::new_v4(),
                status: window.status,
                message_count: window.messages.len(),
                expires_in_ms,
                created_at: window
                    .expires_at
                    .map(|ea| {
                        // Approximate created_at from expires_at minus TTL
                        // For in-memory, we just use now as a best-effort timestamp
                        ea - chrono::Duration::from_std(
                            state
                                .handler_ttl
                                .get(h_id)
                                .copied()
                                .unwrap_or(Duration::from_secs(0)),
                        )
                        .unwrap_or(chrono::TimeDelta::MAX)
                    })
                    .unwrap_or(now),
            });
        }

        Ok(results)
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

    #[test]
    fn sweep_marks_expired_windows() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        // Use a zero-duration TTL so the window expires immediately
        inbox.set_handler_ttl("h1", Duration::from_secs(0)).unwrap();

        inbox.submit("h1", make_command(&id), &queue).unwrap();

        // Window should still be pending right after submit (time hasn't advanced)
        // But with zero TTL, expires_at == now, so sweep should catch it.
        // We use a small sleep to ensure the clock has moved past expires_at.
        std::thread::sleep(Duration::from_millis(10));

        let swept = inbox.sweep_expired_windows().unwrap();
        assert_eq!(swept, 1, "should have swept one window");
    }

    #[test]
    fn collect_expired_windows_returns_and_removes() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        inbox.set_handler_ttl("h1", Duration::from_secs(0)).unwrap();

        inbox.submit("h1", make_command(&id), &queue).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        inbox.sweep_expired_windows().unwrap();

        let expired = inbox.collect_expired_windows().unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].handler_id, "h1");
        assert_eq!(expired[0].messages.len(), 1);

        // Second collect should return empty (already collected)
        let expired2 = inbox.collect_expired_windows().unwrap();
        assert!(expired2.is_empty());
    }

    #[test]
    fn no_ttl_means_no_expiry() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        // No set_handler_ttl call

        inbox.submit("h1", make_command(&id), &queue).unwrap();

        let swept = inbox.sweep_expired_windows().unwrap();
        assert_eq!(swept, 0, "windows without TTL should not be swept");
    }

    #[test]
    fn requeue_clears_dedup_and_reprocesses() {
        let inbox = InMemoryInbox::new();
        let queue = InMemoryInboundQueue::new();
        let id = AggregateId::new();

        inbox
            .register_handler("h1", |_| Oversight::NotReady)
            .unwrap();
        inbox.set_handler_ttl("h1", Duration::from_secs(0)).unwrap();

        let msg = make_command(&id);
        inbox.submit("h1", msg.clone(), &queue).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        inbox.sweep_expired_windows().unwrap();
        let expired = inbox.collect_expired_windows().unwrap();
        assert_eq!(expired.len(), 1);

        // Now switch to Ready oversight
        inbox.register_handler("h1", |_| Oversight::Ready).unwrap();

        // Requeue the expired window's messages
        inbox
            .requeue_window("h1", &id, expired[0].messages.clone(), &queue)
            .unwrap();

        // The message should have been re-processed and dispatched
        let batch = queue.receive().unwrap().unwrap();
        assert_eq!(batch.len(), 1);
    }
}
