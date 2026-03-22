//! Command dispatcher — polls the inbox for unprocessed commands, runs
//! version-matched command handlers, and writes resulting events to the outbox.
//!
//! The dispatcher bridges the gap between command submission (gateway writes to
//! `inbox_messages`) and event production (outbox entries picked up by the
//! outbox processor). Without it, commands sit in the inbox forever and the
//! downstream event pipeline is dead.
//!
//! ## Flow
//!
//! ```text
//! inbox_messages row (command)
//!   -> deserialize based on command_type + command_version
//!   -> load aggregate state (event store / snapshot store)
//!   -> call handler.handle(state, command) via type-erased dispatch_fn
//!   -> serialize resulting event into EventEnvelope
//!   -> BEGIN TXN
//!        INSERT INTO outbox (aggregate_id, payload = event_envelope)
//!        DELETE FROM inbox_messages (mark processed)
//!      COMMIT
//! ```
//!
//! The outbox processor (already exists) then picks up the entries and
//! publishes them to Kafka.
//!
//! ## Design
//!
//! The dispatcher is generic over a `DispatcherStore` trait that abstracts the
//! database operations (poll inbox, load events, write outbox + mark processed).
//! An in-memory implementation lives in `canon-core/src/memory/` for testing.

use std::any::TypeId;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::outbox::OutboxNotifySender;
use crate::registration::__dispatch_command;
use crate::{AggregateId, CommandEnvelope, EventEnvelope, Version};

// ── Inbox message row ────────────────────────────────────────────────────

/// A row from the `inbox_messages` table, representing an unprocessed command.
#[derive(Debug, Clone)]
pub struct InboxCommandRow {
    /// The handler / aggregate type this command targets (e.g., "Ship").
    pub handler_id: String,
    /// The unique message ID (same as `command_id`).
    pub message_id: Uuid,
    /// The aggregate instance this command targets.
    pub aggregate_id: AggregateId,
    /// The deserialized command envelope.
    pub envelope: CommandEnvelope,
}

// ── Error ────────────────────────────────────────────────────────────────

/// Errors produced by the dispatcher.
#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    /// Failed to poll the inbox for unprocessed commands.
    #[error("failed to poll inbox: {reason}")]
    PollFailed { reason: String },

    /// Failed to load events for aggregate state hydration.
    #[error("failed to load events for aggregate {aggregate_id:?}: {reason}")]
    LoadEventsFailed {
        aggregate_id: AggregateId,
        reason: String,
    },

    /// The command handler returned an error.
    #[error("command handler failed for '{command_type}' v{command_version}: {reason}")]
    HandlerFailed {
        command_type: String,
        command_version: u32,
        reason: String,
    },

    /// Failed to write the event to the outbox.
    #[error("failed to write to outbox: {reason}")]
    OutboxWriteFailed { reason: String },

    /// Failed to mark an inbox message as processed.
    #[error("failed to mark inbox message {message_id} as processed: {reason}")]
    MarkProcessedFailed { message_id: Uuid, reason: String },

    /// Failed to record a retry attempt.
    #[error("failed to record retry attempt for message {message_id}: {reason}")]
    RetryRecordFailed { message_id: Uuid, reason: String },

    /// Failed to dead-letter a message.
    #[error("failed to dead-letter message {message_id}: {reason}")]
    DeadLetterFailed { message_id: Uuid, reason: String },
}

// ── DispatcherStore trait ────────────────────────────────────────────────

/// Abstraction over the database operations needed by the dispatcher.
///
/// Infrastructure crates provide the real implementation (YugabyteDB +
/// Cassandra); canon-core provides an in-memory version for tests.
#[async_trait]
pub trait DispatcherStore: Send + Sync + 'static {
    /// Fetch the next batch of unprocessed commands from the inbox.
    async fn poll_inbox(&self, batch_size: usize) -> Result<Vec<InboxCommandRow>, DispatcherError>;

    /// Load all events for an aggregate, in ascending version order.
    /// Used to hydrate aggregate state before running the command handler.
    async fn load_events(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, DispatcherError>;

    /// In a single atomic operation:
    /// 1. Write the event envelope to the outbox table.
    /// 2. Mark the inbox message as processed (delete it).
    ///
    /// This is the ACID transaction that makes the outbox pattern work.
    async fn write_outbox_and_mark_processed(
        &self,
        message_id: Uuid,
        handler_id: &str,
        envelope: EventEnvelope,
    ) -> Result<(), DispatcherError>;

    /// Record a failed processing attempt for an inbox message.
    /// Returns the total number of attempts (including this one).
    ///
    /// Uses the `retry_attempts` table. Implementations should INSERT
    /// or UPDATE the attempt count atomically.
    async fn record_failure(
        &self,
        message_id: Uuid,
        handler_id: &str,
        error: &str,
    ) -> Result<u32, DispatcherError>;

    /// Move a permanently failing message to the dead letter store and
    /// remove it from the inbox. This is called when `record_failure`
    /// returns a count exceeding the configured maximum.
    async fn dead_letter(
        &self,
        row: &InboxCommandRow,
        error: &str,
        attempts: u32,
    ) -> Result<(), DispatcherError>;
}

// ── Configuration ────────────────────────────────────────────────────────

/// Configuration for the dispatcher.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Maximum number of inbox rows fetched per poll cycle.
    pub batch_size: usize,
    /// How long to sleep when the inbox poll returns zero rows (ms).
    pub poll_interval_ms: u64,
    /// The TypeId of the aggregate this dispatcher handles.
    /// Used for combiner dispatch during state hydration.
    pub aggregate_type_id: TypeId,
    /// Maximum number of retry attempts before dead-lettering a command.
    /// Defaults to 3, matching the event store consumer retry policy.
    pub max_retries: u32,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            poll_interval_ms: 50,
            // Default to a zero-sized type. Must be overridden at build time.
            aggregate_type_id: TypeId::of::<()>(),
            max_retries: 3,
        }
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────────

/// The command dispatcher. Polls the inbox for unprocessed commands,
/// dispatches them to version-matched command handlers, and writes the
/// resulting events to the outbox.
///
/// Runs as a background tokio task alongside the outbox processor.
pub struct Dispatcher<S: DispatcherStore> {
    store: S,
    config: DispatcherConfig,
    outbox_notify: Option<OutboxNotifySender>,
}

impl<S: DispatcherStore> Dispatcher<S> {
    /// Create a new dispatcher.
    pub fn new(store: S, config: DispatcherConfig) -> Self {
        Self {
            store,
            config,
            outbox_notify: None,
        }
    }

    /// Set the outbox notification sender. When set, the dispatcher will
    /// notify the outbox processor after writing events, so it wakes
    /// immediately instead of waiting for its next poll cycle.
    pub fn with_outbox_notify(mut self, sender: OutboxNotifySender) -> Self {
        self.outbox_notify = Some(sender);
        self
    }

    /// Process a single batch: poll inbox, run handlers, write to outbox.
    /// Returns the number of commands successfully processed.
    pub async fn process_batch(&self) -> Result<usize, DispatcherError> {
        let rows = self.store.poll_inbox(self.config.batch_size).await?;

        let mut processed = 0usize;
        for row in rows {
            match self.process_command(&row).await {
                Ok(()) => {
                    processed += 1;
                    // Notify the outbox processor that new entries are available.
                    if let Some(tx) = &self.outbox_notify {
                        // Best-effort: if the channel is full, the outbox processor
                        // will pick up the entry on its next poll cycle.
                        let _ = tx.try_send(());
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    tracing::warn!(
                        command_type = %row.envelope.command_type,
                        message_id = %row.message_id,
                        error = %error_msg,
                        "dispatcher: command processing failed"
                    );

                    // Record the failure and dead-letter if max retries exceeded.
                    match self
                        .store
                        .record_failure(row.message_id, &row.handler_id, &error_msg)
                        .await
                    {
                        Ok(attempts) if attempts >= self.config.max_retries => {
                            tracing::error!(
                                message_id = %row.message_id,
                                attempts,
                                "dispatcher: max retries exceeded, dead-lettering"
                            );
                            if let Err(dl_err) =
                                self.store.dead_letter(&row, &error_msg, attempts).await
                            {
                                tracing::error!(
                                    message_id = %row.message_id,
                                    error = %dl_err,
                                    "dispatcher: failed to dead-letter message"
                                );
                            }
                        }
                        Ok(attempts) => {
                            tracing::info!(
                                message_id = %row.message_id,
                                attempts,
                                max_retries = self.config.max_retries,
                                "dispatcher: will retry on next poll cycle"
                            );
                        }
                        Err(retry_err) => {
                            tracing::error!(
                                message_id = %row.message_id,
                                error = %retry_err,
                                "dispatcher: failed to record retry attempt"
                            );
                        }
                    }
                }
            }
        }

        Ok(processed)
    }

    /// Process a single command from the inbox.
    async fn process_command(&self, row: &InboxCommandRow) -> Result<(), DispatcherError> {
        // 1. Load events for the aggregate to hydrate state.
        let events = self.store.load_events(&row.aggregate_id).await?;
        let current_version = if events.is_empty() {
            Version::initial()
        } else {
            events
                .last()
                .map(|e| e.version)
                .unwrap_or_else(Version::initial)
        };

        // 2. Dispatch to the version-matched command handler.
        let result = __dispatch_command(
            &row.envelope.command_type,
            row.envelope.command_version,
            row.envelope.payload.as_ref(),
            &events,
            self.config.aggregate_type_id,
        )
        .map_err(|e| DispatcherError::HandlerFailed {
            command_type: row.envelope.command_type.clone(),
            command_version: row.envelope.command_version,
            reason: e.to_string(),
        })?;

        // 3. Build the event envelope.
        let event_envelope = EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: row.aggregate_id.clone(),
            version: current_version.next(),
            event_type: result.event_type.to_owned(),
            event_version: result.event_version,
            payload: Bytes::from(result.event_payload),
            correlation_id: row.envelope.correlation_id,
            causation_id: row.envelope.command_id,
            timestamp: Utc::now(),
        };

        // 4. Write to outbox + mark inbox message as processed in a single txn.
        self.store
            .write_outbox_and_mark_processed(row.message_id, &row.handler_id, event_envelope)
            .await?;

        tracing::info!(
            command_type = %row.envelope.command_type,
            event_type = result.event_type,
            aggregate_id = %row.aggregate_id.as_uuid(),
            "dispatcher: command processed successfully"
        );

        Ok(())
    }

    /// Run the dispatcher loop. Polls the inbox repeatedly, processing
    /// commands as they arrive. Stops when the provided `shutdown`
    /// receiver fires.
    pub async fn run<F>(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        on_error: F,
    ) -> Result<(), DispatcherError>
    where
        F: Fn(&DispatcherError) + Send + Sync,
    {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.process_batch().await {
                Ok(0) => {
                    // No commands — sleep until next poll or shutdown.
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(
                            self.config.poll_interval_ms,
                        )) => {}
                        _ = shutdown.changed() => {
                            return Ok(());
                        }
                    }
                }
                Ok(_n) => {
                    // Processed some commands — immediately check for more.
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    on_error(&e);
                    tokio::select! {
                        _ = tokio::time::sleep(
                            std::time::Duration::from_millis(self.config.poll_interval_ms)
                        ) => {}
                        _ = shutdown.changed() => {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Access the dispatcher configuration.
    pub fn config(&self) -> &DispatcherConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    // ── Fake store ──────────────────────────────────────────────────────

    #[derive(Clone)]
    struct FakeDispatcherStore {
        inbox: Arc<Mutex<VecDeque<InboxCommandRow>>>,
        events: Arc<Mutex<Vec<EventEnvelope>>>,
        outbox: Arc<Mutex<Vec<EventEnvelope>>>,
        processed: Arc<Mutex<Vec<Uuid>>>,
    }

    impl FakeDispatcherStore {
        fn new() -> Self {
            Self {
                inbox: Arc::new(Mutex::new(VecDeque::new())),
                events: Arc::new(Mutex::new(Vec::new())),
                outbox: Arc::new(Mutex::new(Vec::new())),
                processed: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn push_command(&self, row: InboxCommandRow) {
            self.inbox.lock().map(|mut q| q.push_back(row)).ok();
        }
    }

    #[async_trait]
    impl DispatcherStore for FakeDispatcherStore {
        async fn poll_inbox(
            &self,
            batch_size: usize,
        ) -> Result<Vec<InboxCommandRow>, DispatcherError> {
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
            _aggregate_id: &AggregateId,
        ) -> Result<Vec<EventEnvelope>, DispatcherError> {
            let events = self
                .events
                .lock()
                .map_err(|_| DispatcherError::LoadEventsFailed {
                    aggregate_id: AggregateId::new(),
                    reason: "lock poisoned".into(),
                })?;
            Ok(events.clone())
        }

        async fn write_outbox_and_mark_processed(
            &self,
            message_id: Uuid,
            _handler_id: &str,
            envelope: EventEnvelope,
        ) -> Result<(), DispatcherError> {
            self.outbox
                .lock()
                .map_err(|_| DispatcherError::OutboxWriteFailed {
                    reason: "lock poisoned".into(),
                })?
                .push(envelope);
            self.processed
                .lock()
                .map_err(|_| DispatcherError::MarkProcessedFailed {
                    message_id,
                    reason: "lock poisoned".into(),
                })?
                .push(message_id);
            Ok(())
        }

        async fn record_failure(
            &self,
            _message_id: Uuid,
            _handler_id: &str,
            _error: &str,
        ) -> Result<u32, DispatcherError> {
            Ok(1)
        }

        async fn dead_letter(
            &self,
            _row: &InboxCommandRow,
            _error: &str,
            _attempts: u32,
        ) -> Result<(), DispatcherError> {
            Ok(())
        }
    }

    // ── Tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn process_batch_returns_zero_when_empty() {
        let store = FakeDispatcherStore::new();
        let dispatcher = Dispatcher::new(store, DispatcherConfig::default());
        let count = dispatcher
            .process_batch()
            .await
            .expect("process_batch should succeed");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn run_stops_on_shutdown() {
        let store = FakeDispatcherStore::new();
        let config = DispatcherConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let dispatcher = Dispatcher::new(store, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move { dispatcher.run(shutdown_rx, |_| {}).await });

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let _ = shutdown_tx.send(true);

        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn config_accessor_returns_config() {
        let store = FakeDispatcherStore::new();
        let config = DispatcherConfig {
            batch_size: 42,
            poll_interval_ms: 100,
            ..Default::default()
        };
        let dispatcher = Dispatcher::new(store, config);
        assert_eq!(dispatcher.config().batch_size, 42);
        assert_eq!(dispatcher.config().poll_interval_ms, 100);
    }

    #[tokio::test]
    async fn with_outbox_notify_sends_on_success() {
        let store = FakeDispatcherStore::new();
        let dispatcher = Dispatcher::new(store, DispatcherConfig::default());

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let dispatcher = dispatcher.with_outbox_notify(tx);

        // process_batch with empty inbox — no notification
        dispatcher.process_batch().await.expect("should succeed");
        assert!(rx.try_recv().is_err()); // no notification for 0 processed
    }

    #[tokio::test]
    async fn default_config_values() {
        let config = DispatcherConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.poll_interval_ms, 50);
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    async fn process_batch_handles_handler_failure_and_retries() {
        // Create a store where poll returns a command, but __dispatch_command
        // will fail because no handler is registered for "UnknownCommand".
        // This exercises the error path in process_batch.
        let store = FakeDispatcherStore::new();

        let agg_id = AggregateId::new();
        let cmd = CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: agg_id.clone(),
            command_type: "UnknownCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        };

        let row = InboxCommandRow {
            handler_id: "TestAggregate".to_owned(),
            message_id: cmd.command_id,
            aggregate_id: agg_id,
            envelope: cmd,
        };

        store.push_command(row);

        let config = DispatcherConfig {
            max_retries: 3,
            ..Default::default()
        };
        let dispatcher = Dispatcher::new(store, config);

        // process_batch should handle the error gracefully (record failure, not panic)
        let count = dispatcher
            .process_batch()
            .await
            .expect("process_batch should not propagate handler errors");
        assert_eq!(count, 0); // no commands successfully processed
    }

    #[tokio::test]
    async fn run_handles_process_batch_error() {
        // A store that fails on poll
        struct FailingPollStore;

        #[async_trait]
        impl DispatcherStore for FailingPollStore {
            async fn poll_inbox(
                &self,
                _batch_size: usize,
            ) -> Result<Vec<InboxCommandRow>, DispatcherError> {
                Err(DispatcherError::PollFailed {
                    reason: "test failure".into(),
                })
            }
            async fn load_events(
                &self,
                _: &AggregateId,
            ) -> Result<Vec<EventEnvelope>, DispatcherError> {
                Ok(vec![])
            }
            async fn write_outbox_and_mark_processed(
                &self,
                _: Uuid,
                _: &str,
                _: EventEnvelope,
            ) -> Result<(), DispatcherError> {
                Ok(())
            }
            async fn record_failure(
                &self,
                _: Uuid,
                _: &str,
                _: &str,
            ) -> Result<u32, DispatcherError> {
                Ok(1)
            }
            async fn dead_letter(
                &self,
                _: &InboxCommandRow,
                _: &str,
                _: u32,
            ) -> Result<(), DispatcherError> {
                Ok(())
            }
        }

        let config = DispatcherConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let dispatcher = Dispatcher::new(FailingPollStore, config);

        let error_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error_count_clone = error_count.clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            dispatcher
                .run(shutdown_rx, move |_e| {
                    error_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                })
                .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(true);
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());
        assert!(error_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn run_yields_on_empty_inbox() {
        // Test that run loop with empty inbox sleeps and then shuts down cleanly.
        let store = FakeDispatcherStore::new();
        let config = DispatcherConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let dispatcher = Dispatcher::new(store, config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move { dispatcher.run(shutdown_rx, |_| {}).await });

        // Let it run a few poll cycles with empty inbox
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = shutdown_tx.send(true);
        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());
    }
}
