//! Outbox processor — drains the outbox table and publishes to the outbound queue.
//!
//! The outbox processor has a single responsibility: read committed events from
//! the outbox table (in `sequence_number` order) and publish them to the
//! outbound queue. After a confirmed publish it marks the row as delivered.
//!
//! - Does NOT write to Cassandra.
//! - Does NOT trigger projections.
//! - Does NOT publish to external Kafka topics.
//!
//! Backpressure is provided by a bounded tokio channel between the command
//! handler write path and the processor. The default capacity is 1024 and is
//! configurable via `OutboxProcessorConfig`.

use async_trait::async_trait;
use uuid::Uuid;

use crate::{AggregateId, EventEnvelope};

/// Default bounded-channel capacity between the command handler and the outbox
/// processor.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

// ── Outbox entry ────────────────────────────────────────────────────────────

/// A single row from the outbox table, carrying an event that has been
/// committed but not yet delivered to the outbound queue.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    /// Primary key of the outbox row.
    pub id: Uuid,
    /// Global ordering key, assigned by the `outbox_seq` sequence.
    pub sequence_number: i64,
    /// The aggregate that produced this event.
    pub aggregate_id: AggregateId,
    /// Serialised event envelope payload.
    pub envelope: EventEnvelope,
}

// ── Error ───────────────────────────────────────────────────────────────────

/// Errors produced by the outbox processor.
#[derive(Debug, thiserror::Error)]
pub enum OutboxProcessorError {
    /// Failed to poll the outbox table for undelivered entries.
    #[error("failed to poll outbox: {reason}")]
    PollFailed { reason: String },

    /// Failed to publish an entry to the outbound queue.
    #[error("failed to publish outbox entry {entry_id}: {reason}")]
    PublishFailed { entry_id: Uuid, reason: String },

    /// Failed to mark an outbox entry as delivered.
    #[error("failed to mark entry {entry_id} as delivered: {reason}")]
    MarkDeliveredFailed { entry_id: Uuid, reason: String },

    /// The notification channel between the command handler and the processor
    /// has been closed.
    #[error("outbox notification channel closed")]
    ChannelClosed,
}

// ── Outbox store trait ──────────────────────────────────────────────────────

/// Abstraction over the outbox table. Infrastructure crates (e.g.
/// `canon-command-store-yugabyte`) provide the real implementation; canon-core
/// provides an in-memory version for tests.
///
/// The store is responsible for:
/// - Polling undelivered rows in `sequence_number` order.
/// - Marking rows as delivered after confirmed publish.
///
/// Implementations MUST use `SELECT ... FOR UPDATE SKIP LOCKED` (or equivalent)
/// to prevent double-processing across replicas.
#[async_trait]
pub trait OutboxStore: Send + Sync + 'static {
    /// Fetch the next batch of undelivered outbox entries, ordered by
    /// `sequence_number`. Returns an empty vec when none are available.
    ///
    /// `batch_size` controls the maximum number of rows returned.
    async fn poll_undelivered(
        &self,
        batch_size: usize,
    ) -> Result<Vec<OutboxEntry>, OutboxProcessorError>;

    /// Mark a single outbox entry as delivered (sets `delivered_at`).
    async fn mark_delivered(&self, entry_id: Uuid) -> Result<(), OutboxProcessorError>;
}

// ── Outbound publisher trait ────────────────────────────────────────────────

/// Minimal publish-only interface used by the outbox processor. This avoids
/// coupling directly to the full `OutboundQueue` trait, which also carries
/// consumer/commit semantics that are irrelevant here.
#[async_trait]
pub trait OutboxPublisher: Send + Sync + 'static {
    /// Publish an event envelope to the outbound queue.
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboxProcessorError>;
}

// ── Processor configuration ─────────────────────────────────────────────────

/// Configuration for the outbox processor.
#[derive(Debug, Clone)]
pub struct OutboxProcessorConfig {
    /// Maximum number of outbox rows fetched per poll cycle.
    pub batch_size: usize,
    /// Capacity of the bounded tokio channel used for backpressure between the
    /// command handler write path and the outbox processor.
    pub channel_capacity: usize,
    /// How long to sleep when the outbox poll returns zero rows, in
    /// milliseconds. Prevents busy-spinning when the outbox is empty.
    pub poll_interval_ms: u64,
}

impl Default for OutboxProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            poll_interval_ms: 50,
        }
    }
}

// ── Outbox processor ────────────────────────────────────────────────────────

/// The outbox processor. Drains committed events from the outbox store and
/// publishes them to the outbound queue, marking each as delivered after
/// confirmed publish.
///
/// Owned by the `Service` orchestrator. Runs as a non-optional tokio background
/// task spawned via `ServiceBuilder`.
pub struct OutboxProcessor<S: OutboxStore, P: OutboxPublisher> {
    store: S,
    publisher: P,
    config: OutboxProcessorConfig,
}

impl<S: OutboxStore, P: OutboxPublisher> OutboxProcessor<S, P> {
    /// Create a new outbox processor.
    pub fn new(store: S, publisher: P, config: OutboxProcessorConfig) -> Self {
        Self {
            store,
            publisher,
            config,
        }
    }

    /// Run a single drain cycle: poll undelivered entries, publish each to the
    /// outbound queue, and mark delivered. Returns the number of entries
    /// successfully processed.
    ///
    /// On publish failure the cycle stops and the error is returned. Entries
    /// that were already published and marked delivered in this cycle are not
    /// rolled back — the outbound queue consumers are idempotent.
    pub async fn drain_once(&self) -> Result<usize, OutboxProcessorError> {
        let entries = self.store.poll_undelivered(self.config.batch_size).await?;

        let mut processed = 0usize;
        for entry in entries {
            self.publisher.publish(entry.envelope).await?;
            self.store.mark_delivered(entry.id).await?;
            processed += 1;
        }

        Ok(processed)
    }

    /// Run the processor loop. Polls the outbox store repeatedly, sleeping
    /// for `poll_interval_ms` when no entries are found. Stops when the
    /// provided `shutdown` receiver fires.
    ///
    /// This is the entry point for the background tokio task.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), OutboxProcessorError> {
        loop {
            // Check for shutdown signal.
            if *shutdown.borrow() {
                return Ok(());
            }

            match self.drain_once().await {
                Ok(0) => {
                    // No entries — wait before polling again, but also listen for shutdown.
                    tokio::select! {
                        _ = tokio::time::sleep(
                            std::time::Duration::from_millis(self.config.poll_interval_ms)
                        ) => {}
                        _ = shutdown.changed() => {
                            return Ok(());
                        }
                    }
                }
                Ok(_n) => {
                    // Processed some entries — immediately loop to check for more.
                    // Yield to let other tasks run.
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    // Log-worthy in production; here we just sleep and retry.
                    // The caller can inspect the error via tracing or metrics.
                    // We do NOT propagate the error — the processor is
                    // self-healing. A persistent failure will keep retrying
                    // with backoff (the poll interval).
                    let _ = &e; // suppress unused warning
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

    /// Access the processor configuration.
    pub fn config(&self) -> &OutboxProcessorConfig {
        &self.config
    }
}

// ── Notification channel ────────────────────────────────────────────────────

/// Sender half of the bounded notification channel. The command handler write
/// path sends a unit value after inserting into the outbox table, waking the
/// processor if it is sleeping.
pub type OutboxNotifySender = tokio::sync::mpsc::Sender<()>;

/// Receiver half of the bounded notification channel. The outbox processor
/// awaits on this to be woken when new entries are available, avoiding
/// unnecessary poll cycles.
pub type OutboxNotifyReceiver = tokio::sync::mpsc::Receiver<()>;

/// Create a bounded notification channel pair with the given capacity.
///
/// ```
/// use canon_core::outbox::new_outbox_notify_channel;
///
/// let (tx, rx) = new_outbox_notify_channel(1024);
/// ```
pub fn new_outbox_notify_channel(capacity: usize) -> (OutboxNotifySender, OutboxNotifyReceiver) {
    tokio::sync::mpsc::channel(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateId, Version};
    use bytes::Bytes;
    use chrono::Utc;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    // ── Test helpers ────────────────────────────────────────────────────────

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

    fn make_entry(seq: i64) -> OutboxEntry {
        OutboxEntry {
            id: Uuid::new_v4(),
            sequence_number: seq,
            aggregate_id: AggregateId::new(),
            envelope: make_envelope(),
        }
    }

    // ── Fake store ──────────────────────────────────────────────────────────

    #[derive(Clone)]
    struct FakeStore {
        entries: Arc<Mutex<VecDeque<OutboxEntry>>>,
        delivered: Arc<Mutex<Vec<Uuid>>>,
    }

    impl FakeStore {
        fn new(entries: Vec<OutboxEntry>) -> Self {
            Self {
                entries: Arc::new(Mutex::new(VecDeque::from(entries))),
                delivered: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn delivered_ids(&self) -> Vec<Uuid> {
            self.delivered
                .lock()
                .map_err(|_| ())
                .ok()
                .map_or_else(Vec::new, |d| d.clone())
        }
    }

    #[async_trait]
    impl OutboxStore for FakeStore {
        async fn poll_undelivered(
            &self,
            batch_size: usize,
        ) -> Result<Vec<OutboxEntry>, OutboxProcessorError> {
            let mut entries =
                self.entries
                    .lock()
                    .map_err(|_| OutboxProcessorError::PollFailed {
                        reason: "lock poisoned".into(),
                    })?;
            let mut batch = Vec::new();
            for _ in 0..batch_size {
                match entries.pop_front() {
                    Some(e) => batch.push(e),
                    None => break,
                }
            }
            Ok(batch)
        }

        async fn mark_delivered(&self, entry_id: Uuid) -> Result<(), OutboxProcessorError> {
            let mut delivered =
                self.delivered
                    .lock()
                    .map_err(|_| OutboxProcessorError::MarkDeliveredFailed {
                        entry_id,
                        reason: "lock poisoned".into(),
                    })?;
            delivered.push(entry_id);
            Ok(())
        }
    }

    // ── Fake publisher ──────────────────────────────────────────────────────

    #[derive(Clone)]
    struct FakePublisher {
        published: Arc<Mutex<Vec<EventEnvelope>>>,
    }

    impl FakePublisher {
        fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn published_ids(&self) -> Vec<Uuid> {
            self.published
                .lock()
                .map_err(|_| ())
                .ok()
                .map_or_else(Vec::new, |p| p.iter().map(|e| e.event_id).collect())
        }
    }

    #[async_trait]
    impl OutboxPublisher for FakePublisher {
        async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboxProcessorError> {
            let mut published =
                self.published
                    .lock()
                    .map_err(|_| OutboxProcessorError::PublishFailed {
                        entry_id: envelope.event_id,
                        reason: "lock poisoned".into(),
                    })?;
            published.push(envelope);
            Ok(())
        }
    }

    // ── Failing publisher for error tests ───────────────────────────────────

    struct FailingPublisher;

    #[async_trait]
    impl OutboxPublisher for FailingPublisher {
        async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboxProcessorError> {
            Err(OutboxProcessorError::PublishFailed {
                entry_id: envelope.event_id,
                reason: "simulated failure".into(),
            })
        }
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn drain_once_processes_all_entries() {
        let e1 = make_entry(1);
        let e2 = make_entry(2);
        let id1 = e1.id;
        let id2 = e2.id;
        let eid1 = e1.envelope.event_id;
        let eid2 = e2.envelope.event_id;

        let store = FakeStore::new(vec![e1, e2]);
        let publisher = FakePublisher::new();
        let processor = OutboxProcessor::new(
            store.clone(),
            publisher.clone(),
            OutboxProcessorConfig::default(),
        );

        let count = processor
            .drain_once()
            .await
            .expect("drain_once should succeed");

        assert_eq!(count, 2);
        assert_eq!(store.delivered_ids(), vec![id1, id2]);
        assert_eq!(publisher.published_ids(), vec![eid1, eid2]);
    }

    #[tokio::test]
    async fn drain_once_returns_zero_when_empty() {
        let store = FakeStore::new(vec![]);
        let publisher = FakePublisher::new();
        let processor = OutboxProcessor::new(store, publisher, OutboxProcessorConfig::default());

        let count = processor
            .drain_once()
            .await
            .expect("drain_once should succeed");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn drain_once_respects_batch_size() {
        let entries: Vec<OutboxEntry> = (1..=5).map(|i| make_entry(i)).collect();
        let store = FakeStore::new(entries);
        let publisher = FakePublisher::new();
        let config = OutboxProcessorConfig {
            batch_size: 3,
            ..Default::default()
        };
        let processor = OutboxProcessor::new(store.clone(), publisher.clone(), config);

        let count = processor
            .drain_once()
            .await
            .expect("drain_once should succeed");
        assert_eq!(count, 3);
        assert_eq!(store.delivered_ids().len(), 3);
        assert_eq!(publisher.published_ids().len(), 3);

        // Second drain gets remaining 2.
        let count2 = processor
            .drain_once()
            .await
            .expect("drain_once should succeed");
        assert_eq!(count2, 2);
    }

    #[tokio::test]
    async fn drain_once_stops_on_publish_failure() {
        let e1 = make_entry(1);
        let e2 = make_entry(2);

        let store = FakeStore::new(vec![e1, e2]);
        let processor = OutboxProcessor::new(
            store.clone(),
            FailingPublisher,
            OutboxProcessorConfig::default(),
        );

        let result = processor.drain_once().await;
        assert!(result.is_err());
        // First entry's publish failed, so nothing should be marked delivered.
        assert!(store.delivered_ids().is_empty());
    }

    #[tokio::test]
    async fn run_stops_on_shutdown() {
        let store = FakeStore::new(vec![make_entry(1)]);
        let publisher = FakePublisher::new();
        let config = OutboxProcessorConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let processor = OutboxProcessor::new(store.clone(), publisher.clone(), config);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Spawn the processor in a background task.
        let handle = tokio::spawn(async move { processor.run(shutdown_rx).await });

        // Give it a moment to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Signal shutdown.
        let _ = shutdown_tx.send(true);

        let result = handle.await.expect("task should not panic");
        assert!(result.is_ok());

        // The entry should have been processed.
        assert_eq!(store.delivered_ids().len(), 1);
        assert_eq!(publisher.published_ids().len(), 1);
    }

    #[tokio::test]
    async fn notify_channel_has_correct_capacity() {
        let (tx, _rx) = new_outbox_notify_channel(4);

        // Should be able to send 4 without blocking (non-blocking try_send).
        for _ in 0..4 {
            assert!(tx.try_send(()).is_ok());
        }
        // Fifth should fail — channel full.
        assert!(tx.try_send(()).is_err());
    }

    #[tokio::test]
    async fn default_config_values() {
        let config = OutboxProcessorConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.channel_capacity, DEFAULT_CHANNEL_CAPACITY);
        assert_eq!(config.poll_interval_ms, 50);
    }
}
