//! Projection consumer — applies events to projection read models.
//!
//! Consumes `EventEnvelope` messages from the outbound queue and applies them
//! to registered projection implementations. Tracks a global sequence-number
//! checkpoint per projection for idempotent replay. The sequence number is
//! the outbox `sequence_number` (1-based) or `Kafka offset + 1` — a strictly
//! positive, monotonically increasing global ordering key that spans all
//! aggregates. Using a per-aggregate version would cause cross-aggregate
//! events to be silently skipped.
//!
//! Each projection runs in its own tokio task. While `rebuilding == true`, read
//! endpoints fall back to read-through.
//!
//! Generic over `ProjectionCheckpointStore` so the same consumer logic works
//! with both in-memory test impls and production infrastructure.

use std::sync::Arc;

use tokio::sync::Notify;

use crate::traits::ProjectionCheckpointStore;
use crate::{EventEnvelope, Version};

/// Errors emitted by the projection consumer.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionConsumerError {
    /// Failed to update or read projection checkpoint.
    #[error("projection checkpoint store error: {0}")]
    CheckpointStore(String),

    /// A projection apply function returned an error.
    #[error("projection apply error for '{projection_id}': {message}")]
    ApplyFailed {
        projection_id: String,
        message: String,
    },
}

/// Type-erased projection apply function.
/// Takes `(projection_id, event_envelope)` and applies the event to the projection's read model.
pub type ProjectionApplyFn = Box<dyn Fn(&str, &EventEnvelope) -> Result<(), String> + Send + Sync>;

/// A registered projection for the consumer to apply events to.
pub struct RegisteredProjection {
    /// Unique identifier for checkpoint tracking.
    pub projection_id: String,
    /// The apply function that processes an event for this projection.
    pub apply_fn: ProjectionApplyFn,
}

/// Consumes events from the outbound queue and applies them to projection read models.
///
/// Each projection registered with the consumer has its own checkpoint. The consumer
/// skips events older than the checkpoint (idempotent). After successful apply, it
/// advances the checkpoint.
///
/// Generic over `ProjectionCheckpointStore` so the same logic works with both
/// in-memory and production checkpoint stores.
pub struct ProjectionConsumer<CS>
where
    CS: ProjectionCheckpointStore,
{
    projections: Vec<RegisteredProjection>,
    checkpoint_store: CS,
}

impl<CS> ProjectionConsumer<CS>
where
    CS: ProjectionCheckpointStore,
{
    pub fn new(checkpoint_store: CS) -> Self {
        Self {
            projections: Vec::new(),
            checkpoint_store,
        }
    }

    /// Register a projection with the consumer.
    pub fn register(&mut self, projection: RegisteredProjection) {
        tracing::info!(
            projection_id = %projection.projection_id,
            "projection consumer: registered projection"
        );
        self.projections.push(projection);
    }

    /// Process a single event envelope against all registered projections.
    ///
    /// `sequence_number` is the global ordering key — typically the outbox
    /// `sequence_number` (1-based) or the Kafka offset + 1. It must be
    /// **strictly positive** and increase monotonically across **all**
    /// aggregates. A value of 0 will be silently skipped on a fresh
    /// projection because the initial checkpoint is also 0.
    ///
    /// Using the per-aggregate `envelope.version` would cause events from
    /// different aggregates to be silently skipped whenever one aggregate's
    /// version exceeds another's.
    ///
    /// For each projection:
    /// 1. Read the checkpoint (last processed sequence number).
    /// 2. Skip if `sequence_number` is not newer than the checkpoint.
    /// 3. Apply the event.
    /// 4. Advance the checkpoint to `sequence_number`.
    pub async fn process(
        &self,
        envelope: &EventEnvelope,
        sequence_number: u64,
    ) -> Result<(), ProjectionConsumerError> {
        for projection in &self.projections {
            let checkpoint = self
                .checkpoint_store
                .get_checkpoint(&projection.projection_id)
                .await
                .map_err(|e| ProjectionConsumerError::CheckpointStore(e.to_string()))?;

            if sequence_number <= checkpoint.as_u64() {
                tracing::debug!(
                    projection_id = %projection.projection_id,
                    sequence_number = sequence_number,
                    checkpoint = checkpoint.as_u64(),
                    "projection consumer: skipping already-processed sequence"
                );
                continue;
            }

            tracing::debug!(
                projection_id = %projection.projection_id,
                sequence_number = sequence_number,
                event_type = %envelope.event_type,
                aggregate_id = ?envelope.aggregate_id,
                aggregate_version = envelope.version.as_u64(),
                "projection consumer: applying event"
            );

            (projection.apply_fn)(&projection.projection_id, envelope).map_err(|message| {
                ProjectionConsumerError::ApplyFailed {
                    projection_id: projection.projection_id.clone(),
                    message,
                }
            })?;

            self.checkpoint_store
                .set_checkpoint(
                    &projection.projection_id,
                    Version::from_u64(sequence_number),
                )
                .await
                .map_err(|e| ProjectionConsumerError::CheckpointStore(e.to_string()))?;

            tracing::debug!(
                projection_id = %projection.projection_id,
                new_checkpoint = sequence_number,
                "projection consumer: checkpoint advanced"
            );
        }

        Ok(())
    }

    /// Access the underlying checkpoint store (for test assertions).
    pub fn checkpoint_store(&self) -> &CS {
        &self.checkpoint_store
    }

    /// Run the consumer loop. Receives events from the given
    /// [`ConsumerReceiver`](super::ConsumerReceiver), processes each one via
    /// [`Self::process`], commits offsets, and stops when `shutdown` fires.
    ///
    /// When `outbound_notify` is provided, the consumer wakes immediately on
    /// notification instead of waiting for the receiver's poll timeout. This
    /// reduces pipeline latency from ~100ms (poll timeout) to near-zero when
    /// the outbox processor notifies after publishing.
    ///
    /// On receive or commit errors the `on_error` callback is invoked and the
    /// loop sleeps briefly before retrying.
    pub async fn run<R, F>(
        self,
        receiver: R,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        outbound_notify: Option<Arc<Notify>>,
        on_error: F,
    ) where
        R: super::ConsumerReceiver,
        F: Fn(&ProjectionConsumerError) + Send + Sync,
    {
        loop {
            if *shutdown.borrow() {
                return;
            }

            let received = tokio::select! {
                r = receiver.receive() => r,
                _ = async {
                    match outbound_notify.as_ref() {
                        Some(notify) => notify.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    // Woken by outbound notify — immediately try to receive.
                    receiver.receive().await
                }
                _ = shutdown.changed() => return,
            };

            match received {
                Ok(Some(re)) => {
                    if let Err(e) = self.process(&re.envelope, re.sequence_number).await {
                        on_error(&e);
                    }
                    if let Err(commit_err) = receiver.commit().await {
                        tracing::warn!(error = %commit_err, "projection consumer: commit failed");
                    }
                }
                Ok(None) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "projection consumer: receive error");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};
    use crate::memory::InMemoryProjectionStore;
    use crate::AggregateId;
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    struct MockReceiver {
        events: Arc<Mutex<VecDeque<ReceivedEnvelope>>>,
        committed: Arc<AtomicU32>,
    }

    impl MockReceiver {
        fn new(events: Vec<ReceivedEnvelope>) -> Self {
            Self {
                events: Arc::new(Mutex::new(VecDeque::from(events))),
                committed: Arc::new(AtomicU32::new(0)),
            }
        }

        #[allow(dead_code)]
        fn committed_count(&self) -> u32 {
            self.committed.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ConsumerReceiver for MockReceiver {
        async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
            let mut events = self.events.lock().unwrap();
            Ok(events.pop_front())
        }
        async fn commit(&self) -> Result<(), ConsumerReceiverError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn make_event_for(aggregate_id: &AggregateId, version: u64) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::from_u64(version),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    fn make_event(version: u64) -> EventEnvelope {
        make_event_for(&AggregateId::new(), version)
    }

    fn counting_projection(id: &str, counter: Arc<AtomicU32>) -> RegisteredProjection {
        RegisteredProjection {
            projection_id: id.to_owned(),
            apply_fn: Box::new(move |_proj_id, _envelope| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        }
    }

    #[tokio::test]
    async fn process_applies_event_to_all_projections() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        consumer.register(counting_projection("proj-a", Arc::clone(&counter_a)));
        consumer.register(counting_projection("proj-b", Arc::clone(&counter_b)));

        let event = make_event(1);
        consumer.process(&event, 1).await.unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn process_skips_stale_sequence() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Sequence 3 is older than checkpoint 5 — should be skipped
        let event = make_event(10);
        consumer.process(&event, 3).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn process_skips_same_sequence_as_checkpoint() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Sequence exactly 5 — should be skipped (already processed)
        let event = make_event(1);
        consumer.process(&event, 5).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn process_advances_checkpoint() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1), 1).await.unwrap();
        consumer.process(&make_event(2), 2).await.unwrap();
        consumer.process(&make_event(3), 3).await.unwrap();

        let checkpoint = consumer
            .checkpoint_store()
            .get_checkpoint("proj-a")
            .await
            .unwrap();
        assert_eq!(checkpoint.as_u64(), 3);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn process_propagates_apply_error() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        consumer.register(RegisteredProjection {
            projection_id: "failing-proj".to_owned(),
            apply_fn: Box::new(|_proj_id, _envelope| Err("something went wrong".to_owned())),
        });

        let result = consumer.process(&make_event(1), 1).await;
        assert!(matches!(
            result,
            Err(ProjectionConsumerError::ApplyFailed { .. })
        ));

        // Checkpoint should NOT advance on error
        let checkpoint = consumer
            .checkpoint_store()
            .get_checkpoint("failing-proj")
            .await
            .unwrap();
        assert_eq!(checkpoint.as_u64(), 0);
    }

    #[tokio::test]
    async fn projections_have_independent_checkpoints() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(3))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);

        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        consumer.register(counting_projection("proj-a", Arc::clone(&counter_a)));
        consumer.register(counting_projection("proj-b", Arc::clone(&counter_b)));

        // Sequence 2: proj-a skips (checkpoint 3), proj-b applies (checkpoint 0)
        consumer.process(&make_event(1), 2).await.unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 0);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn idempotent_replay_skips_duplicate_sequence() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1), 1).await.unwrap();
        consumer.process(&make_event(1), 1).await.unwrap(); // duplicate sequence — should skip
        consumer.process(&make_event(2), 2).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    /// Regression test for issue #117: cross-aggregate events must not be
    /// silently skipped. When the checkpoint was based on per-aggregate version,
    /// processing event A@v100 would advance the checkpoint to 100, causing
    /// event B@v5 to be dropped (5 <= 100). With global sequence numbers, each
    /// event gets a unique, monotonically increasing sequence regardless of
    /// which aggregate produced it.
    #[tokio::test]
    async fn cross_aggregate_events_not_skipped() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("inventory", Arc::clone(&counter)));

        let agg_a = AggregateId::new();
        let agg_b = AggregateId::new();

        // Aggregate A event at per-aggregate version 100, global sequence 1
        let event_a = make_event_for(&agg_a, 100);
        consumer.process(&event_a, 1).await.unwrap();

        // Aggregate B event at per-aggregate version 5, global sequence 2
        // Before the fix this would be skipped because 5 <= 100.
        let event_b = make_event_for(&agg_b, 5);
        consumer.process(&event_b, 2).await.unwrap();

        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "both events must be applied"
        );

        let checkpoint = consumer
            .checkpoint_store()
            .get_checkpoint("inventory")
            .await
            .unwrap();
        assert_eq!(
            checkpoint.as_u64(),
            2,
            "checkpoint must track global sequence"
        );
    }

    /// Verify that a high per-aggregate version with a low sequence number is
    /// correctly skipped when the checkpoint has already advanced past it.
    #[tokio::test]
    async fn stale_sequence_with_high_aggregate_version_is_skipped() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(10))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Per-aggregate version is 999, but sequence is 5 — already processed
        let event = make_event(999);
        consumer.process(&event, 5).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_processes_and_commits() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        let events = vec![ReceivedEnvelope {
            envelope: make_event(1),
            sequence_number: 1,
        }];

        let receiver = MockReceiver::new(events);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let counter_clone = Arc::clone(&counter);
        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, None, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        assert!(counter_clone.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn run_stops_on_immediate_shutdown() {
        let store = InMemoryProjectionStore::new();
        let consumer = ProjectionConsumer::new(store);
        let receiver = MockReceiver::new(vec![]);
        let (_tx, shutdown_rx) = tokio::sync::watch::channel(true);

        consumer.run(receiver, shutdown_rx, None, |_| {}).await;
    }

    #[tokio::test]
    async fn run_handles_receive_error() {
        let store = InMemoryProjectionStore::new();
        let consumer = ProjectionConsumer::new(store);

        struct FailingReceiver;
        #[async_trait]
        impl ConsumerReceiver for FailingReceiver {
            async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
                Err(ConsumerReceiverError::Receive("fail".into()))
            }
            async fn commit(&self) -> Result<(), ConsumerReceiverError> {
                Ok(())
            }
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer
                .run(FailingReceiver, shutdown_rx, None, |_| {})
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn run_commit_error_does_not_stop_loop() {
        struct FailingCommitReceiver {
            events: Arc<Mutex<VecDeque<ReceivedEnvelope>>>,
        }

        #[async_trait]
        impl ConsumerReceiver for FailingCommitReceiver {
            async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
                let mut events = self.events.lock().unwrap();
                Ok(events.pop_front())
            }
            async fn commit(&self) -> Result<(), ConsumerReceiverError> {
                Err(ConsumerReceiverError::Commit("commit failed".into()))
            }
        }

        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        let receiver = FailingCommitReceiver {
            events: Arc::new(Mutex::new(VecDeque::from(vec![ReceivedEnvelope {
                envelope: make_event(1),
                sequence_number: 1,
            }]))),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let counter_clone = Arc::clone(&counter);
        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, None, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        // The event should have been processed despite commit error
        assert!(counter_clone.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn run_calls_on_error_for_apply_failure() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        consumer.register(RegisteredProjection {
            projection_id: "failing-proj".to_owned(),
            apply_fn: Box::new(|_proj_id, _envelope| Err("apply broke".to_owned())),
        });

        let receiver = MockReceiver::new(vec![ReceivedEnvelope {
            envelope: make_event(1),
            sequence_number: 1,
        }]);

        let error_count = Arc::new(AtomicU32::new(0));
        let error_count_clone = error_count.clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer
                .run(receiver, shutdown_rx, None, move |_| {
                    error_count_clone.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        assert!(error_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn error_display_messages() {
        let err = ProjectionConsumerError::CheckpointStore("db error".into());
        assert!(err.to_string().contains("checkpoint"));
        assert!(err.to_string().contains("db error"));

        let err = ProjectionConsumerError::ApplyFailed {
            projection_id: "inventory".into(),
            message: "oops".into(),
        };
        assert!(err.to_string().contains("inventory"));
        assert!(err.to_string().contains("oops"));
    }
}
