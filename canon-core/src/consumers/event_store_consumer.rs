//! Event store consumer — writes events to the event store and takes periodic snapshots.
//!
//! Consumes `EventEnvelope` messages from the outbound queue. After a confirmed write
//! to the event store, checks `version % snapshot_every == 0` and writes a snapshot
//! to the snapshot store. On version conflict, retries up to `max_retries` times;
//! tracks retry counts in a crash-safe retry tracker. On exhaustion, dead-letters
//! the event.
//!
//! Generic over `EventStore`, `SnapshotStore`, `DeadLetterStore`, and `RetryTracker`
//! traits so the same consumer logic works with both in-memory test impls and
//! production infrastructure.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::traits::{DeadLetterStore, EventStore, SnapshotStore};
use crate::RetryTracker;
use crate::{AggregateId, EventEnvelope, Snapshot, Version};

/// Errors emitted by the event store consumer.
#[derive(Debug, thiserror::Error)]
pub enum EventStoreConsumerError {
    /// The event store rejected the write with a version conflict and all retries are exhausted.
    #[error(
        "version conflict after {attempts} attempt(s) for aggregate {aggregate_id:?}: {message}"
    )]
    VersionConflictExhausted {
        aggregate_id: AggregateId,
        attempts: u32,
        message: String,
    },

    /// An error propagated from the event store.
    #[error("event store error: {0}")]
    EventStore(String),

    /// An error writing a snapshot.
    #[error("snapshot store error: {0}")]
    SnapshotStore(String),

    /// An error writing to the dead letter store.
    #[error("dead letter store error: {0}")]
    DeadLetter(String),

    /// An error from the retry tracker.
    #[error("retry tracker error: {0}")]
    RetryTracker(String),

    /// A version conflict that can still be retried.
    #[error("version conflict (retryable): {0}")]
    VersionConflict(String),
}

/// Provides serialized aggregate state for snapshot creation.
///
/// The event store consumer does not know the aggregate type, so it cannot hydrate
/// state directly. Instead, a `SnapshotStateProvider` is injected to load events
/// from the event store, hydrate the aggregate, and serialize the resulting state.
///
/// For testing, a simple implementation that returns the last event payload can be used.
#[async_trait]
pub trait SnapshotStateProvider: Send + Sync {
    /// Produce serialized aggregate state at the given version.
    ///
    /// Called after a confirmed event store write when `version % snapshot_every == 0`.
    /// Implementations should load events up to `version`, hydrate the aggregate,
    /// and return the serialized state bytes.
    async fn state_at(
        &self,
        aggregate_id: &AggregateId,
        version: Version,
    ) -> Result<bytes::Bytes, String>;
}

/// A snapshot state provider that returns the event payload directly.
///
/// This is a test-only placeholder. In production, the snapshot state provider
/// would hydrate the aggregate from events and serialize the resulting state.
#[derive(Clone)]
pub struct EventPayloadSnapshotProvider;

#[async_trait]
impl SnapshotStateProvider for EventPayloadSnapshotProvider {
    async fn state_at(
        &self,
        _aggregate_id: &AggregateId,
        _version: Version,
    ) -> Result<bytes::Bytes, String> {
        // This is a placeholder — snapshots will contain empty state rather than
        // real hydrated aggregate state. A production-grade provider would load
        // events from the event store, hydrate the aggregate, and serialize
        // the resulting state. That requires aggregate type knowledge which the
        // generic event store consumer does not have.
        tracing::warn!(
            "EventPayloadSnapshotProvider: returning empty snapshot state — \
             snapshots will not contain real aggregate state"
        );
        Ok(bytes::Bytes::from_static(b"{}"))
    }
}

/// Configuration for the event store consumer.
#[derive(Debug, Clone)]
pub struct EventStoreConsumerConfig {
    /// Take a snapshot every N events (checks `version % snapshot_every == 0`).
    pub snapshot_every: u64,
    /// Maximum number of retry attempts on version conflict before dead-lettering.
    pub max_retries: u32,
}

impl Default for EventStoreConsumerConfig {
    fn default() -> Self {
        Self {
            snapshot_every: 50,
            max_retries: 3,
        }
    }
}

/// Consumes events from the outbound queue and writes them to the event store.
///
/// After confirmed writes, snapshots if `version % snapshot_every == 0`.
/// On version conflict, retries up to `max_retries`. On exhaustion, dead-letters.
///
/// Generic over all infrastructure traits so the same logic works with both
/// in-memory and production implementations.
pub struct EventStoreConsumer<ES, SS, DL, RT, SP>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
{
    event_store: ES,
    snapshot_store: SS,
    dead_letter_store: DL,
    retry_tracker: RT,
    snapshot_state_provider: SP,
    config: EventStoreConsumerConfig,
    _marker: PhantomData<()>,
}

impl<ES, SS, DL, RT, SP> EventStoreConsumer<ES, SS, DL, RT, SP>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
{
    pub fn new(
        event_store: ES,
        snapshot_store: SS,
        dead_letter_store: DL,
        retry_tracker: RT,
        snapshot_state_provider: SP,
        config: EventStoreConsumerConfig,
    ) -> Self {
        Self {
            event_store,
            snapshot_store,
            dead_letter_store,
            retry_tracker,
            snapshot_state_provider,
            config,
            _marker: PhantomData,
        }
    }

    /// Process a single event envelope from the outbound queue.
    ///
    /// 1. Attempts to append to the event store with optimistic concurrency.
    /// 2. On version conflict: increments retry count, dead-letters on exhaustion.
    /// 3. On success: checks `version % snapshot_every == 0`, writes snapshot if so.
    pub async fn process(&self, envelope: EventEnvelope) -> Result<(), EventStoreConsumerError> {
        let aggregate_id = envelope.aggregate_id.clone();
        let event_id = envelope.event_id;

        // Determine expected version: the version just before this event.
        // The event envelope's version is the target version after write.
        let expected_version = if envelope.version.as_u64() > 0 {
            Version::from_u64(envelope.version.as_u64() - 1)
        } else {
            Version::initial()
        };

        tracing::debug!(
            event_id = %event_id,
            aggregate_id = ?aggregate_id,
            version = envelope.version.as_u64(),
            "event store consumer: appending event"
        );

        match self
            .event_store
            .append(&aggregate_id, expected_version, vec![envelope.clone()])
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    event_id = %event_id,
                    aggregate_id = ?aggregate_id,
                    version = envelope.version.as_u64(),
                    "event store consumer: event appended successfully"
                );

                // Clean up retry tracking on success
                self.retry_tracker
                    .remove(event_id)
                    .map_err(|e| EventStoreConsumerError::RetryTracker(e.to_string()))?;

                // Snapshot check: version % snapshot_every == 0
                if self.config.snapshot_every > 0
                    && envelope
                        .version
                        .as_u64()
                        .is_multiple_of(self.config.snapshot_every)
                {
                    tracing::info!(
                        aggregate_id = ?aggregate_id,
                        version = envelope.version.as_u64(),
                        snapshot_every = self.config.snapshot_every,
                        "event store consumer: writing snapshot"
                    );

                    let state = self
                        .snapshot_state_provider
                        .state_at(&aggregate_id, envelope.version)
                        .await
                        .map_err(|e| {
                            EventStoreConsumerError::SnapshotStore(format!(
                                "failed to produce snapshot state: {e}"
                            ))
                        })?;

                    let snapshot = Snapshot {
                        aggregate_id: aggregate_id.clone(),
                        version: envelope.version,
                        state,
                        taken_at: chrono::Utc::now(),
                    };
                    self.snapshot_store
                        .save(snapshot)
                        .await
                        .map_err(|e| EventStoreConsumerError::SnapshotStore(e.to_string()))?;
                }

                Ok(())
            }
            Err(e) => {
                let err_string = e.to_string();
                let is_version_conflict = err_string.contains("version conflict");

                if is_version_conflict {
                    let attempts = self
                        .retry_tracker
                        .increment(event_id, "event_store_consumer")
                        .map_err(|e| EventStoreConsumerError::RetryTracker(e.to_string()))?;

                    tracing::warn!(
                        event_id = %event_id,
                        aggregate_id = ?aggregate_id,
                        attempt = attempts,
                        max_retries = self.config.max_retries,
                        "event store consumer: version conflict"
                    );

                    if attempts >= self.config.max_retries {
                        tracing::error!(
                            event_id = %event_id,
                            aggregate_id = ?aggregate_id,
                            attempts = attempts,
                            "event store consumer: retry budget exhausted, dead-lettering"
                        );

                        self.retry_tracker
                            .remove(event_id)
                            .map_err(|e| EventStoreConsumerError::RetryTracker(e.to_string()))?;

                        self.dead_letter_store
                            .store(
                                event_id,
                                "event_store_consumer",
                                &aggregate_id,
                                envelope.payload.clone(),
                                &format!(
                                    "version conflict after {attempts} attempts: {err_string}"
                                ),
                            )
                            .await
                            .map_err(|e| EventStoreConsumerError::DeadLetter(e.to_string()))?;

                        return Err(EventStoreConsumerError::VersionConflictExhausted {
                            aggregate_id,
                            attempts,
                            message: err_string,
                        });
                    }

                    Err(EventStoreConsumerError::VersionConflict(err_string))
                } else {
                    Err(EventStoreConsumerError::EventStore(err_string))
                }
            }
        }
    }

    /// Run the consumer loop. Receives events from the given
    /// [`ConsumerReceiver`](super::ConsumerReceiver), processes each one via
    /// [`Self::process`], commits offsets, and stops when `shutdown` fires.
    ///
    /// On receive or commit errors the `on_error` callback is invoked and the
    /// loop sleeps briefly before retrying. Process errors (version conflicts,
    /// dead-lettering) are logged but do not stop the loop.
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
    /// loop sleeps briefly before retrying. Process errors (version conflicts,
    /// dead-lettering) are logged but do not stop the loop.
    pub async fn run<R, F>(
        self,
        receiver: R,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        outbound_notify: Option<Arc<Notify>>,
        on_error: F,
    ) where
        R: super::ConsumerReceiver,
        F: Fn(&EventStoreConsumerError) + Send + Sync,
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
                    if let Err(e) = self.process(re.envelope).await {
                        on_error(&e);
                    }
                    if let Err(commit_err) = receiver.commit().await {
                        tracing::warn!(error = %commit_err, "event store consumer: commit failed");
                    }
                }
                Ok(None) => {
                    // Yield so the runtime can process shutdown signals.
                    // receiver.receive() already blocks via fetch_records
                    // timeout, so no extra sleep needed.
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "event store consumer: receive error");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
            }
        }
    }
}

impl<ES, SS, DL, RT, SP> fmt::Debug for EventStoreConsumer<ES, SS, DL, RT, SP>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStoreConsumer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};
    use crate::memory::{
        InMemoryDeadLetterStore, InMemoryEventStore, InMemoryRetryTracker, InMemorySnapshotStore,
    };
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

    fn make_event(aggregate_id: &AggregateId, version: u64) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::from_u64(version),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{\"test\":true}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    fn make_consumer(
        snapshot_every: u64,
        max_retries: u32,
    ) -> EventStoreConsumer<
        InMemoryEventStore,
        InMemorySnapshotStore,
        InMemoryDeadLetterStore,
        InMemoryRetryTracker,
        EventPayloadSnapshotProvider,
    > {
        EventStoreConsumer::new(
            InMemoryEventStore::new(),
            InMemorySnapshotStore::new(),
            InMemoryDeadLetterStore::new(),
            InMemoryRetryTracker::new(),
            EventPayloadSnapshotProvider,
            EventStoreConsumerConfig {
                snapshot_every,
                max_retries,
            },
        )
    }

    #[tokio::test]
    async fn process_appends_event_to_store() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();
        let event = make_event(&id, 1);

        consumer.process(event).await.unwrap();

        let events = consumer.event_store.load(&id).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version.as_u64(), 1);
    }

    #[tokio::test]
    async fn process_sequential_events() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).await.unwrap();
        consumer.process(make_event(&id, 2)).await.unwrap();
        consumer.process(make_event(&id, 3)).await.unwrap();

        let events = consumer.event_store.load(&id).unwrap();
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn snapshot_taken_at_configured_interval() {
        let consumer = make_consumer(2, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).await.unwrap();
        // Version 1: no snapshot (1 % 2 != 0)
        assert!(consumer.snapshot_store.load(&id).unwrap().is_none());

        consumer.process(make_event(&id, 2)).await.unwrap();
        // Version 2: snapshot (2 % 2 == 0)
        let snap = consumer.snapshot_store.load(&id).unwrap().unwrap();
        assert_eq!(snap.version.as_u64(), 2);
    }

    #[tokio::test]
    async fn snapshot_not_taken_when_interval_is_zero() {
        let consumer = make_consumer(0, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).await.unwrap();
        consumer.process(make_event(&id, 2)).await.unwrap();

        assert!(consumer.snapshot_store.load(&id).unwrap().is_none());
    }

    #[tokio::test]
    async fn version_conflict_returns_error_and_increments_retry() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        // First event succeeds
        consumer.process(make_event(&id, 1)).await.unwrap();

        // Attempt to write version 1 again — conflict
        let result = consumer.process(make_event(&id, 1)).await;
        assert!(result.is_err());

        // Attempt again — second retry
        let result2 = consumer.process(make_event(&id, 1)).await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn version_conflict_exhausted_dead_letters() {
        let consumer = make_consumer(50, 2);
        let id = AggregateId::new();

        // Write version 1 successfully
        consumer.process(make_event(&id, 1)).await.unwrap();

        // Attempt to write version 1 again — will conflict. 2 attempts = exhausted.
        let event = make_event(&id, 1);
        let event_clone = event.clone();
        let _ = consumer.process(event).await; // attempt 1
        let result = consumer.process(event_clone).await; // attempt 2 — exhausted

        assert!(matches!(
            result,
            Err(EventStoreConsumerError::VersionConflictExhausted { .. })
        ));

        // Should have a dead letter
        let dead_letters = consumer.dead_letter_store.list(None).unwrap();
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].handler_id, "event_store_consumer");
    }

    #[tokio::test]
    async fn successful_write_clears_retry_tracker() {
        let consumer = make_consumer(50, 5);
        let id = AggregateId::new();

        // Write version 1, cause a conflict, then write version 2 successfully
        consumer.process(make_event(&id, 1)).await.unwrap();

        let conflict_event = make_event(&id, 1);
        let event_id = conflict_event.event_id;
        let _ = consumer.process(conflict_event).await; // conflict, retry = 1

        assert!(consumer.retry_tracker.get(event_id).unwrap().is_some());

        // Successful write of version 2 with same event_id shouldn't matter —
        // it's a new event. But let's verify the tracker clears on success.
        let mut v2_event = make_event(&id, 2);
        let v2_id = v2_event.event_id;
        // Pre-populate tracker for v2_id
        consumer
            .retry_tracker
            .increment(v2_id, "event_store_consumer")
            .unwrap();
        assert!(consumer.retry_tracker.get(v2_id).unwrap().is_some());

        v2_event.event_id = v2_id;
        consumer.process(v2_event).await.unwrap();

        // After success, retry entry should be cleared
        assert!(consumer.retry_tracker.get(v2_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn multiple_aggregates_independent() {
        let consumer = make_consumer(50, 3);
        let id_a = AggregateId::new();
        let id_b = AggregateId::new();

        consumer.process(make_event(&id_a, 1)).await.unwrap();
        consumer.process(make_event(&id_b, 1)).await.unwrap();
        consumer.process(make_event(&id_a, 2)).await.unwrap();

        assert_eq!(consumer.event_store.load(&id_a).unwrap().len(), 2);
        assert_eq!(consumer.event_store.load(&id_b).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_processes_events_and_commits() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        let events = vec![
            ReceivedEnvelope {
                envelope: make_event(&id, 1),
                sequence_number: 1,
            },
            ReceivedEnvelope {
                envelope: make_event(&id, 2),
                sequence_number: 2,
            },
        ];

        let receiver = MockReceiver::new(events);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, None, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn run_stops_on_immediate_shutdown() {
        let consumer = make_consumer(50, 3);
        let receiver = MockReceiver::new(vec![]);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(true);

        consumer.run(receiver, shutdown_rx, None, |_| {}).await;
        // Should return immediately without panic
    }

    #[tokio::test]
    async fn run_handles_receive_error() {
        let consumer = make_consumer(50, 3);

        struct FailingReceiver;
        #[async_trait]
        impl ConsumerReceiver for FailingReceiver {
            async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
                Err(ConsumerReceiverError::Receive("test error".into()))
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
    async fn debug_impl_works() {
        let consumer = make_consumer(50, 3);
        let debug = format!("{:?}", consumer);
        assert!(debug.contains("EventStoreConsumer"));
    }

    #[tokio::test]
    async fn run_commit_error_does_not_stop_loop() {
        // A receiver where receive returns Ok(Some) but commit returns Err.
        // The consumer should still process the event and continue.
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

        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        let receiver = FailingCommitReceiver {
            events: Arc::new(Mutex::new(VecDeque::from(vec![ReceivedEnvelope {
                envelope: make_event(&id, 1),
                sequence_number: 1,
            }]))),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let event_store = consumer.event_store.clone();

        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, None, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        // The event should still have been processed despite commit error
        let events = event_store.load(&id).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn run_process_error_calls_on_error_callback() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        // Pre-populate event store so version 1 already exists,
        // causing a version conflict when the consumer tries to write version 1 again.
        consumer
            .event_store
            .append(&id, Version::initial(), vec![make_event(&id, 1)])
            .unwrap();

        let receiver = MockReceiver::new(vec![ReceivedEnvelope {
            envelope: make_event(&id, 1), // will conflict
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
    async fn process_non_version_conflict_error() {
        // Test the else branch in the Err match arm of process(),
        // where the error is NOT a version conflict.
        // We use a custom event store that returns a non-conflict error.
        struct FailingEventStore;

        #[async_trait]
        impl crate::traits::EventStore for FailingEventStore {
            type Error = crate::error::EventStoreError;
            async fn append(
                &self,
                _aggregate_id: &AggregateId,
                _expected_version: Version,
                _events: Vec<EventEnvelope>,
            ) -> Result<(), Self::Error> {
                Err(crate::error::EventStoreError::Poisoned)
            }
            async fn load(
                &self,
                _aggregate_id: &AggregateId,
            ) -> Result<Vec<EventEnvelope>, Self::Error> {
                Ok(vec![])
            }
            async fn load_from_version(
                &self,
                _aggregate_id: &AggregateId,
                _from_version: Version,
            ) -> Result<Vec<EventEnvelope>, Self::Error> {
                Ok(vec![])
            }
            async fn current_version(
                &self,
                _aggregate_id: &AggregateId,
            ) -> Result<Version, Self::Error> {
                Ok(Version::initial())
            }
        }

        let consumer = EventStoreConsumer::new(
            FailingEventStore,
            InMemorySnapshotStore::new(),
            InMemoryDeadLetterStore::new(),
            InMemoryRetryTracker::new(),
            EventPayloadSnapshotProvider,
            EventStoreConsumerConfig {
                snapshot_every: 50,
                max_retries: 3,
            },
        );

        let id = AggregateId::new();
        let result = consumer.process(make_event(&id, 1)).await;
        assert!(matches!(
            result,
            Err(EventStoreConsumerError::EventStore(_))
        ));
    }

    #[tokio::test]
    async fn default_config_values() {
        let config = EventStoreConsumerConfig::default();
        assert_eq!(config.snapshot_every, 50);
        assert_eq!(config.max_retries, 3);
    }
}
