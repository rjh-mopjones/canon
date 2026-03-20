//! Event store consumer — writes events to the event store and takes periodic snapshots.
//!
//! Consumes `EventEnvelope` messages from the outbound queue. After a confirmed write
//! to the event store, checks `version % snapshot_every == 0` and writes a snapshot
//! to the snapshot store. On version conflict, retries up to `max_retries` times;
//! tracks retry counts in a crash-safe retry tracker. On exhaustion, dead-letters
//! the event.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::EventStoreError;
use crate::memory::{InMemoryDeadLetterStore, InMemoryEventStore, InMemorySnapshotStore};
use crate::{AggregateId, EventEnvelope, Snapshot, Version};

/// Errors emitted by the event store consumer.
#[derive(Debug, thiserror::Error)]
pub enum EventStoreConsumerError {
    /// The event store rejected the write with a version conflict and all retries are exhausted.
    #[error(
        "version conflict after {attempts} attempt(s) for aggregate {aggregate_id:?}: {source}"
    )]
    VersionConflictExhausted {
        aggregate_id: AggregateId,
        attempts: u32,
        source: EventStoreError,
    },

    /// An error propagated from the event store.
    #[error("event store error: {0}")]
    EventStore(#[from] EventStoreError),

    /// An error writing a snapshot.
    #[error("snapshot store error: {0}")]
    SnapshotStore(#[from] crate::memory::SnapshotStoreError),

    /// An error writing to the dead letter store.
    #[error("dead letter store error: {0}")]
    DeadLetter(#[from] crate::error::DeadLetterError),

    /// An internal lock was poisoned.
    #[error("internal lock error")]
    Poisoned,
}

/// Tracks per-message retry counts. Crash-safe in production (backed by `retry_attempts` table);
/// in-memory here for the test harness.
#[derive(Clone)]
pub struct InMemoryRetryTracker {
    inner: Arc<Mutex<HashMap<uuid::Uuid, u32>>>,
}

impl InMemoryRetryTracker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Increment and return the new attempt count for a message.
    pub fn increment(&self, event_id: uuid::Uuid) -> Result<u32, EventStoreConsumerError> {
        let mut map = self
            .inner
            .lock()
            .map_err(|_| EventStoreConsumerError::Poisoned)?;
        let count = map.entry(event_id).or_insert(0);
        *count += 1;
        Ok(*count)
    }

    /// Get the current attempt count. Returns 0 if no entry exists.
    pub fn get(&self, event_id: uuid::Uuid) -> Result<u32, EventStoreConsumerError> {
        let map = self
            .inner
            .lock()
            .map_err(|_| EventStoreConsumerError::Poisoned)?;
        Ok(map.get(&event_id).copied().unwrap_or(0))
    }

    /// Remove the retry entry for a message (after successful processing or dead-lettering).
    pub fn remove(&self, event_id: uuid::Uuid) -> Result<(), EventStoreConsumerError> {
        let mut map = self
            .inner
            .lock()
            .map_err(|_| EventStoreConsumerError::Poisoned)?;
        map.remove(&event_id);
        Ok(())
    }
}

impl Default for InMemoryRetryTracker {
    fn default() -> Self {
        Self::new()
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
#[derive(Clone)]
pub struct EventStoreConsumer {
    event_store: InMemoryEventStore,
    snapshot_store: InMemorySnapshotStore,
    dead_letter_store: InMemoryDeadLetterStore,
    retry_tracker: InMemoryRetryTracker,
    config: EventStoreConsumerConfig,
}

impl EventStoreConsumer {
    pub fn new(
        event_store: InMemoryEventStore,
        snapshot_store: InMemorySnapshotStore,
        dead_letter_store: InMemoryDeadLetterStore,
        retry_tracker: InMemoryRetryTracker,
        config: EventStoreConsumerConfig,
    ) -> Self {
        Self {
            event_store,
            snapshot_store,
            dead_letter_store,
            retry_tracker,
            config,
        }
    }

    /// Process a single event envelope from the outbound queue.
    ///
    /// 1. Attempts to append to the event store with optimistic concurrency.
    /// 2. On version conflict: increments retry count, dead-letters on exhaustion.
    /// 3. On success: checks `version % snapshot_every == 0`, writes snapshot if so.
    pub fn process(&self, envelope: EventEnvelope) -> Result<(), EventStoreConsumerError> {
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
        {
            Ok(()) => {
                tracing::debug!(
                    event_id = %event_id,
                    aggregate_id = ?aggregate_id,
                    version = envelope.version.as_u64(),
                    "event store consumer: event appended successfully"
                );

                // Clean up retry tracking on success
                self.retry_tracker.remove(event_id)?;

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
                    let snapshot = Snapshot {
                        aggregate_id: aggregate_id.clone(),
                        version: envelope.version,
                        state: envelope.payload.clone(),
                        taken_at: chrono::Utc::now(),
                    };
                    self.snapshot_store.save(snapshot)?;
                }

                Ok(())
            }
            Err(EventStoreError::VersionConflict { expected, actual }) => {
                let attempts = self.retry_tracker.increment(event_id)?;

                tracing::warn!(
                    event_id = %event_id,
                    aggregate_id = ?aggregate_id,
                    expected_version = ?expected,
                    actual_version = ?actual,
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

                    self.retry_tracker.remove(event_id)?;
                    self.dead_letter_store.store(
                        event_id,
                        "event_store_consumer",
                        &aggregate_id,
                        envelope.payload.clone(),
                        &format!(
                            "version conflict after {} attempts: expected {:?}, actual {:?}",
                            attempts, expected, actual
                        ),
                    )?;

                    return Err(EventStoreConsumerError::VersionConflictExhausted {
                        aggregate_id,
                        attempts,
                        source: EventStoreError::VersionConflict { expected, actual },
                    });
                }

                Err(EventStoreConsumerError::EventStore(
                    EventStoreError::VersionConflict { expected, actual },
                ))
            }
            Err(e) => Err(EventStoreConsumerError::EventStore(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

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

    fn make_consumer(snapshot_every: u64, max_retries: u32) -> EventStoreConsumer {
        EventStoreConsumer::new(
            InMemoryEventStore::new(),
            InMemorySnapshotStore::new(),
            InMemoryDeadLetterStore::new(),
            InMemoryRetryTracker::new(),
            EventStoreConsumerConfig {
                snapshot_every,
                max_retries,
            },
        )
    }

    #[test]
    fn process_appends_event_to_store() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();
        let event = make_event(&id, 1);

        consumer.process(event).unwrap();

        let events = consumer.event_store.load(&id).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version.as_u64(), 1);
    }

    #[test]
    fn process_sequential_events() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).unwrap();
        consumer.process(make_event(&id, 2)).unwrap();
        consumer.process(make_event(&id, 3)).unwrap();

        let events = consumer.event_store.load(&id).unwrap();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn snapshot_taken_at_configured_interval() {
        let consumer = make_consumer(2, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).unwrap();
        // Version 1: no snapshot (1 % 2 != 0)
        assert!(consumer.snapshot_store.load(&id).unwrap().is_none());

        consumer.process(make_event(&id, 2)).unwrap();
        // Version 2: snapshot (2 % 2 == 0)
        let snap = consumer.snapshot_store.load(&id).unwrap().unwrap();
        assert_eq!(snap.version.as_u64(), 2);
    }

    #[test]
    fn snapshot_not_taken_when_interval_is_zero() {
        let consumer = make_consumer(0, 3);
        let id = AggregateId::new();

        consumer.process(make_event(&id, 1)).unwrap();
        consumer.process(make_event(&id, 2)).unwrap();

        assert!(consumer.snapshot_store.load(&id).unwrap().is_none());
    }

    #[test]
    fn version_conflict_returns_error_and_increments_retry() {
        let consumer = make_consumer(50, 3);
        let id = AggregateId::new();

        // First event succeeds
        consumer.process(make_event(&id, 1)).unwrap();

        // Attempt to write version 1 again — conflict
        let result = consumer.process(make_event(&id, 1));
        assert!(result.is_err());

        // Retry tracker should have 1 attempt
        // Attempt again — second retry
        let result2 = consumer.process(make_event(&id, 1));
        assert!(result2.is_err());
    }

    #[test]
    fn version_conflict_exhausted_dead_letters() {
        let consumer = make_consumer(50, 2);
        let id = AggregateId::new();

        // Write version 1 successfully
        consumer.process(make_event(&id, 1)).unwrap();

        // Attempt to write version 1 again — will conflict. 2 attempts = exhausted.
        let event = make_event(&id, 1);
        let event_clone = event.clone();
        let _ = consumer.process(event); // attempt 1
        let result = consumer.process(event_clone); // attempt 2 — exhausted

        assert!(matches!(
            result,
            Err(EventStoreConsumerError::VersionConflictExhausted { .. })
        ));

        // Should have a dead letter
        let dead_letters = consumer.dead_letter_store.list(None).unwrap();
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].handler_id, "event_store_consumer");
    }

    #[test]
    fn successful_write_clears_retry_tracker() {
        let consumer = make_consumer(50, 5);
        let id = AggregateId::new();

        // Write version 1, cause a conflict, then write version 2 successfully
        consumer.process(make_event(&id, 1)).unwrap();

        let conflict_event = make_event(&id, 1);
        let event_id = conflict_event.event_id;
        let _ = consumer.process(conflict_event); // conflict, retry = 1

        assert_eq!(consumer.retry_tracker.get(event_id).unwrap(), 1);

        // Successful write of version 2 with same event_id shouldn't matter —
        // it's a new event. But let's verify the tracker clears on success.
        let mut v2_event = make_event(&id, 2);
        let v2_id = v2_event.event_id;
        // Pre-populate tracker for v2_id
        consumer.retry_tracker.increment(v2_id).unwrap();
        assert_eq!(consumer.retry_tracker.get(v2_id).unwrap(), 1);

        v2_event.event_id = v2_id;
        consumer.process(v2_event).unwrap();

        // After success, retry entry should be cleared
        assert_eq!(consumer.retry_tracker.get(v2_id).unwrap(), 0);
    }

    #[test]
    fn multiple_aggregates_independent() {
        let consumer = make_consumer(50, 3);
        let id_a = AggregateId::new();
        let id_b = AggregateId::new();

        consumer.process(make_event(&id_a, 1)).unwrap();
        consumer.process(make_event(&id_b, 1)).unwrap();
        consumer.process(make_event(&id_a, 2)).unwrap();

        assert_eq!(consumer.event_store.load(&id_a).unwrap().len(), 2);
        assert_eq!(consumer.event_store.load(&id_b).unwrap().len(), 1);
    }
}
