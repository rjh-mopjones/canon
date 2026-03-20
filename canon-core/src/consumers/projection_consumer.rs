//! Projection consumer — applies events to projection read models.
//!
//! Consumes `EventEnvelope` messages from the outbound queue and applies them
//! to registered projection implementations. Tracks `last_version` checkpoint
//! per projection for idempotent replay. Each projection runs in its own tokio
//! task. While `rebuilding == true`, read endpoints fall back to read-through.

use crate::memory::{InMemoryProjectionStore, ProjectionStoreError};
use crate::{EventEnvelope, Version};

/// Errors emitted by the projection consumer.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionConsumerError {
    /// Failed to update or read projection checkpoint.
    #[error("projection store error: {0}")]
    ProjectionStore(#[from] ProjectionStoreError),

    /// A projection apply function returned an error.
    #[error("projection apply error for '{projection_id}': {message}")]
    ApplyFailed {
        projection_id: String,
        message: String,
    },

    /// The event version is not newer than the checkpoint — skip (not a fatal error).
    #[error("event version {event_version} not newer than checkpoint {checkpoint} for '{projection_id}'")]
    StaleEvent {
        projection_id: String,
        event_version: u64,
        checkpoint: u64,
    },
}

/// Type-erased projection apply function.
/// Takes `(projection_id, event_envelope, projection_store)` and applies the event.
pub type ProjectionApplyFn =
    Box<dyn Fn(&str, &EventEnvelope, &InMemoryProjectionStore) -> Result<(), String> + Send + Sync>;

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
pub struct ProjectionConsumer {
    projections: Vec<RegisteredProjection>,
    projection_store: InMemoryProjectionStore,
}

impl ProjectionConsumer {
    pub fn new(projection_store: InMemoryProjectionStore) -> Self {
        Self {
            projections: Vec::new(),
            projection_store,
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
    /// For each projection:
    /// 1. Read the checkpoint (`last_version`).
    /// 2. Skip if the event version is not newer than the checkpoint.
    /// 3. Apply the event.
    /// 4. Update the checkpoint to the event's version.
    pub fn process(&self, envelope: &EventEnvelope) -> Result<(), ProjectionConsumerError> {
        let event_version = envelope.version.as_u64();

        for projection in &self.projections {
            let checkpoint = self
                .projection_store
                .get_checkpoint(&projection.projection_id)?;

            if event_version <= checkpoint.as_u64() {
                tracing::debug!(
                    projection_id = %projection.projection_id,
                    event_version = event_version,
                    checkpoint = checkpoint.as_u64(),
                    "projection consumer: skipping stale event"
                );
                continue;
            }

            tracing::debug!(
                projection_id = %projection.projection_id,
                event_version = event_version,
                event_type = %envelope.event_type,
                "projection consumer: applying event"
            );

            (projection.apply_fn)(&projection.projection_id, envelope, &self.projection_store)
                .map_err(|message| ProjectionConsumerError::ApplyFailed {
                    projection_id: projection.projection_id.clone(),
                    message,
                })?;

            self.projection_store
                .set_checkpoint(&projection.projection_id, Version::from_u64(event_version))?;

            tracing::debug!(
                projection_id = %projection.projection_id,
                new_checkpoint = event_version,
                "projection consumer: checkpoint advanced"
            );
        }

        Ok(())
    }

    /// Access the underlying projection store (for test assertions).
    pub fn projection_store(&self) -> &InMemoryProjectionStore {
        &self.projection_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AggregateId;
    use bytes::Bytes;
    use chrono::Utc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    fn make_event(version: u64) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::from_u64(version),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    fn counting_projection(id: &str, counter: Arc<AtomicU32>) -> RegisteredProjection {
        RegisteredProjection {
            projection_id: id.to_owned(),
            apply_fn: Box::new(move |_proj_id, _envelope, _store| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        }
    }

    #[test]
    fn process_applies_event_to_all_projections() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        consumer.register(counting_projection("proj-a", Arc::clone(&counter_a)));
        consumer.register(counting_projection("proj-b", Arc::clone(&counter_b)));

        let event = make_event(1);
        consumer.process(&event).unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn process_skips_stale_events() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Event at version 3 is older than checkpoint 5 — should be skipped
        let event = make_event(3);
        consumer.process(&event).unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn process_skips_same_version_as_checkpoint() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Event at exactly version 5 — should be skipped (already processed)
        let event = make_event(5);
        consumer.process(&event).unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn process_advances_checkpoint() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1)).unwrap();
        consumer.process(&make_event(2)).unwrap();
        consumer.process(&make_event(3)).unwrap();

        let checkpoint = consumer
            .projection_store()
            .get_checkpoint("proj-a")
            .unwrap();
        assert_eq!(checkpoint.as_u64(), 3);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn process_propagates_apply_error() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        consumer.register(RegisteredProjection {
            projection_id: "failing-proj".to_owned(),
            apply_fn: Box::new(
                |_proj_id, _envelope, _store| Err("something went wrong".to_owned()),
            ),
        });

        let result = consumer.process(&make_event(1));
        assert!(matches!(
            result,
            Err(ProjectionConsumerError::ApplyFailed { .. })
        ));

        // Checkpoint should NOT advance on error
        let checkpoint = consumer
            .projection_store()
            .get_checkpoint("failing-proj")
            .unwrap();
        assert_eq!(checkpoint.as_u64(), 0);
    }

    #[test]
    fn projections_have_independent_checkpoints() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(3))
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);

        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        consumer.register(counting_projection("proj-a", Arc::clone(&counter_a)));
        consumer.register(counting_projection("proj-b", Arc::clone(&counter_b)));

        // Event at version 2: proj-a skips (checkpoint 3), proj-b applies (checkpoint 0)
        consumer.process(&make_event(2)).unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 0);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn idempotent_replay_skips_duplicates() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1)).unwrap();
        consumer.process(&make_event(1)).unwrap(); // duplicate — should skip
        consumer.process(&make_event(2)).unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
