//! Projection consumer — applies events to projection read models.
//!
//! Consumes `EventEnvelope` messages from the outbound queue and applies them
//! to registered projection implementations. Tracks `last_version` checkpoint
//! per projection for idempotent replay. Each projection runs in its own tokio
//! task. While `rebuilding == true`, read endpoints fall back to read-through.
//!
//! Generic over `ProjectionCheckpointStore` so the same consumer logic works
//! with both in-memory test impls and production infrastructure.

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
    /// For each projection:
    /// 1. Read the checkpoint (`last_version`).
    /// 2. Skip if the event version is not newer than the checkpoint.
    /// 3. Apply the event.
    /// 4. Update the checkpoint to the event's version.
    pub async fn process(&self, envelope: &EventEnvelope) -> Result<(), ProjectionConsumerError> {
        let event_version = envelope.version.as_u64();

        for projection in &self.projections {
            let checkpoint = self
                .checkpoint_store
                .get_checkpoint(&projection.projection_id)
                .await
                .map_err(|e| ProjectionConsumerError::CheckpointStore(e.to_string()))?;

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

            (projection.apply_fn)(&projection.projection_id, envelope).map_err(|message| {
                ProjectionConsumerError::ApplyFailed {
                    projection_id: projection.projection_id.clone(),
                    message,
                }
            })?;

            self.checkpoint_store
                .set_checkpoint(&projection.projection_id, Version::from_u64(event_version))
                .await
                .map_err(|e| ProjectionConsumerError::CheckpointStore(e.to_string()))?;

            tracing::debug!(
                projection_id = %projection.projection_id,
                new_checkpoint = event_version,
                "projection consumer: checkpoint advanced"
            );
        }

        Ok(())
    }

    /// Access the underlying checkpoint store (for test assertions).
    pub fn checkpoint_store(&self) -> &CS {
        &self.checkpoint_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryProjectionStore;
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
        consumer.process(&event).await.unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn process_skips_stale_events() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Event at version 3 is older than checkpoint 5 — should be skipped
        let event = make_event(3);
        consumer.process(&event).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn process_skips_same_version_as_checkpoint() {
        let store = InMemoryProjectionStore::new();
        store
            .set_checkpoint("proj-a", Version::from_u64(5))
            .await
            .unwrap();

        let mut consumer = ProjectionConsumer::new(store);
        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        // Event at exactly version 5 — should be skipped (already processed)
        let event = make_event(5);
        consumer.process(&event).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn process_advances_checkpoint() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1)).await.unwrap();
        consumer.process(&make_event(2)).await.unwrap();
        consumer.process(&make_event(3)).await.unwrap();

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

        let result = consumer.process(&make_event(1)).await;
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

        // Event at version 2: proj-a skips (checkpoint 3), proj-b applies (checkpoint 0)
        consumer.process(&make_event(2)).await.unwrap();

        assert_eq!(counter_a.load(Ordering::SeqCst), 0);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn idempotent_replay_skips_duplicates() {
        let store = InMemoryProjectionStore::new();
        let mut consumer = ProjectionConsumer::new(store);

        let counter = Arc::new(AtomicU32::new(0));
        consumer.register(counting_projection("proj-a", Arc::clone(&counter)));

        consumer.process(&make_event(1)).await.unwrap();
        consumer.process(&make_event(1)).await.unwrap(); // duplicate — should skip
        consumer.process(&make_event(2)).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
