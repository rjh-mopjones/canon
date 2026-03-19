pub use canon_core::{AggregateId, EventEnvelope};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum OutboundQueueError {
    #[error("outbound queue error: {0}")]
    Queue(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Carries committed `EventEnvelope` events from the outbox processor to
/// downstream consumers. Each consumer group (event store, projection,
/// publisher) creates its own `OutboundQueue` instance with a distinct
/// `group_id` configured at construction time.
///
/// - `publish()` produces an event envelope to the queue, partitioned by `aggregate_id`.
/// - `receive()` returns the next envelope for this consumer group.
/// - `commit()` commits the offset for the last received message (manual commit).
#[async_trait]
pub trait OutboundQueue: Send + Sync + 'static {
    /// Publish an event envelope to the outbound queue.
    /// Partition key is derived from the envelope's `aggregate_id`.
    async fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError>;

    /// Receive the next event envelope for this consumer group.
    /// Returns `None` if no messages are currently available.
    async fn receive(&self) -> Result<Option<EventEnvelope>, OutboundQueueError>;

    /// Commit the offset of the last received message.
    /// Must be called after confirmed downstream write to ensure at-least-once delivery.
    async fn commit(&self) -> Result<(), OutboundQueueError>;
}
