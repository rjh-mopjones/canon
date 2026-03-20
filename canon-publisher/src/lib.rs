pub use canon_core::{AggregateId, EventEnvelope};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("publish error: {0}")]
    Publish(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Publishes confirmed events to an external topic for cross-service consumption.
///
/// Implementations must be idempotent — publishing the same event twice
/// should not result in duplicates on the target topic.
#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    /// Publish an event envelope to the given topic.
    /// The partition key should be derived from `envelope.aggregate_id`.
    async fn publish(&self, envelope: &EventEnvelope, topic: &str) -> Result<(), PublisherError>;
}
