pub use canon_core::EventEnvelope;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("publish error: {0}")]
    Publish(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait EventPublisher: Send + Sync + 'static {
    /// Publish an event to a named topic (e.g. "canon.fleet.events").
    /// Called by the outbox worker after confirming the event is persisted.
    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), PublisherError>;
}
