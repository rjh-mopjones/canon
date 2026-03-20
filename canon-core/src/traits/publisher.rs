use async_trait::async_trait;

use crate::EventEnvelope;

/// Trait for publishing events to external topics (e.g., `canon.{service}.events`).
#[async_trait]
pub trait Publisher: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Publish an event to the specified topic.
    async fn publish(&self, envelope: EventEnvelope, topic: &str) -> Result<(), Self::Error>;
}
