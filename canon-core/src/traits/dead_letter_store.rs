use async_trait::async_trait;
use uuid::Uuid;

use crate::{AggregateId, DeadLetter};

/// Trait for storing, querying, and managing dead-lettered messages.
#[async_trait]
pub trait DeadLetterStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a dead-lettered message. Returns the dead letter ID.
    async fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: bytes::Bytes,
        error: &str,
    ) -> Result<Uuid, Self::Error>;

    /// List dead letters, optionally filtered by handler ID.
    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, Self::Error>;

    /// Re-enter a dead letter into the inbox for reprocessing.
    async fn requeue(&self, id: Uuid) -> Result<(), Self::Error>;

    /// Permanently remove a dead letter.
    async fn discard(&self, id: Uuid) -> Result<(), Self::Error>;
}
