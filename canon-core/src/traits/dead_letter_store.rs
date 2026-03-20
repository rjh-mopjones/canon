use async_trait::async_trait;
use bytes::Bytes;
use uuid::Uuid;

use crate::AggregateId;

/// Trait for storing dead-lettered messages that could not be processed.
#[async_trait]
pub trait DeadLetterStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store a dead-lettered message. Returns the dead letter ID.
    async fn store(
        &self,
        message_id: Uuid,
        handler_id: &str,
        aggregate_id: &AggregateId,
        payload: Bytes,
        error: &str,
    ) -> Result<Uuid, Self::Error>;
}
