use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub use canon_core::AggregateId;

/// A message that has exhausted its retry budget and been moved to the dead letter store.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: Uuid,
    pub message_id: Uuid,
    pub handler_id: String,
    pub aggregate_id: AggregateId,
    pub payload: Bytes,
    pub error: String,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
    pub last_attempted: DateTime<Utc>,
}

/// Errors returned by [`DeadLetterStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum DeadLetterError {
    #[error("dead letter error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Port for persisting and managing dead-lettered messages.
///
/// Implementations are provided by infrastructure crates such as `canon-deadletter-yugabyte`.
#[async_trait]
pub trait DeadLetterStore: Send + Sync + 'static {
    /// Persist a dead letter entry.
    async fn store(&self, letter: DeadLetter) -> Result<(), DeadLetterError>;

    /// List dead letters, optionally filtered by handler ID.
    async fn list(&self, handler_id: Option<&str>) -> Result<Vec<DeadLetter>, DeadLetterError>;

    /// Re-enter a dead letter into the inbox for reprocessing.
    async fn requeue(&self, id: Uuid) -> Result<(), DeadLetterError>;

    /// Permanently remove a dead letter.
    async fn discard(&self, id: Uuid) -> Result<(), DeadLetterError>;
}
