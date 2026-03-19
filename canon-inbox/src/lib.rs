pub use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, IncomingMessage, Oversight};

use async_trait::async_trait;

/// Registration metadata for an event handler's inbox subscription.
#[derive(Debug, Clone)]
pub struct HandlerRegistration {
    /// Unique identifier for this handler.
    pub handler_id: String,
    /// The `event_type` strings this handler subscribes to.
    pub event_types: Vec<String>,
}

/// Errors returned by [`Inbox`] operations.
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("inbox error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Port for idempotent message intake with windowed accumulation and oversight.
///
/// Implementations are responsible for:
/// - Deduplication via `handler_id` + `message_id` composite key
/// - Windowed accumulation per handler + aggregate
/// - Oversight evaluation after each non-duplicate submission
///
/// The infrastructure implementation lives in `canon-inbox-yugabyte`.
#[async_trait]
pub trait Inbox: Send + Sync + 'static {
    /// Register a handler at startup. Must be called before [`submit`](Inbox::submit).
    async fn register_handler(&self, registration: HandlerRegistration) -> Result<(), InboxError>;

    /// Submit a message for a specific handler.
    ///
    /// Idempotent — same `handler_id` + `message_id` is a no-op.
    /// Runs oversight after each non-duplicate submission.
    async fn submit(
        &self,
        handler_id: &str,
        message_id: uuid::Uuid,
        message: IncomingMessage,
    ) -> Result<(), InboxError>;
}
