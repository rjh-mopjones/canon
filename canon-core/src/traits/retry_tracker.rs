use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A single row in the retry_attempts table.
///
/// Tracks how many times a message has been attempted so the count
/// survives process crashes.
#[derive(Debug, Clone)]
pub struct RetryAttempt {
    pub message_id: Uuid,
    pub handler_id: String,
    pub attempts: u32,
    pub last_attempted: DateTime<Utc>,
}

/// Port for crash-safe retry tracking.
///
/// Implementations persist attempt counts so that a process restart does not
/// reset the counter. The event-store consumer calls `increment` on every
/// Cassandra version conflict and checks the returned count against
/// `max_retries` to decide whether to dead-letter the message.
pub trait RetryTracker: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Increment the attempt counter for this message/handler pair.
    /// Returns the new attempt count after incrementing.
    fn increment(&self, message_id: Uuid, handler_id: &str) -> Result<u32, Self::Error>;

    /// Read the current attempt record, or `None` if the message has never been retried.
    fn get(&self, message_id: Uuid) -> Result<Option<RetryAttempt>, Self::Error>;

    /// Remove the retry record for this message. Called after the message has
    /// been dead-lettered or successfully processed.
    fn remove(&self, message_id: Uuid) -> Result<(), Self::Error>;
}
