use crate::Version;

#[derive(Debug, thiserror::Error)]
pub enum EventStoreError {
    #[error("version conflict: expected {expected:?}, got {actual:?}")]
    VersionConflict {
        expected: Version,
        actual: Option<Version>,
    },
    #[error("internal lock error")]
    Poisoned,
}

#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    #[error("handler not registered: {handler_id}")]
    HandlerNotRegistered { handler_id: String },
    #[error("internal lock error")]
    Poisoned,
}

#[derive(Debug, thiserror::Error)]
pub enum DeadLetterError {
    #[error("dead letter not found: {id}")]
    NotFound { id: uuid::Uuid },
    #[error("internal lock error")]
    Poisoned,
}
