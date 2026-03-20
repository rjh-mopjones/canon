use crate::CommandEnvelope;
use async_trait::async_trait;

/// Errors returned by [`InboxPort`] operations.
#[derive(Debug, thiserror::Error)]
pub enum InboxPortError {
    /// The underlying inbox rejected the submission.
    #[error("inbox submission failed: {reason}")]
    SubmitFailed { reason: String },
}

/// Local re-entry port for event handlers that produce commands.
///
/// When an event handler returns `Some(CommandEnvelope)`, the dispatcher
/// submits it back into the local inbox via this trait. This keeps command
/// flow within the same service boundary — cross-service communication
/// uses REST, not `InboxPort`.
///
/// `ServiceBuilder` injects the concrete implementation into the
/// dispatcher at startup.
#[async_trait]
pub trait InboxPort: Send + Sync + 'static {
    /// Submit a command envelope to the local inbox for dispatch.
    ///
    /// Implementations must be idempotent: re-submitting the same
    /// `command_id` is a safe no-op.
    async fn submit(&self, command: CommandEnvelope) -> Result<(), InboxPortError>;
}
