use crate::{CommandEnvelope, IncomingMessage, Oversight};
use async_trait::async_trait;

/// Receives a batch of events and optionally produces one command.
/// An event can have multiple handlers — fan-out via registration.
/// One handler produces zero or one command. Never more than one.
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error>;

    /// Inspect accumulated inbox messages and decide dispatch readiness.
    /// Default: always Ready (dispatch on every incoming message).
    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        let _ = accumulated;
        Oversight::Ready
    }
}
