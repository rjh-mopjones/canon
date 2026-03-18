use async_trait::async_trait;
use crate::CommandEnvelope;
use super::aggregate::Aggregate;

/// Loads aggregate state, validates a command, emits events.
/// One CommandHandler per command type. One executor per command — no fan-out.
#[async_trait]
pub trait CommandHandler<A: Aggregate>: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        envelope: CommandEnvelope,
        state: &A::State,
    ) -> Result<Vec<A::Event>, Self::Error>;
}
