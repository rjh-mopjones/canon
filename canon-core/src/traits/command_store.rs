use async_trait::async_trait;
use crate::{AggregateId, CommandEnvelope};

/// Trait for storing and retrieving commands.
#[async_trait]
pub trait CommandStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn append(&self, envelope: CommandEnvelope) -> Result<(), Self::Error>;

    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<CommandEnvelope>, Self::Error>;
}
