use async_trait::async_trait;
use uuid::Uuid;
use crate::{AggregateId, CommandEnvelope, CommandStatus};

/// Trait for storing and retrieving commands.
#[async_trait]
pub trait CommandStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn append(&self, envelope: CommandEnvelope) -> Result<(), Self::Error>;

    async fn load(
        &self,
        command_id: Uuid,
    ) -> Result<Option<CommandEnvelope>, Self::Error>;

    async fn load_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<CommandEnvelope>, Self::Error>;

    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<CommandEnvelope>, Self::Error>;

    async fn update_status(
        &self,
        command_id: Uuid,
        status: CommandStatus,
    ) -> Result<(), Self::Error>;
}
