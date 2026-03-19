use async_trait::async_trait;

pub use canon_core::{AggregateId, CommandEnvelope, Version};

#[derive(Debug, thiserror::Error)]
pub enum CommandStoreError {
    #[error("store error: {0}")]
    Store(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait CommandStore: Send + Sync + 'static {
    /// Idempotent — duplicate command_id is silently ignored.
    async fn append(&self, envelope: CommandEnvelope) -> Result<(), CommandStoreError>;

    /// Load commands for an aggregate. Both version bounds are optional.
    /// Used by the counterfactual replay engine.
    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from_version: Option<Version>,
        to_version: Option<Version>,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError>;
}
