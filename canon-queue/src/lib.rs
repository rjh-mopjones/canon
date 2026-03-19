pub use canon_core::{AggregateId, IncomingMessage};

#[derive(Debug, thiserror::Error)]
pub enum InboundQueueError {
    #[error("inbound queue error: {0}")]
    Queue(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait::async_trait]
pub trait InboundQueue: Send + Sync + 'static {
    /// Publish an assembled batch of IncomingMessages from the inbox to the inbound queue.
    /// Partitioned by aggregate_id — all messages for the same aggregate go to the same partition.
    async fn publish(
        &self,
        batch: Vec<IncomingMessage>,
        aggregate_id: &AggregateId,
    ) -> Result<(), InboundQueueError>;

    /// Receive the next batch. Returns None if no messages available.
    /// Consumer group ensures competing consumers across replicas.
    async fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError>;

    /// Commit the offset for the last received message.
    async fn commit(&self) -> Result<(), InboundQueueError>;
}
