pub use canon_core::{AggregateId, IncomingMessage};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum InboundQueueError {
    #[error("inbound queue error: {0}")]
    Queue(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Carries assembled `IncomingMessage` batches from the inbox to handlers.
///
/// Messages are partitioned by `aggregate_id` to ensure strict per-aggregate
/// ordering. In production this is backed by `canon-inbound-queue-kafka`.
///
/// - `publish()` sends a batch to the queue, partitioned by `aggregate_id`.
/// - `receive()` returns the next batch for this consumer group.
/// - `commit()` commits the offset for the last received message (manual commit).
#[async_trait]
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
