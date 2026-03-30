//! Outbound queue consumers.
//!
//! Four independent consumer groups consume from the outbound queue:
//!
//! - **Event store consumer** — writes events to the event store, takes snapshots every N versions
//! - **Projection consumer** — applies events to projection read models
//! - **Publisher consumer** — publishes events to the external topic for cross-service consumption
//! - **Internal event consumer** — routes a service's own events back to the inbox for event handler dispatch
//!
//! All consumers are generic over their infrastructure traits, so the same logic
//! works with both in-memory test impls and production infrastructure.
//!
//! Each consumer implements a `run()` loop that receives events from a
//! [`ConsumerReceiver`], processes them, commits offsets, and shuts down
//! gracefully when signalled.

use async_trait::async_trait;

use crate::EventEnvelope;

pub mod event_store_consumer;
pub mod internal_event_consumer;
pub mod projection_consumer;
pub mod publisher_consumer;

pub use event_store_consumer::{
    EventPayloadSnapshotProvider, EventStoreConsumer, EventStoreConsumerConfig,
    EventStoreConsumerError, SnapshotStateProvider,
};
pub use internal_event_consumer::{InternalEventConsumer, InternalEventConsumerError};
pub use projection_consumer::{
    ProjectionApplyFn, ProjectionConsumer, ProjectionConsumerError, RegisteredProjection,
};
pub use publisher_consumer::{PublisherConsumer, PublisherConsumerError};

/// Errors emitted by a [`ConsumerReceiver`].
#[derive(Debug, thiserror::Error)]
pub enum ConsumerReceiverError {
    /// Failed to receive an event from the outbound queue.
    #[error("receive error: {0}")]
    Receive(String),

    /// Failed to commit the consumer offset.
    #[error("commit error: {0}")]
    Commit(String),
}

/// An event envelope received from the outbound queue, together with its
/// global sequence number (outbox `sequence_number` or Kafka offset + 1).
#[derive(Debug, Clone)]
pub struct ReceivedEnvelope {
    /// The event envelope.
    pub envelope: EventEnvelope,
    /// Global sequence number for checkpoint tracking.
    pub sequence_number: u64,
}

/// Trait for receiving events from the outbound queue.
///
/// Implementations wrap a Kafka consumer, an in-memory channel, or any other
/// transport. The `run()` loops on all three consumers call `receive()` in a
/// loop, process the returned envelope, and then `commit()` the offset.
#[async_trait]
pub trait ConsumerReceiver: Send + Sync + 'static {
    /// Receive the next event from the outbound queue.
    ///
    /// Returns `Ok(None)` when no event is currently available (the caller
    /// should sleep briefly and retry). Returns `Ok(Some(...))` with the
    /// next event. Returns `Err(...)` on transport failure.
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError>;

    /// Commit the current consumer offset.
    ///
    /// Called after successful processing of one or more events.
    async fn commit(&self) -> Result<(), ConsumerReceiverError>;
}
