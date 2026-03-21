//! Outbound queue consumers.
//!
//! Three independent consumer groups consume from the outbound queue:
//!
//! - **Event store consumer** — writes events to the event store, takes snapshots every N versions
//! - **Projection consumer** — applies events to projection read models
//! - **Publisher consumer** — publishes events to the external topic for cross-service consumption
//!
//! All consumers are generic over their infrastructure traits, so the same logic
//! works with both in-memory test impls and production infrastructure.
//!
//! Each consumer exposes a `run()` method that polls a [`ConsumerReceiver`] in a
//! loop, processing envelopes until a shutdown signal fires.

use async_trait::async_trait;

use crate::EventEnvelope;

pub mod event_store_consumer;
pub mod projection_consumer;
pub mod publisher_consumer;

pub use event_store_consumer::{
    EventPayloadSnapshotProvider, EventStoreConsumer, EventStoreConsumerConfig,
    EventStoreConsumerError, SnapshotStateProvider,
};
pub use projection_consumer::{
    ProjectionApplyFn, ProjectionConsumer, ProjectionConsumerError, RegisteredProjection,
};
pub use publisher_consumer::{PublisherConsumer, PublisherConsumerError};

// ── Consumer receiver trait ───────────────────────────────────────────────

/// Errors produced by a consumer receiver.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerReceiverError {
    /// The underlying queue returned an error.
    #[error("consumer receive error: {0}")]
    Receive(String),

    /// Offset commit failed.
    #[error("consumer commit error: {0}")]
    Commit(String),
}

/// A received message from the outbound queue, pairing the event envelope
/// with a monotonically increasing global sequence number.
///
/// The sequence number is used by the projection consumer for checkpoint
/// tracking. For Kafka this maps to `offset + 1`; for the in-memory
/// implementation it is an auto-incrementing counter.
#[derive(Debug, Clone)]
pub struct ReceivedEnvelope {
    /// The event envelope.
    pub envelope: EventEnvelope,
    /// Global sequence number (1-based, monotonically increasing).
    pub sequence_number: u64,
}

/// Consumer-side interface to the outbound queue.
///
/// Each consumer group (event store, projection, publisher) receives its own
/// `ConsumerReceiver` instance. The trait is defined in `canon-core` so that
/// consumer `run()` loops can be written without depending on the
/// `canon-outbound-queue` trait crate.
///
/// Implementations:
/// - `InMemoryConsumerReceiver` in `canon-core::memory` (wraps `InMemoryOutboundQueue` + `ConsumerHandle`)
/// - `KafkaOutboundQueue` in `canon-outbound-queue-kafka` (implements via a blanket or direct impl)
#[async_trait]
pub trait ConsumerReceiver: Send + Sync + 'static {
    /// Receive the next event envelope for this consumer group.
    /// Returns `None` if no messages are currently available.
    async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError>;

    /// Commit the offset of the last received message.
    /// Must be called after confirmed downstream processing.
    async fn commit(&self) -> Result<(), ConsumerReceiverError>;
}
