//! Outbound queue consumers.
//!
//! Three independent consumer groups consume from the outbound queue:
//!
//! - **Event store consumer** — writes events to the event store, takes snapshots every N versions
//! - **Projection consumer** — applies events to projection read models
//! - **Publisher consumer** — publishes events to the external topic for cross-service consumption

pub mod event_store_consumer;
pub mod projection_consumer;
pub mod publisher_consumer;

pub use event_store_consumer::{
    EventStoreConsumer, EventStoreConsumerConfig, EventStoreConsumerError, InMemoryRetryTracker,
};
pub use projection_consumer::{
    ProjectionApplyFn, ProjectionConsumer, ProjectionConsumerError, RegisteredProjection,
};
pub use publisher_consumer::{PublisherConsumer, PublisherConsumerError};
