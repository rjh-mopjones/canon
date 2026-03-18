pub mod types;
pub mod traits;
pub mod error;
pub mod memory;

pub use types::*;
pub use traits::{
    Aggregate, CommandHandler, EventHandler,
    Projection, ProjectionStore,
    CounterfactualReplay,
};
pub use error::{EventStoreError, InboxError, DeadLetterError};
pub use memory::{
    InMemoryEventStore,
    InMemoryCommandStore, CommandStoreError,
    InMemorySnapshotStore, SnapshotStoreError,
    InMemoryInbox,
    InMemoryInboundQueue, InboundQueueError,
    InMemoryOutboundQueue, OutboundQueueError, ConsumerHandle,
    InMemoryProjectionStore, ProjectionStoreError,
    InMemoryPublisher, PublisherError,
    InMemoryAdaptor, AdaptorError,
    InMemoryDeadLetterStore, InMemoryDeadLetter,
};
