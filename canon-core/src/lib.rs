pub mod types;
pub mod traits;
pub mod error;
pub mod memory;
pub mod registration;

pub use types::*;
pub use traits::{
    Aggregate, CommandHandler, EventHandler, EventCombiner,
    Projection, ProjectionStore, ProjectionHandler,
    CounterfactualReplay,
};
pub use error::{EventStoreError, InboxError, DeadLetterError, MacroError};
pub use registration::*;
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

// Re-export proc macros so users can write `canon_core::aggregate` etc.
pub use canon_core_macros::{
    aggregate, command, event, event_combiner,
    command_handler, event_handler, projection, projection_handler,
};

// Re-export derive macros and attribute macros used in generated code.
// These are #[doc(hidden)] to keep the public API clean.
#[doc(hidden)]
pub use serde::Serialize as __Serialize;
#[doc(hidden)]
pub use serde::Deserialize as __Deserialize;
#[doc(hidden)]
pub use async_trait::async_trait as __async_trait;
#[doc(hidden)]
pub use inventory::submit as __submit;
