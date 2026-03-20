pub mod error;
pub mod memory;
pub mod registration;
pub mod traits;
pub mod types;

pub use error::{DeadLetterError, EventStoreError, InboxError, MacroError, RetryError};
pub use memory::{
    AdaptorError, CommandStoreError, ConsumerHandle, CounterfactualReplayError,
    DefaultCounterfactualReplay, ExpiredWindow, InMemoryAdaptor, InMemoryCommandStore,
    InMemoryDeadLetter, InMemoryDeadLetterStore, InMemoryEventStore, InMemoryInboundQueue,
    InMemoryInbox, InMemoryInboxPort, InMemoryOutboundQueue, InMemoryProjectionRebuildManager,
    InMemoryProjectionStore, InMemoryPublisher, InMemoryRetryTracker, InMemorySnapshotStore,
    InboundQueueError, OutboundQueueError, ProjectionStoreError, PublisherError, RetryOutcome,
    RetryPolicy, RetryPolicyError, SnapshotStoreError, DEFAULT_MAX_RETRIES,
};
pub use registration::*;
pub use traits::{
    Aggregate, CommandHandler, CommandStore, CounterfactualReplay, EventCombiner, EventHandler,
    InboxPort, InboxPortError, Projection, ProjectionHandler, ProjectionRebuildError,
    ProjectionRebuildManager, ProjectionStore, RetryAttempt, RetryTracker,
};
pub use types::*;

// Re-export proc macros so users can write `canon_core::aggregate` etc.
pub use canon_core_macros::{
    aggregate, command, command_handler, event, event_combiner, event_handler, projection,
    projection_handler,
};

// Re-export derive macros and attribute macros used in generated code.
// These are #[doc(hidden)] to keep the public API clean.
#[doc(hidden)]
pub use async_trait::async_trait as __async_trait;
#[doc(hidden)]
pub use inventory::submit as __submit;
#[doc(hidden)]
pub use serde::Deserialize as __Deserialize;
#[doc(hidden)]
pub use serde::Serialize as __Serialize;
