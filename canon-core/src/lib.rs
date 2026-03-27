pub mod admin;
pub mod consumers;
pub mod debug;
pub mod dispatcher;
pub mod error;
pub mod health;
pub mod memory;
pub mod observability;
pub mod outbox;
pub mod registration;
pub mod service_builder;
pub mod traits;
pub mod types;

pub use admin::{
    AdminError, AdminHandler, InboxWindowFilter, InboxWindowResponse, RebuildResponse,
    RetryStatusResponse,
};
pub use consumers::{
    ConsumerReceiver, ConsumerReceiverError, EventPayloadSnapshotProvider, EventStoreConsumer,
    EventStoreConsumerConfig, EventStoreConsumerError, ProjectionConsumer, ProjectionConsumerError,
    PublisherConsumer, PublisherConsumerError, ReceivedEnvelope, SnapshotStateProvider,
};
pub use debug::{
    resolve_debug_enabled, DebugAggregateResponse, DebugCommandResponse, DebugEndpointHandler,
    DebugEventResponse, DebugInspector, DebugInspectorError,
};
pub use dispatcher::{
    new_dispatcher_notify_channel, Dispatcher, DispatcherConfig, DispatcherError,
    DispatcherNotifyReceiver, DispatcherNotifySender, DispatcherStore, InboxCommandRow,
};
pub use error::{DeadLetterError, EventStoreError, InboxError, MacroError, RetryError};
pub use health::{
    HealthCheck, HealthCheckDetail, HealthChecker, InMemoryHealthCheck, LivenessResponse,
    ReadinessResponse,
};
pub use memory::{
    AdaptorError, CommandStoreError, ConsumerHandle, CounterfactualReplayError,
    DeadLetteredCommand, DefaultCounterfactualReplay, ExpiredWindow, InMemoryAdaptor,
    InMemoryCommandStore, InMemoryConsumerReceiver, InMemoryDeadLetter, InMemoryDeadLetterStore,
    InMemoryDispatcherStore, InMemoryEventStore, InMemoryInboundQueue, InMemoryInbox,
    InMemoryInboxPort, InMemoryOutboundQueue, InMemoryOutboxPublisher, InMemoryOutboxStore,
    InMemoryProjectionRebuildManager, InMemoryProjectionStore, InMemoryPublisher,
    InMemoryReplayEventStore, InMemoryRetryTracker, InMemorySnapshotStore, InboundQueueError,
    InboxWindowInfo, OutboundQueueError, ProjectionStoreError, PublisherError, RetryOutcome,
    RetryPolicy, RetryPolicyError, SnapshotStoreError, DEFAULT_MAX_RETRIES,
};
pub use observability::{
    CassandraStatus, InMemoryInfraStatusProvider, InMemoryOutboxStatusProvider, InfraStatus,
    InfraStatusProvider, KafkaConsumerGroups, KafkaGroupLag, KafkaStatus, ObservabilityError,
    ObservabilityHandler, OutboxStatus, OutboxStatusProvider, ProjectionStatus, YugabyteStatus,
};
pub use outbox::{
    new_outbox_notify_channel, OutboxEntry, OutboxNotifyReceiver, OutboxNotifySender,
    OutboxProcessor, OutboxProcessorConfig, OutboxProcessorError, OutboxPublisher, OutboxStore,
    DEFAULT_CHANNEL_CAPACITY,
};
pub use registration::*;
pub use service_builder::{
    Service, ServiceBuilder, ServiceBuilderError, ServiceRegistrations, ServiceStartError,
};
pub use traits::{
    Aggregate, CommandHandler, CommandStore, CounterfactualReplay, DeadLetterStore, EventCombiner,
    EventHandler, EventStore, InboxPort, InboxPortError, Projection, ProjectionCheckpointStore,
    ProjectionHandler, ProjectionRebuildError, ProjectionRebuildManager, ProjectionStore,
    Publisher, ReplayEventStore, RetryAttempt, RetryTracker, SnapshotStore,
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
