//! ServiceBuilder — discovers macro-generated registrations via `inventory` and
//! validates exhaustiveness before creating a runnable `Service`.
//!
//! Usage:
//! ```ignore
//! let service = ServiceBuilder::new("fleet")
//!     .for_aggregate::<Ship>()
//!     .event_store(event_store)
//!     .snapshot_store(snapshot_store)
//!     .dead_letter_store(dead_letter_store)
//!     .retry_tracker(retry_tracker)
//!     .snapshot_state_provider(snapshot_provider)
//!     .outbox_store(outbox_store)
//!     .outbox_publisher(outbox_publisher)
//!     .projection_checkpoint_store(projection_store)
//!     .publisher(publisher)
//!     .build()?;
//!
//! service.start(shutdown_rx).await;
//! ```

use std::collections::HashSet;

use crate::consumers::{
    EventStoreConsumer, EventStoreConsumerConfig, ProjectionConsumer, PublisherConsumer,
    SnapshotStateProvider,
};
use crate::outbox::{OutboxProcessor, OutboxProcessorConfig, OutboxPublisher, OutboxStore};
use crate::registration::{
    CommandHandlerRegistration, CommandRegistration, EventCombinerRegistration, EventRegistration,
};
use crate::traits::{DeadLetterStore, EventStore, Publisher, RetryTracker, SnapshotStore};
use crate::{Aggregate, ProjectionCheckpointStore};

/// Errors produced during service validation.
#[derive(Debug, thiserror::Error)]
pub enum ServiceBuilderError {
    /// A command registration has no matching command handler.
    #[error(
        "missing command handler: aggregate={aggregate}, command={command}, version={version}"
    )]
    MissingCommandHandler {
        aggregate: &'static str,
        command: &'static str,
        version: u32,
    },

    /// An event registration has no matching event combiner.
    #[error("missing event combiner: aggregate={aggregate}, event={event}, version={version}")]
    MissingEventCombiner {
        aggregate: &'static str,
        event: &'static str,
        version: u32,
    },

    /// A required infrastructure component was not provided.
    #[error("missing required component: {0}")]
    MissingComponent(&'static str),
}

/// Validates inventory registrations and builds a runnable `Service`.
///
/// The builder collects aggregate type names, then at `build()` time:
/// 1. Scans `inventory` for all command/event/handler/combiner registrations.
/// 2. Validates exhaustiveness: every command has a handler, every event has a combiner.
/// 3. Constructs the runtime `Service` with all background processors wired.
pub struct ServiceBuilder<
    ES = (),
    SS = (),
    DL = (),
    RT = (),
    SP = (),
    OS = (),
    OP = (),
    CS = (),
    PB = (),
> {
    service_name: String,
    aggregate_names: HashSet<&'static str>,
    event_store: Option<ES>,
    snapshot_store: Option<SS>,
    dead_letter_store: Option<DL>,
    retry_tracker: Option<RT>,
    snapshot_state_provider: Option<SP>,
    outbox_store: Option<OS>,
    outbox_publisher: Option<OP>,
    projection_checkpoint_store: Option<CS>,
    publisher: Option<PB>,
    snapshot_every: u64,
    topic: Option<String>,
}

impl ServiceBuilder {
    /// Create a new ServiceBuilder for the named service.
    pub fn new(service_name: impl Into<String>) -> Self {
        ServiceBuilder {
            service_name: service_name.into(),
            aggregate_names: HashSet::new(),
            event_store: None,
            snapshot_store: None,
            dead_letter_store: None,
            retry_tracker: None,
            snapshot_state_provider: None,
            outbox_store: None,
            outbox_publisher: None,
            projection_checkpoint_store: None,
            publisher: None,
            snapshot_every: 50,
            topic: None,
        }
    }
}

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB> {
    /// Register an aggregate type. The builder will validate that all commands
    /// and events for this aggregate have matching handlers and combiners.
    pub fn for_aggregate<A: Aggregate>(mut self) -> Self {
        let name = std::any::type_name::<A>();
        // Extract short name (after last ::)
        let short = name.rsplit("::").next().unwrap_or(name);
        self.aggregate_names.insert(short);
        self
    }

    /// Set the event store implementation.
    pub fn event_store<NewES>(
        self,
        es: NewES,
    ) -> ServiceBuilder<NewES, SS, DL, RT, SP, OS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: Some(es),
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the snapshot store implementation.
    pub fn snapshot_store<NewSS>(
        self,
        ss: NewSS,
    ) -> ServiceBuilder<ES, NewSS, DL, RT, SP, OS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: Some(ss),
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the dead letter store implementation.
    pub fn dead_letter_store<NewDL>(
        self,
        dl: NewDL,
    ) -> ServiceBuilder<ES, SS, NewDL, RT, SP, OS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: Some(dl),
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the retry tracker implementation.
    pub fn retry_tracker<NewRT>(
        self,
        rt: NewRT,
    ) -> ServiceBuilder<ES, SS, DL, NewRT, SP, OS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: Some(rt),
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the snapshot state provider.
    pub fn snapshot_state_provider<NewSP>(
        self,
        sp: NewSP,
    ) -> ServiceBuilder<ES, SS, DL, RT, NewSP, OS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: Some(sp),
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the outbox store implementation.
    pub fn outbox_store<NewOS>(
        self,
        os: NewOS,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, NewOS, OP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: Some(os),
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the outbox publisher implementation.
    pub fn outbox_publisher<NewOP>(
        self,
        op: NewOP,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, NewOP, CS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: Some(op),
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the projection checkpoint store implementation.
    pub fn projection_checkpoint_store<NewCS>(
        self,
        cs: NewCS,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, NewCS, PB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: Some(cs),
            publisher: self.publisher,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Set the publisher implementation for cross-service event publishing.
    pub fn publisher<NewPB>(
        self,
        pb: NewPB,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, NewPB> {
        ServiceBuilder {
            service_name: self.service_name,
            aggregate_names: self.aggregate_names,
            event_store: self.event_store,
            snapshot_store: self.snapshot_store,
            dead_letter_store: self.dead_letter_store,
            retry_tracker: self.retry_tracker,
            snapshot_state_provider: self.snapshot_state_provider,
            outbox_store: self.outbox_store,
            outbox_publisher: self.outbox_publisher,
            projection_checkpoint_store: self.projection_checkpoint_store,
            publisher: Some(pb),
            snapshot_every: self.snapshot_every,
            topic: self.topic,
        }
    }

    /// Override the snapshot interval (default: 50).
    pub fn snapshot_every(mut self, n: u64) -> Self {
        self.snapshot_every = n;
        self
    }

    /// Set the external publish topic (e.g., `canon.fleet.events`).
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }
}

/// Validate that all commands and events for the registered aggregates
/// have matching handlers and combiners. Returns errors for any missing
/// registrations.
pub fn validate_registrations(
    aggregate_names: &HashSet<&str>,
) -> Result<ServiceRegistrations, Vec<ServiceBuilderError>> {
    let mut errors = Vec::new();
    let mut registrations = ServiceRegistrations::default();

    // Collect all handler and combiner registrations into sets for O(1) lookup.
    let mut handler_set: HashSet<(&str, &str, u32)> = HashSet::new();
    for reg in inventory::iter::<CommandHandlerRegistration> {
        handler_set.insert((
            reg.aggregate_type_name,
            reg.command_type_name,
            reg.command_version,
        ));
    }

    let mut combiner_set: HashSet<(&str, u32)> = HashSet::new();
    for reg in inventory::iter::<EventCombinerRegistration> {
        combiner_set.insert((reg.event_type_name, reg.event_version));
    }

    // Validate commands: every command must have a matching handler.
    for reg in inventory::iter::<CommandRegistration> {
        if !aggregate_names
            .iter()
            .any(|name| reg.aggregate_type_name.ends_with(name))
        {
            continue;
        }
        registrations.commands.push(reg);

        if !handler_set.iter().any(|(agg, cmd, ver)| {
            agg.ends_with(
                reg.aggregate_type_name
                    .rsplit("::")
                    .next()
                    .unwrap_or(reg.aggregate_type_name),
            ) && cmd.ends_with(
                reg.command_type_name
                    .rsplit("::")
                    .next()
                    .unwrap_or(reg.command_type_name),
            ) && *ver == reg.command_version
        }) {
            errors.push(ServiceBuilderError::MissingCommandHandler {
                aggregate: reg.aggregate_type_name,
                command: reg.command_type_name,
                version: reg.command_version,
            });
        }
    }

    // Validate events: every event must have a matching combiner.
    for reg in inventory::iter::<EventRegistration> {
        if !aggregate_names
            .iter()
            .any(|name| reg.aggregate_type_name.ends_with(name))
        {
            continue;
        }
        registrations.events.push(reg);

        if !combiner_set.iter().any(|(evt, ver)| {
            evt.ends_with(
                reg.event_type_name
                    .rsplit("::")
                    .next()
                    .unwrap_or(reg.event_type_name),
            ) && *ver == reg.event_version
        }) {
            errors.push(ServiceBuilderError::MissingEventCombiner {
                aggregate: reg.aggregate_type_name,
                event: reg.event_type_name,
                version: reg.event_version,
            });
        }
    }

    if errors.is_empty() {
        Ok(registrations)
    } else {
        Err(errors)
    }
}

/// Discovered registrations after validation.
#[derive(Default)]
pub struct ServiceRegistrations {
    pub commands: Vec<&'static CommandRegistration>,
    pub events: Vec<&'static EventRegistration>,
}

// ── Service (full infrastructure) ──────────────────────────────────────────

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore,
    PB: Publisher,
{
    /// Validate all registrations and build the `Service`.
    ///
    /// Returns `Err` if any commands are missing handlers or events are
    /// missing combiners, or if a required infrastructure component was
    /// not provided.
    #[allow(clippy::type_complexity)]
    pub fn build(self) -> Result<Service<ES, SS, DL, RT, SP, OS, OP, CS, PB>, ServiceBuilderError> {
        // Validate exhaustiveness.
        let registrations = validate_registrations(&self.aggregate_names).map_err(|errs| {
            // Return the first error for simplicity.
            errs.into_iter()
                .next()
                .unwrap_or(ServiceBuilderError::MissingComponent("unknown"))
        })?;

        let topic = self
            .topic
            .unwrap_or_else(|| format!("canon.{}.events", self.service_name));

        let event_store = self
            .event_store
            .ok_or(ServiceBuilderError::MissingComponent("event_store"))?;
        let snapshot_store = self
            .snapshot_store
            .ok_or(ServiceBuilderError::MissingComponent("snapshot_store"))?;
        let dead_letter_store = self
            .dead_letter_store
            .ok_or(ServiceBuilderError::MissingComponent("dead_letter_store"))?;
        let retry_tracker = self
            .retry_tracker
            .ok_or(ServiceBuilderError::MissingComponent("retry_tracker"))?;
        let snapshot_state_provider =
            self.snapshot_state_provider
                .ok_or(ServiceBuilderError::MissingComponent(
                    "snapshot_state_provider",
                ))?;
        let outbox_store = self
            .outbox_store
            .ok_or(ServiceBuilderError::MissingComponent("outbox_store"))?;
        let outbox_publisher = self
            .outbox_publisher
            .ok_or(ServiceBuilderError::MissingComponent("outbox_publisher"))?;
        let projection_checkpoint_store =
            self.projection_checkpoint_store
                .ok_or(ServiceBuilderError::MissingComponent(
                    "projection_checkpoint_store",
                ))?;
        let publisher = self
            .publisher
            .ok_or(ServiceBuilderError::MissingComponent("publisher"))?;

        let event_store_consumer = EventStoreConsumer::new(
            event_store,
            snapshot_store,
            dead_letter_store,
            retry_tracker,
            snapshot_state_provider,
            EventStoreConsumerConfig {
                snapshot_every: self.snapshot_every,
                ..Default::default()
            },
        );

        let outbox_processor = OutboxProcessor::new(
            outbox_store,
            outbox_publisher,
            OutboxProcessorConfig::default(),
        );

        let projection_consumer = ProjectionConsumer::new(projection_checkpoint_store);

        let publisher_consumer = PublisherConsumer::new(publisher, &topic);

        let command_count = registrations.commands.len();
        let event_count = registrations.events.len();

        tracing::info!(
            service = %self.service_name,
            aggregates = ?self.aggregate_names,
            commands = command_count,
            events = event_count,
            topic = %topic,
            "service built successfully"
        );

        Ok(Service {
            service_name: self.service_name,
            event_store_consumer,
            outbox_processor,
            projection_consumer,
            publisher_consumer,
        })
    }
}

/// A fully wired Canon service, ready to process commands and events.
///
/// Created by `ServiceBuilder::build()` after successful validation.
/// Contains all the runtime processors: outbox processor, event store
/// consumer, projection consumer, and publisher consumer.
pub struct Service<ES, SS, DL, RT, SP, OS, OP, CS, PB>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore,
    PB: Publisher,
{
    service_name: String,
    pub event_store_consumer: EventStoreConsumer<ES, SS, DL, RT, SP>,
    pub outbox_processor: OutboxProcessor<OS, OP>,
    pub projection_consumer: ProjectionConsumer<CS>,
    pub publisher_consumer: PublisherConsumer<PB>,
}

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB> Service<ES, SS, DL, RT, SP, OS, OP, CS, PB>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore,
    PB: Publisher,
{
    /// The name of this service.
    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{
        InMemoryDeadLetterStore, InMemoryEventStore, InMemoryOutboundQueue,
        InMemoryOutboxPublisher, InMemoryOutboxStore, InMemoryProjectionStore, InMemoryPublisher,
        InMemoryRetryTracker, InMemorySnapshotStore,
    };
    use crate::EventPayloadSnapshotProvider;

    fn make_builder() -> ServiceBuilder<
        InMemoryEventStore,
        InMemorySnapshotStore,
        InMemoryDeadLetterStore,
        InMemoryRetryTracker,
        EventPayloadSnapshotProvider,
        InMemoryOutboxStore,
        InMemoryOutboxPublisher,
        InMemoryProjectionStore,
        InMemoryPublisher,
    > {
        ServiceBuilder::new("test")
            .event_store(InMemoryEventStore::new())
            .snapshot_store(InMemorySnapshotStore::new())
            .dead_letter_store(InMemoryDeadLetterStore::new())
            .retry_tracker(InMemoryRetryTracker::new())
            .snapshot_state_provider(EventPayloadSnapshotProvider)
            .outbox_store(InMemoryOutboxStore::new())
            .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
            .projection_checkpoint_store(InMemoryProjectionStore::new())
            .publisher(InMemoryPublisher::new())
    }

    #[test]
    fn build_succeeds_with_no_aggregates() {
        let result = make_builder().build();
        assert!(result.is_ok());
    }

    #[test]
    fn service_name_is_set() {
        let service = make_builder().build().unwrap();
        assert_eq!(service.service_name(), "test");
    }

    // Note: missing components are caught at the type level — you cannot call
    // .build() if any infrastructure type parameter is still `()`, since `()`
    // does not implement EventStore, SnapshotStore, etc. This is enforced by
    // the trait bounds on the `build()` impl block.
}
