//! ServiceBuilder — discovers macro-generated registrations via `inventory` and
//! validates exhaustiveness before creating a runnable `Service`.
//!
//! Usage:
//! ```ignore
//! let service = ServiceBuilder::new("fleet")
//!     .for_aggregate::<Ship>()
//!     .event_store(event_store)
//!     .snapshot_store(snapshot_store)
//!     .command_store(command_store)
//!     .dead_letter_store(dead_letter_store)
//!     .retry_tracker(retry_tracker)
//!     .snapshot_state_provider(snapshot_provider)
//!     .outbox_store(outbox_store)
//!     .outbox_publisher(outbox_publisher)
//!     .projection_checkpoint_store(projection_store)
//!     .publisher(publisher)
//!     .debug_endpoints(true)
//!     .build()?;
//!
//! // Access the debug handler (only available when debug endpoints are enabled
//! // and a command store was provided):
//! if let Some(debug_handler) = service.debug_handler() {
//!     // Wire into axum, actix, etc.
//! }
//!
//! service.start(shutdown_rx).await;
//! ```

use std::collections::HashSet;

use crate::admin::AdminHandler;
use crate::consumers::{
    EventStoreConsumer, EventStoreConsumerConfig, ProjectionConsumer, PublisherConsumer,
    SnapshotStateProvider,
};
use crate::debug::{resolve_debug_enabled, DebugEndpointHandler};
use crate::health::{HealthCheck, HealthChecker};
use crate::memory::{InMemoryInbox, InMemoryProjectionRebuildManager, InMemoryRetryTracker};
use crate::observability::{
    InMemoryInfraStatusProvider, InMemoryOutboxStatusProvider, InfraStatusProvider,
    ObservabilityHandler, OutboxStatusProvider,
};
use crate::outbox::{OutboxProcessor, OutboxProcessorConfig, OutboxPublisher, OutboxStore};
use crate::registration::{
    CommandHandlerRegistration, CommandRegistration, EventCombinerRegistration, EventRegistration,
};
use crate::traits::{
    CommandStore, DeadLetterStore, EventStore, Publisher, RetryTracker, SnapshotStore,
};
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

/// Errors produced during [`Service::start()`].
#[derive(Debug, thiserror::Error)]
pub enum ServiceStartError {
    /// Failed to set up the service (e.g., registering consumer handles).
    #[error("service setup error: {0}")]
    Setup(String),

    /// A runtime error from a background task.
    #[error("service runtime error: {0}")]
    Runtime(String),
}

/// Validates inventory registrations and builds a runnable `Service`.
///
/// The builder collects aggregate type names, then at `build()` time:
/// 1. Scans `inventory` for all command/event/handler/combiner registrations.
/// 2. Validates exhaustiveness: every command has a handler, every event has a combiner.
/// 3. Constructs the runtime `Service` with all background processors wired.
/// 4. Optionally creates a [`DebugEndpointHandler`] for `/debug/*` endpoints.
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
    CmdS = (),
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
    command_store: Option<CmdS>,
    snapshot_every: u64,
    topic: Option<String>,
    debug_endpoints: Option<bool>,
    infra_status_provider: Option<Box<dyn InfraStatusProvider>>,
    outbox_status_provider: Option<Box<dyn OutboxStatusProvider>>,
    admin_inbox: Option<InMemoryInbox>,
    admin_retry_tracker: Option<InMemoryRetryTracker>,
    admin_rebuild_manager: Option<InMemoryProjectionRebuildManager>,
    health_checks: Vec<Box<dyn HealthCheck>>,
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
            command_store: None,
            snapshot_every: 50,
            topic: None,
            debug_endpoints: None,
            infra_status_provider: None,
            outbox_status_provider: None,
            admin_inbox: None,
            admin_retry_tracker: None,
            admin_rebuild_manager: None,
            health_checks: Vec::new(),
        }
    }
}

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
    ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
{
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
    ) -> ServiceBuilder<NewES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the snapshot store implementation.
    pub fn snapshot_store<NewSS>(
        self,
        ss: NewSS,
    ) -> ServiceBuilder<ES, NewSS, DL, RT, SP, OS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the dead letter store implementation.
    pub fn dead_letter_store<NewDL>(
        self,
        dl: NewDL,
    ) -> ServiceBuilder<ES, SS, NewDL, RT, SP, OS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the retry tracker implementation.
    pub fn retry_tracker<NewRT>(
        self,
        rt: NewRT,
    ) -> ServiceBuilder<ES, SS, DL, NewRT, SP, OS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the snapshot state provider.
    pub fn snapshot_state_provider<NewSP>(
        self,
        sp: NewSP,
    ) -> ServiceBuilder<ES, SS, DL, RT, NewSP, OS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the outbox store implementation.
    pub fn outbox_store<NewOS>(
        self,
        os: NewOS,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, NewOS, OP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the outbox publisher implementation.
    pub fn outbox_publisher<NewOP>(
        self,
        op: NewOP,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, NewOP, CS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the projection checkpoint store implementation.
    pub fn projection_checkpoint_store<NewCS>(
        self,
        cs: NewCS,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, NewCS, PB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the publisher implementation for cross-service event publishing.
    pub fn publisher<NewPB>(
        self,
        pb: NewPB,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, NewPB, CmdS> {
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
            command_store: self.command_store,
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
        }
    }

    /// Set the command store implementation. Required for debug endpoints.
    pub fn command_store<NewCmdS>(
        self,
        cmd_store: NewCmdS,
    ) -> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB, NewCmdS> {
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
            publisher: self.publisher,
            command_store: Some(cmd_store),
            snapshot_every: self.snapshot_every,
            topic: self.topic,
            debug_endpoints: self.debug_endpoints,
            infra_status_provider: self.infra_status_provider,
            outbox_status_provider: self.outbox_status_provider,
            admin_inbox: self.admin_inbox,
            admin_retry_tracker: self.admin_retry_tracker,
            admin_rebuild_manager: self.admin_rebuild_manager,
            health_checks: self.health_checks,
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

    /// Enable or disable debug endpoints.
    ///
    /// When enabled and a command store is provided, the built [`Service`] will
    /// expose a [`DebugEndpointHandler`] via [`Service::debug_handler()`].
    ///
    /// If not explicitly set, falls back to the `CANON_DEBUG_ENDPOINTS`
    /// environment variable (`true` or `1` to enable), then defaults to `false`.
    pub fn debug_endpoints(mut self, enabled: bool) -> Self {
        self.debug_endpoints = Some(enabled);
        self
    }

    /// Set a custom infrastructure status provider for observability endpoints.
    ///
    /// When not provided, the built [`Service`] will use
    /// [`InMemoryInfraStatusProvider`] which returns synthetic healthy defaults.
    pub fn infra_status_provider(mut self, provider: impl InfraStatusProvider + 'static) -> Self {
        self.infra_status_provider = Some(Box::new(provider));
        self
    }

    /// Set a custom outbox status provider for observability endpoints.
    ///
    /// When not provided, the built [`Service`] will use
    /// [`InMemoryOutboxStatusProvider`] which reports zero pending and zero
    /// throughput.
    pub fn outbox_status_provider(mut self, provider: impl OutboxStatusProvider + 'static) -> Self {
        self.outbox_status_provider = Some(Box::new(provider));
        self
    }

    /// Set the inbox for admin endpoint inspection.
    ///
    /// When provided alongside a retry tracker and rebuild manager, and debug
    /// endpoints are enabled, the built [`Service`] will expose an
    /// [`AdminHandler`] via [`Service::admin_handler()`].
    pub fn admin_inbox(mut self, inbox: InMemoryInbox) -> Self {
        self.admin_inbox = Some(inbox);
        self
    }

    /// Set the retry tracker for admin endpoint inspection.
    ///
    /// Uses the in-memory retry tracker that supports `list_all()`.
    pub fn admin_retry_tracker(mut self, tracker: InMemoryRetryTracker) -> Self {
        self.admin_retry_tracker = Some(tracker);
        self
    }

    /// Set the projection rebuild manager for admin endpoint support.
    ///
    /// When provided alongside an inbox and retry tracker, and debug endpoints
    /// are enabled, the built [`Service`] will expose an [`AdminHandler`] via
    /// [`Service::admin_handler()`].
    pub fn admin_rebuild_manager(mut self, manager: InMemoryProjectionRebuildManager) -> Self {
        self.admin_rebuild_manager = Some(manager);
        self
    }

    /// Register an additional health check for Kubernetes health probes.
    ///
    /// Each registered check will be executed when `/health/ready` or
    /// `/health/startup` is called. The liveness probe (`/health/live`)
    /// never runs dependency checks.
    ///
    /// Infrastructure stores that implement [`HealthCheck`] are automatically
    /// registered; use this method for custom checks.
    pub fn health_check(mut self, check: Box<dyn HealthCheck>) -> Self {
        self.health_checks.push(check);
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

// ── Build helpers ──────────────────────────────────────────────────────────

/// Internal helper: extract and validate core infrastructure from the builder.
/// Shared by both build paths (with and without command store).
struct ValidatedInfra<ES, SS, DL, RT, SP, OS, OP, CS, PB> {
    event_store: ES,
    snapshot_store: SS,
    dead_letter_store: DL,
    retry_tracker: RT,
    snapshot_state_provider: SP,
    outbox_store: OS,
    outbox_publisher: OP,
    projection_checkpoint_store: CS,
    publisher: PB,
    topic: String,
    registrations: ServiceRegistrations,
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn extract_infra<ES, SS, DL, RT, SP, OS, OP, CS, PB>(
    service_name: &str,
    aggregate_names: &HashSet<&str>,
    event_store: Option<ES>,
    snapshot_store: Option<SS>,
    dead_letter_store: Option<DL>,
    retry_tracker: Option<RT>,
    snapshot_state_provider: Option<SP>,
    outbox_store: Option<OS>,
    outbox_publisher: Option<OP>,
    projection_checkpoint_store: Option<CS>,
    publisher: Option<PB>,
    topic: Option<String>,
) -> Result<ValidatedInfra<ES, SS, DL, RT, SP, OS, OP, CS, PB>, ServiceBuilderError> {
    let registrations = validate_registrations(aggregate_names).map_err(|errs| {
        for err in &errs {
            tracing::error!(%err, "service validation failed");
        }
        errs.into_iter()
            .next()
            .unwrap_or(ServiceBuilderError::MissingComponent("unknown"))
    })?;

    let topic = topic.unwrap_or_else(|| format!("canon.{}.events", service_name));

    Ok(ValidatedInfra {
        event_store: event_store.ok_or(ServiceBuilderError::MissingComponent("event_store"))?,
        snapshot_store: snapshot_store
            .ok_or(ServiceBuilderError::MissingComponent("snapshot_store"))?,
        dead_letter_store: dead_letter_store
            .ok_or(ServiceBuilderError::MissingComponent("dead_letter_store"))?,
        retry_tracker: retry_tracker
            .ok_or(ServiceBuilderError::MissingComponent("retry_tracker"))?,
        snapshot_state_provider: snapshot_state_provider.ok_or(
            ServiceBuilderError::MissingComponent("snapshot_state_provider"),
        )?,
        outbox_store: outbox_store.ok_or(ServiceBuilderError::MissingComponent("outbox_store"))?,
        outbox_publisher: outbox_publisher
            .ok_or(ServiceBuilderError::MissingComponent("outbox_publisher"))?,
        projection_checkpoint_store: projection_checkpoint_store.ok_or(
            ServiceBuilderError::MissingComponent("projection_checkpoint_store"),
        )?,
        publisher: publisher.ok_or(ServiceBuilderError::MissingComponent("publisher"))?,
        topic,
        registrations,
    })
}

/// Type alias for the observability handler stored in [`Service`].
///
/// Uses trait objects for the infra and outbox status providers so that
/// the `Service` type signature does not grow with additional type parameters.
type BoxedObservabilityHandler<CS> =
    ObservabilityHandler<CS, Box<dyn InfraStatusProvider>, Box<dyn OutboxStatusProvider>>;

/// Build the observability handler from the builder's provider options and the
/// validated projection checkpoint store. Extracted to avoid duplication between
/// the two `build()` paths.
fn build_observability_handler<CS: ProjectionCheckpointStore + Clone>(
    projection_checkpoint_store: &CS,
    infra_status_provider: Option<Box<dyn InfraStatusProvider>>,
    outbox_status_provider: Option<Box<dyn OutboxStatusProvider>>,
) -> BoxedObservabilityHandler<CS> {
    let infra_provider: Box<dyn InfraStatusProvider> = match infra_status_provider {
        Some(p) => p,
        None => Box::new(InMemoryInfraStatusProvider),
    };
    let outbox_provider: Box<dyn OutboxStatusProvider> = match outbox_status_provider {
        Some(p) => p,
        None => Box::new(InMemoryOutboxStatusProvider::new(
            crate::memory::InMemoryOutboxStore::new(),
        )),
    };
    tracing::info!("observability endpoints auto-mounted");
    ObservabilityHandler::new(
        projection_checkpoint_store.clone(),
        infra_provider,
        outbox_provider,
    )
}

#[allow(clippy::too_many_arguments)]
fn assemble_service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>(
    service_name: String,
    aggregate_names: &HashSet<&str>,
    infra: ValidatedInfra<ES, SS, DL, RT, SP, OS, OP, CS, PB>,
    debug_handler: Option<DebugEndpointHandler<ES, SS, CmdS>>,
    observability_handler: BoxedObservabilityHandler<CS>,
    admin_handler: Option<AdminHandler<InMemoryProjectionRebuildManager>>,
    debug_enabled: bool,
    snapshot_every: u64,
    health_checks: Vec<Box<dyn HealthCheck>>,
) -> Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
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
    let event_store_consumer = EventStoreConsumer::new(
        infra.event_store,
        infra.snapshot_store,
        infra.dead_letter_store,
        infra.retry_tracker,
        infra.snapshot_state_provider,
        EventStoreConsumerConfig {
            snapshot_every,
            ..Default::default()
        },
    );

    let outbox_processor = OutboxProcessor::new(
        infra.outbox_store,
        infra.outbox_publisher,
        OutboxProcessorConfig::default(),
    );

    let projection_consumer = ProjectionConsumer::new(infra.projection_checkpoint_store);
    let publisher_consumer = PublisherConsumer::new(infra.publisher, &infra.topic);

    let admin_enabled = admin_handler.is_some();
    let health_check_count = health_checks.len();
    let health_checker = HealthChecker::new(health_checks);

    tracing::info!(
        service = %service_name,
        aggregates = ?aggregate_names,
        commands = infra.registrations.commands.len(),
        events = infra.registrations.events.len(),
        topic = %infra.topic,
        debug_enabled = debug_enabled,
        observability_enabled = true,
        admin_enabled = admin_enabled,
        health_checks = health_check_count,
        "service built successfully"
    );

    Service {
        service_name,
        event_store_consumer,
        outbox_processor,
        projection_consumer,
        publisher_consumer,
        debug_handler,
        observability_handler,
        admin_handler,
        health_checker,
    }
}

/// Build an admin handler from optional components.
///
/// Returns `Some(AdminHandler)` when debug/admin endpoints are enabled and all
/// three components (inbox, retry tracker, rebuild manager) are provided.
fn build_admin_handler(
    debug_enabled: bool,
    inbox: Option<InMemoryInbox>,
    retry_tracker: Option<InMemoryRetryTracker>,
    rebuild_manager: Option<InMemoryProjectionRebuildManager>,
) -> Option<AdminHandler<InMemoryProjectionRebuildManager>> {
    if !debug_enabled {
        return None;
    }

    match (inbox, retry_tracker, rebuild_manager) {
        (Some(inbox), Some(tracker), Some(manager)) => {
            tracing::info!("admin endpoints enabled");
            Some(AdminHandler::new(tracker, inbox, manager))
        }
        _ => {
            tracing::warn!(
                "admin endpoints enabled but not all admin components provided \
                 (inbox, retry_tracker, rebuild_manager) — admin handler will not be available"
            );
            None
        }
    }
}

// ── Service (full infrastructure, with command store) ─────────────────────

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
    ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
where
    ES: EventStore + Clone,
    SS: SnapshotStore + Clone,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore + Clone,
    PB: Publisher,
    CmdS: CommandStore,
{
    /// Validate all registrations and build the `Service`.
    ///
    /// Returns `Err` if any commands are missing handlers or events are
    /// missing combiners, or if a required infrastructure component was
    /// not provided.
    ///
    /// When debug endpoints are enabled (via [`debug_endpoints(true)`](ServiceBuilder::debug_endpoints)
    /// or `CANON_DEBUG_ENDPOINTS=true`), the resulting [`Service`] will expose
    /// a [`DebugEndpointHandler`] via [`Service::debug_handler()`].
    ///
    /// The observability handler is always auto-created and accessible via
    /// [`Service::observability_handler()`].
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>, ServiceBuilderError> {
        let infra = extract_infra(
            &self.service_name,
            &self.aggregate_names,
            self.event_store,
            self.snapshot_store,
            self.dead_letter_store,
            self.retry_tracker,
            self.snapshot_state_provider,
            self.outbox_store,
            self.outbox_publisher,
            self.projection_checkpoint_store,
            self.publisher,
            self.topic,
        )?;

        let debug_enabled = resolve_debug_enabled(self.debug_endpoints);
        let debug_handler = if debug_enabled {
            if let Some(cmd_store) = self.command_store {
                tracing::info!("debug endpoints enabled");
                Some(DebugEndpointHandler::new(
                    infra.event_store.clone(),
                    infra.snapshot_store.clone(),
                    cmd_store,
                ))
            } else {
                tracing::warn!(
                    "debug endpoints enabled but no command store provided — \
                     debug handler will not be available"
                );
                None
            }
        } else {
            None
        };

        let observability_handler = build_observability_handler(
            &infra.projection_checkpoint_store,
            self.infra_status_provider,
            self.outbox_status_provider,
        );

        let admin_handler = build_admin_handler(
            debug_enabled,
            self.admin_inbox,
            self.admin_retry_tracker,
            self.admin_rebuild_manager,
        );

        Ok(assemble_service(
            self.service_name,
            &self.aggregate_names,
            infra,
            debug_handler,
            observability_handler,
            admin_handler,
            debug_enabled,
            self.snapshot_every,
            self.health_checks,
        ))
    }
}

// ── Service (full infrastructure, no command store) ───────────────────────

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB> ServiceBuilder<ES, SS, DL, RT, SP, OS, OP, CS, PB, ()>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore + Clone,
    PB: Publisher,
{
    /// Validate all registrations and build the `Service` without a command store.
    ///
    /// Debug endpoints are not available because no command store was provided.
    /// To enable debug endpoints, call `.command_store(store)` before `.build()`.
    ///
    /// The observability handler is always auto-created and accessible via
    /// [`Service::observability_handler()`].
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, ()>, ServiceBuilderError> {
        let infra = extract_infra(
            &self.service_name,
            &self.aggregate_names,
            self.event_store,
            self.snapshot_store,
            self.dead_letter_store,
            self.retry_tracker,
            self.snapshot_state_provider,
            self.outbox_store,
            self.outbox_publisher,
            self.projection_checkpoint_store,
            self.publisher,
            self.topic,
        )?;

        let debug_enabled = resolve_debug_enabled(self.debug_endpoints);
        if debug_enabled {
            tracing::warn!(
                "debug endpoints enabled but no command store provided — \
                 debug handler will not be available"
            );
        }

        let observability_handler = build_observability_handler(
            &infra.projection_checkpoint_store,
            self.infra_status_provider,
            self.outbox_status_provider,
        );

        let admin_handler = build_admin_handler(
            debug_enabled,
            self.admin_inbox,
            self.admin_retry_tracker,
            self.admin_rebuild_manager,
        );

        Ok(assemble_service(
            self.service_name,
            &self.aggregate_names,
            infra,
            None,
            observability_handler,
            admin_handler,
            false,
            self.snapshot_every,
            self.health_checks,
        ))
    }
}

/// A fully wired Canon service, ready to process commands and events.
///
/// Created by `ServiceBuilder::build()` after successful validation.
/// Contains all the runtime processors: outbox processor, event store
/// consumer, projection consumer, and publisher consumer.
///
/// When debug endpoints are enabled, also contains a [`DebugEndpointHandler`]
/// accessible via [`Service::debug_handler()`].
///
/// The [`ObservabilityHandler`] is always auto-created and accessible via
/// [`Service::observability_handler()`]. It provides:
/// - `get_infra_status()` — infrastructure connectivity and latency
/// - `get_projection_status()` — projection checkpoint state and lag
/// - `get_outbox_status()` — outbox health metrics
///
/// Always contains a [`HealthChecker`] accessible via [`Service::health_checks()`]
/// for Kubernetes health probe integration.
pub struct Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS = ()>
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
    debug_handler: Option<DebugEndpointHandler<ES, SS, CmdS>>,
    observability_handler: BoxedObservabilityHandler<CS>,
    admin_handler: Option<AdminHandler<InMemoryProjectionRebuildManager>>,
    health_checker: HealthChecker,
}

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS> Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
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

    /// Returns a reference to the debug endpoint handler, if debug endpoints
    /// were enabled and a command store was provided during building.
    ///
    /// The returned handler provides:
    /// - `get_aggregate()` — hydrate and return aggregate state as JSON
    /// - `get_events()` — return event history as JSON
    /// - `get_commands()` — return command history as JSON
    ///
    /// Wire these into your web framework of choice:
    /// ```ignore
    /// if let Some(debug) = service.debug_handler() {
    ///     let router = axum::Router::new()
    ///         .route("/debug/aggregate", get(/* use debug.get_aggregate() */))
    ///         .route("/debug/events", get(/* use debug.get_events() */))
    ///         .route("/debug/commands", get(/* use debug.get_commands() */));
    /// }
    /// ```
    pub fn debug_handler(&self) -> Option<&DebugEndpointHandler<ES, SS, CmdS>> {
        self.debug_handler.as_ref()
    }

    /// Returns a reference to the observability handler.
    ///
    /// The observability handler is always auto-created by [`ServiceBuilder::build()`].
    /// It provides:
    /// - `get_infra_status()` — infrastructure connectivity and latency
    /// - `get_projection_status()` — projection checkpoint state and lag
    /// - `get_outbox_status()` — outbox health metrics
    ///
    /// Wire these into your web framework of choice:
    /// ```ignore
    /// let obs = service.observability_handler();
    /// let router = axum::Router::new()
    ///     .route("/debug/infra", get(/* use obs.get_infra_status() */))
    ///     .route("/debug/projections", get(/* use obs.get_projection_status() */))
    ///     .route("/debug/outbox", get(/* use obs.get_outbox_status() */));
    /// ```
    pub fn observability_handler(&self) -> &BoxedObservabilityHandler<CS> {
        &self.observability_handler
    }

    /// Returns a reference to the admin endpoint handler, if admin endpoints
    /// were enabled and all required components (inbox, retry tracker,
    /// rebuild manager) were provided during building.
    ///
    /// The returned handler provides:
    /// - `list_retries()` — list messages in retry state with attempt counts
    /// - `list_inbox_windows(filter)` — list inbox windows with optional filters
    /// - `trigger_rebuild(projection_id)` — trigger a projection rebuild
    ///
    /// Wire these into your web framework of choice:
    /// ```ignore
    /// if let Some(admin) = service.admin_handler() {
    ///     let router = axum::Router::new()
    ///         .route("/debug/retries", get(/* use admin.list_retries() */))
    ///         .route("/debug/inbox/windows", get(/* use admin.list_inbox_windows() */))
    ///         .route("/debug/projections/:id/rebuild", post(/* use admin.trigger_rebuild() */));
    /// }
    /// ```
    pub fn admin_handler(&self) -> Option<&AdminHandler<InMemoryProjectionRebuildManager>> {
        self.admin_handler.as_ref()
    }

    /// Returns a reference to the health checker for Kubernetes health probes.
    ///
    /// The [`HealthChecker`] provides:
    /// - `liveness()` — always returns OK (process is running)
    /// - `readiness()` — checks all registered infrastructure dependencies
    /// - `startup()` — same as readiness
    ///
    /// Wire these into your web framework of choice:
    /// ```ignore
    /// let checker = service.health_checks().clone();
    /// let router = axum::Router::new()
    ///     .route("/health/live", get(|| async move { Json(checker.liveness()) }))
    ///     .route("/health/ready", get(|| async move { Json(checker.readiness().await) }))
    ///     .route("/health/startup", get(|| async move { Json(checker.startup().await) }));
    /// ```
    pub fn health_checks(&self) -> &HealthChecker {
        &self.health_checker
    }

    /// Start all background tasks: outbox processor, event store consumer,
    /// projection consumer, and publisher consumer.
    ///
    /// Each consumer registers an independent handle on the outbound queue
    /// so every event is delivered to all three consumer groups. The outbox
    /// processor drains the outbox store and publishes events to the outbound
    /// queue.
    ///
    /// All tasks run until the `shutdown` signal is sent (`true`). The method
    /// consumes the service (moving each component into its own tokio task)
    /// and awaits all tasks to completion. Returns `Ok(())` if every task
    /// finished cleanly.
    ///
    /// `outbox_notify` is an optional receiver for backpressure between the
    /// command handler write path and the outbox processor. Pass `None` to
    /// rely on periodic polling only.
    ///
    /// The `poll_interval_ms` controls how frequently each consumer loop
    /// checks the outbound queue when it is empty.
    pub async fn start(
        self,
        outbound_queue: crate::memory::InMemoryOutboundQueue,
        shutdown: tokio::sync::watch::Receiver<bool>,
        outbox_notify: Option<crate::outbox::OutboxNotifyReceiver>,
        poll_interval_ms: u64,
    ) -> Result<(), ServiceStartError>
    where
        SP: 'static,
    {
        // Register consumer handles for each of the three consumer groups.
        let es_handle = outbound_queue
            .register_consumer()
            .map_err(|e| ServiceStartError::Setup(format!("event store consumer: {e}")))?;
        let proj_handle = outbound_queue
            .register_consumer()
            .map_err(|e| ServiceStartError::Setup(format!("projection consumer: {e}")))?;
        let pub_handle = outbound_queue
            .register_consumer()
            .map_err(|e| ServiceStartError::Setup(format!("publisher consumer: {e}")))?;

        let poll_interval = std::time::Duration::from_millis(poll_interval_ms);

        // Destructure the service to move each component into its own task.
        let service_name = self.service_name;
        let outbox_processor = self.outbox_processor;
        let event_store_consumer = self.event_store_consumer;
        let projection_consumer = self.projection_consumer;
        let publisher_consumer = self.publisher_consumer;

        // Spawn the outbox processor.
        let outbox_shutdown = shutdown.clone();
        let outbox_handle = tokio::spawn(async move {
            outbox_processor
                .run(outbox_shutdown, outbox_notify, |e| {
                    tracing::error!(error = %e, "outbox processor error");
                })
                .await
                .map_err(|e| ServiceStartError::Runtime(format!("outbox processor: {e}")))
        });

        // Spawn the event store consumer loop.
        let mut es_shutdown = shutdown.clone();
        let es_oq = outbound_queue.clone();
        let es_task_handle = tokio::spawn(async move {
            loop {
                if *es_shutdown.borrow() {
                    return Ok::<(), ServiceStartError>(());
                }
                match es_oq.receive(&es_handle) {
                    Ok(Some(envelope)) => {
                        if let Err(e) = event_store_consumer.process(envelope).await {
                            tracing::warn!(error = %e, "event store consumer: processing error");
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => {
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = es_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "event store consumer: receive error");
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = es_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        });

        // Global sequence counter for projection consumer checkpointing.
        let sequence_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));

        // Spawn the projection consumer loop.
        let mut proj_shutdown = shutdown.clone();
        let proj_oq = outbound_queue.clone();
        let seq = sequence_counter;
        let proj_task_handle = tokio::spawn(async move {
            loop {
                if *proj_shutdown.borrow() {
                    return Ok::<(), ServiceStartError>(());
                }
                match proj_oq.receive(&proj_handle) {
                    Ok(Some(envelope)) => {
                        let seq_num = seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if let Err(e) = projection_consumer.process(&envelope, seq_num).await {
                            tracing::warn!(error = %e, "projection consumer: processing error");
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => {
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = proj_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "projection consumer: receive error");
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = proj_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        });

        // Spawn the publisher consumer loop.
        let mut pub_shutdown = shutdown.clone();
        let pub_oq = outbound_queue.clone();
        let pub_task_handle = tokio::spawn(async move {
            loop {
                if *pub_shutdown.borrow() {
                    return Ok::<(), ServiceStartError>(());
                }
                match pub_oq.receive(&pub_handle) {
                    Ok(Some(envelope)) => {
                        if let Err(e) = publisher_consumer.process(&envelope).await {
                            tracing::warn!(error = %e, "publisher consumer: processing error");
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => {
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = pub_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "publisher consumer: receive error");
                        tokio::select! {
                            _ = tokio::time::sleep(poll_interval) => {}
                            _ = pub_shutdown.changed() => {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        });

        tracing::info!(
            service = %service_name,
            "service started: outbox processor + 3 consumer loops running"
        );

        // Wait for all tasks. If any task panics, propagate the error.
        let (outbox_result, es_result, proj_result, pub_result) = tokio::join!(
            outbox_handle,
            es_task_handle,
            proj_task_handle,
            pub_task_handle,
        );

        outbox_result
            .map_err(|e| ServiceStartError::Runtime(format!("outbox task panicked: {e}")))?
            .map_err(|e| ServiceStartError::Runtime(format!("outbox processor: {e}")))?;
        es_result
            .map_err(|e| {
                ServiceStartError::Runtime(format!("event store consumer task panicked: {e}"))
            })?
            .map_err(|e| ServiceStartError::Runtime(format!("event store consumer: {e}")))?;
        proj_result
            .map_err(|e| {
                ServiceStartError::Runtime(format!("projection consumer task panicked: {e}"))
            })?
            .map_err(|e| ServiceStartError::Runtime(format!("projection consumer: {e}")))?;
        pub_result
            .map_err(|e| {
                ServiceStartError::Runtime(format!("publisher consumer task panicked: {e}"))
            })?
            .map_err(|e| ServiceStartError::Runtime(format!("publisher consumer: {e}")))?;

        tracing::info!(
            service = %service_name,
            "service stopped"
        );

        Ok(())
    }
}

// ── Service::start() — spawns all background processors ───────────────────
//
// Requires `SP: 'static` because the event store consumer is moved into a
// spawned tokio task. All other type parameters already have `'static` via
// their trait supertraits.

impl<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS> Service<ES, SS, DL, RT, SP, OS, OP, CS, PB, CmdS>
where
    ES: EventStore,
    SS: SnapshotStore,
    DL: DeadLetterStore,
    RT: RetryTracker,
    SP: SnapshotStateProvider + 'static,
    OS: OutboxStore,
    OP: OutboxPublisher,
    CS: ProjectionCheckpointStore,
    PB: Publisher,
{
    /// Start all background processors as concurrent tokio tasks.
    ///
    /// Consumes the `Service`, destructuring it into its component processors
    /// so that each can be moved into its own spawned task.
    ///
    /// Spawns four loops:
    /// 1. **Outbox processor** — drains outbox → outbound queue
    /// 2. **Event store consumer** — outbound queue → event store + snapshots
    /// 3. **Projection consumer** — outbound queue → read-model projections
    /// 4. **Publisher consumer** — outbound queue → external topic
    ///
    /// All tasks share the same `shutdown` watch channel. When the sender
    /// sends `true`, every loop drains its current work item and exits.
    /// This method returns when the first task exits (the others continue
    /// draining in background).
    ///
    /// # Arguments
    ///
    /// * `shutdown` — shared shutdown watch receiver. Clone for each task.
    /// * `outbox_notify` — optional notification channel for the outbox processor.
    /// * `es_receiver` — consumer receiver for the event store consumer.
    /// * `proj_receiver` — consumer receiver for the projection consumer.
    /// * `pub_receiver` — consumer receiver for the publisher consumer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    ///
    /// let queue = InMemoryOutboundQueue::new();
    /// let es_rx = InMemoryConsumerReceiver::new(queue.clone()).unwrap();
    /// let proj_rx = InMemoryConsumerReceiver::new(queue.clone()).unwrap();
    /// let pub_rx = InMemoryConsumerReceiver::new(queue).unwrap();
    ///
    /// service.start(shutdown_rx, None, es_rx, proj_rx, pub_rx).await;
    /// ```
    pub async fn start<CR1, CR2, CR3>(
        self,
        shutdown: tokio::sync::watch::Receiver<bool>,
        outbox_notify: Option<crate::outbox::OutboxNotifyReceiver>,
        es_receiver: CR1,
        proj_receiver: CR2,
        pub_receiver: CR3,
    ) where
        CR1: crate::consumers::ConsumerReceiver,
        CR2: crate::consumers::ConsumerReceiver,
        CR3: crate::consumers::ConsumerReceiver,
    {
        let service_name = self.service_name;

        tracing::info!(
            service = %service_name,
            "starting all background processors"
        );

        // Destructure self so each processor can be moved into its own task.
        let outbox_processor = self.outbox_processor;
        let event_store_consumer = self.event_store_consumer;
        let projection_consumer = self.projection_consumer;
        let publisher_consumer = self.publisher_consumer;

        let outbox_shutdown = shutdown.clone();
        let outbox_handle = tokio::spawn(async move {
            let result = outbox_processor
                .run(outbox_shutdown, outbox_notify, |e| {
                    tracing::error!(error = %e, "outbox processor error");
                })
                .await;
            if let Err(e) = &result {
                tracing::error!(error = %e, "outbox processor exited with error");
            }
        });

        let es_shutdown = shutdown.clone();
        let es_handle = tokio::spawn(async move {
            event_store_consumer
                .run(es_receiver, es_shutdown, |e| {
                    tracing::error!(error = %e, "event store consumer error");
                })
                .await;
        });

        let proj_shutdown = shutdown.clone();
        let proj_handle = tokio::spawn(async move {
            projection_consumer
                .run(proj_receiver, proj_shutdown, |e| {
                    tracing::error!(error = %e, "projection consumer error");
                })
                .await;
        });

        let pub_shutdown = shutdown;
        let pub_handle = tokio::spawn(async move {
            publisher_consumer
                .run(pub_receiver, pub_shutdown, |e| {
                    tracing::error!(error = %e, "publisher consumer error");
                })
                .await;
        });

        // Wait for the first task to exit. In normal operation this only
        // happens after the shutdown signal fires.
        tokio::select! {
            r = outbox_handle => {
                tracing::info!(
                    service = %service_name,
                    result = ?r,
                    "outbox processor exited"
                );
            }
            r = es_handle => {
                tracing::info!(
                    service = %service_name,
                    result = ?r,
                    "event store consumer exited"
                );
            }
            r = proj_handle => {
                tracing::info!(
                    service = %service_name,
                    result = ?r,
                    "projection consumer exited"
                );
            }
            r = pub_handle => {
                tracing::info!(
                    service = %service_name,
                    result = ?r,
                    "publisher consumer exited"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{
        InMemoryCommandStore, InMemoryDeadLetterStore, InMemoryEventStore, InMemoryOutboundQueue,
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
        InMemoryCommandStore,
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
            .command_store(InMemoryCommandStore::new())
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

    #[test]
    fn debug_handler_none_when_not_enabled() {
        let service = make_builder().build().unwrap();
        assert!(service.debug_handler().is_none());
    }

    #[test]
    fn debug_handler_some_when_enabled() {
        let service = make_builder().debug_endpoints(true).build().unwrap();
        assert!(service.debug_handler().is_some());
    }

    #[test]
    fn debug_handler_none_when_explicitly_disabled() {
        let service = make_builder().debug_endpoints(false).build().unwrap();
        assert!(service.debug_handler().is_none());
    }

    #[test]
    fn build_without_command_store_and_debug_enabled() {
        // Build without command_store — debug handler should be None even
        // when debug_endpoints is true, because no command store was provided.
        let service = ServiceBuilder::new("test")
            .event_store(InMemoryEventStore::new())
            .snapshot_store(InMemorySnapshotStore::new())
            .dead_letter_store(InMemoryDeadLetterStore::new())
            .retry_tracker(InMemoryRetryTracker::new())
            .snapshot_state_provider(EventPayloadSnapshotProvider)
            .outbox_store(InMemoryOutboxStore::new())
            .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
            .projection_checkpoint_store(InMemoryProjectionStore::new())
            .publisher(InMemoryPublisher::new())
            .debug_endpoints(true)
            .build()
            .unwrap();
        assert!(service.debug_handler().is_none());
    }

    #[test]
    fn observability_handler_always_present() {
        let service = make_builder().build().unwrap();
        // observability_handler() returns a direct reference (not Option),
        // so if this compiles and does not panic, the handler is present.
        let _handler = service.observability_handler();
    }

    #[test]
    fn observability_handler_present_without_command_store() {
        let service = ServiceBuilder::new("test")
            .event_store(InMemoryEventStore::new())
            .snapshot_store(InMemorySnapshotStore::new())
            .dead_letter_store(InMemoryDeadLetterStore::new())
            .retry_tracker(InMemoryRetryTracker::new())
            .snapshot_state_provider(EventPayloadSnapshotProvider)
            .outbox_store(InMemoryOutboxStore::new())
            .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
            .projection_checkpoint_store(InMemoryProjectionStore::new())
            .publisher(InMemoryPublisher::new())
            .build()
            .unwrap();
        let _handler = service.observability_handler();
    }

    #[tokio::test]
    async fn observability_handler_get_infra_status() {
        let service = make_builder().build().unwrap();
        let handler = service.observability_handler();
        let status = handler.get_infra_status().await.unwrap();
        assert!(status.cassandra.connected);
        assert!(status.yugabyte.connected);
        assert!(status.kafka.connected);
    }

    #[tokio::test]
    async fn observability_handler_get_outbox_status() {
        let service = make_builder().build().unwrap();
        let handler = service.observability_handler();
        let status = handler.get_outbox_status().await.unwrap();
        // Default in-memory provider reports zero pending.
        assert_eq!(status.pending_count, 0);
    }

    #[tokio::test]
    async fn observability_handler_get_projection_status() {
        let service = make_builder().build().unwrap();
        let handler = service.observability_handler();
        // Should not error — returns whatever projections are registered.
        let _statuses = handler.get_projection_status().await.unwrap();
    }

    #[test]
    fn admin_handler_none_when_not_enabled() {
        let service = make_builder().build().unwrap();
        assert!(service.admin_handler().is_none());
    }

    #[test]
    fn admin_handler_some_when_enabled_with_all_components() {
        let projection_store = InMemoryProjectionStore::new();
        let service = make_builder()
            .debug_endpoints(true)
            .admin_inbox(InMemoryInbox::new())
            .admin_retry_tracker(InMemoryRetryTracker::new())
            .admin_rebuild_manager(InMemoryProjectionRebuildManager::new(projection_store))
            .build()
            .unwrap();
        assert!(service.admin_handler().is_some());
    }

    #[test]
    fn admin_handler_none_when_enabled_but_missing_components() {
        // Only inbox provided, missing retry_tracker and rebuild_manager
        let service = make_builder()
            .debug_endpoints(true)
            .admin_inbox(InMemoryInbox::new())
            .build()
            .unwrap();
        assert!(service.admin_handler().is_none());
    }

    #[test]
    fn admin_handler_none_when_explicitly_disabled() {
        let projection_store = InMemoryProjectionStore::new();
        let service = make_builder()
            .debug_endpoints(false)
            .admin_inbox(InMemoryInbox::new())
            .admin_retry_tracker(InMemoryRetryTracker::new())
            .admin_rebuild_manager(InMemoryProjectionRebuildManager::new(projection_store))
            .build()
            .unwrap();
        assert!(service.admin_handler().is_none());
    }

    // Note: missing components are caught at the type level — you cannot call
    // .build() if any infrastructure type parameter is still `()`, since `()`
    // does not implement EventStore, SnapshotStore, etc. This is enforced by
    // the trait bounds on the `build()` impl block.

    // -- Health check tests ---------------------------------------------------

    #[test]
    fn health_checks_always_available() {
        let service = make_builder().build().unwrap();
        let checker = service.health_checks();
        // Liveness always returns ok
        assert_eq!(checker.liveness().status, "ok");
    }

    #[tokio::test]
    async fn health_checks_empty_by_default_is_ready() {
        let service = make_builder().build().unwrap();
        let response = service.health_checks().readiness().await;
        assert_eq!(response.status, "ready");
        assert!(response.checks.is_empty());
    }

    #[tokio::test]
    async fn health_checks_with_in_memory_check() {
        use crate::health::InMemoryHealthCheck;

        let service = make_builder()
            .health_check(Box::new(InMemoryHealthCheck::new("test-store")))
            .build()
            .unwrap();

        let response = service.health_checks().readiness().await;
        assert_eq!(response.status, "ready");
        assert_eq!(response.checks.len(), 1);
        assert_eq!(response.checks["test-store"].status, "ok");
    }

    #[tokio::test]
    async fn health_checks_with_multiple_checks() {
        use crate::health::InMemoryHealthCheck;

        let service = make_builder()
            .health_check(Box::new(InMemoryHealthCheck::new("cassandra")))
            .health_check(Box::new(InMemoryHealthCheck::new("yugabyte")))
            .health_check(Box::new(InMemoryHealthCheck::new("kafka")))
            .build()
            .unwrap();

        let response = service.health_checks().readiness().await;
        assert_eq!(response.status, "ready");
        assert_eq!(response.checks.len(), 3);
    }

    #[test]
    fn health_checks_available_without_command_store() {
        let service = ServiceBuilder::new("test")
            .event_store(InMemoryEventStore::new())
            .snapshot_store(InMemorySnapshotStore::new())
            .dead_letter_store(InMemoryDeadLetterStore::new())
            .retry_tracker(InMemoryRetryTracker::new())
            .snapshot_state_provider(EventPayloadSnapshotProvider)
            .outbox_store(InMemoryOutboxStore::new())
            .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
            .projection_checkpoint_store(InMemoryProjectionStore::new())
            .publisher(InMemoryPublisher::new())
            .build()
            .unwrap();
        assert_eq!(service.health_checks().liveness().status, "ok");
    }
}
