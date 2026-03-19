use canon_core::{
    Aggregate, DefaultCounterfactualReplay, InMemoryAdaptor, InMemoryCommandStore,
    InMemoryDeadLetterStore, InMemoryEventStore, InMemoryInboundQueue, InMemoryInbox,
    InMemoryOutboundQueue, InMemoryProjectionStore, InMemoryPublisher, InMemorySnapshotStore,
};

// ── TestHarness ─────────────────────────────────────────────────────────────

/// Wires all in-memory implementations from `canon-core` together.
/// Provides direct field access for asserting state in tests.
pub struct TestHarness {
    pub event_store: InMemoryEventStore,
    pub command_store: InMemoryCommandStore,
    pub snapshot_store: InMemorySnapshotStore,
    pub inbox: InMemoryInbox,
    pub inbound_queue: InMemoryInboundQueue,
    pub outbound_queue: InMemoryOutboundQueue,
    pub projection_store: InMemoryProjectionStore,
    pub publisher: InMemoryPublisher,
    pub adaptor: InMemoryAdaptor,
    pub dead_letter_store: InMemoryDeadLetterStore,
}

impl TestHarness {
    pub fn new() -> Self {
        Self {
            event_store: InMemoryEventStore::new(),
            command_store: InMemoryCommandStore::new(),
            snapshot_store: InMemorySnapshotStore::new(),
            inbox: InMemoryInbox::new(),
            inbound_queue: InMemoryInboundQueue::new(),
            outbound_queue: InMemoryOutboundQueue::new(),
            projection_store: InMemoryProjectionStore::new(),
            publisher: InMemoryPublisher::new(),
            adaptor: InMemoryAdaptor::new(),
            dead_letter_store: InMemoryDeadLetterStore::new(),
        }
    }

    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder
    }

    /// Create a `DefaultCounterfactualReplay` from the harness's command store.
    pub fn counterfactual_replay(&self) -> DefaultCounterfactualReplay<InMemoryCommandStore> {
        DefaultCounterfactualReplay::new(self.command_store.clone())
    }
}

impl Default for TestHarness {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for `TestHarness`. Supports `for_aggregate::<A>()` for
/// ServiceBuilder-style auto-registration (currently a no-op placeholder
/// until ServiceBuilder is implemented).
pub struct TestHarnessBuilder;

impl TestHarnessBuilder {
    pub fn new() -> Self {
        Self
    }

    /// Register an aggregate type for auto-discovery of its handlers,
    /// combiners, and projections. Currently a no-op — ServiceBuilder
    /// will implement actual inventory-based discovery.
    pub fn for_aggregate<A: Aggregate>(self) -> Self {
        self
    }

    pub fn build(self) -> TestHarness {
        TestHarness::new()
    }
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self::new()
    }
}

