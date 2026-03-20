use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{
    Aggregate, AggregateId, CommandEnvelope, DefaultCounterfactualReplay, EventEnvelope,
    InMemoryAdaptor, InMemoryCommandStore, InMemoryDeadLetterStore, InMemoryEventStore,
    InMemoryInboundQueue, InMemoryInbox, InMemoryOutboundQueue, InMemoryProjectionStore,
    InMemoryPublisher, InMemorySnapshotStore, Snapshot, Version,
};

// ── TestHarness ─────────────────────────────────────────────────────────────

/// Wires all in-memory implementations from `canon-core` together.
/// Provides direct field access for asserting state in tests, plus
/// convenience methods for common test patterns.
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

    // ── Convenience: submit a command ────────────────────────────────────

    /// Store a command in the command store and return its command_id.
    pub fn submit_command(
        &self,
        aggregate_id: &AggregateId,
        command_type: &str,
        payload: &[u8],
    ) -> Uuid {
        let cmd = CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: command_type.to_owned(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::copy_from_slice(payload),
            command_version: 1,
        };
        let id = cmd.command_id;
        self.command_store
            .append(cmd)
            .expect("submit_command: append failed");
        id
    }

    // ── Convenience: append events to event store ────────────────────────

    /// Append events to the event store at the expected version.
    pub fn append_events(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) {
        self.event_store
            .append(aggregate_id, expected_version, events)
            .expect("append_events: append failed");
    }

    // ── Convenience: assert events ──────────────────────────────────────

    /// Load all events for an aggregate from the event store.
    pub fn load_events(&self, aggregate_id: &AggregateId) -> Vec<EventEnvelope> {
        self.event_store
            .load(aggregate_id)
            .expect("load_events: load failed")
    }

    /// Assert the event count for an aggregate.
    pub fn assert_event_count(&self, aggregate_id: &AggregateId, expected: usize) {
        let events = self.load_events(aggregate_id);
        assert_eq!(
            events.len(),
            expected,
            "expected {} events for aggregate {:?}, found {}",
            expected,
            aggregate_id,
            events.len()
        );
    }

    // ── Convenience: assert projection state ────────────────────────────

    /// Get the projection checkpoint version.
    pub fn projection_checkpoint(&self, projection_id: &str) -> Version {
        self.projection_store
            .get_checkpoint(projection_id)
            .expect("projection_checkpoint: get failed")
    }

    /// Assert the projection checkpoint equals an expected version.
    pub fn assert_projection_at(&self, projection_id: &str, expected_version: u64) {
        let actual = self.projection_checkpoint(projection_id);
        assert_eq!(
            actual.as_u64(),
            expected_version,
            "expected projection '{}' at version {}, found {}",
            projection_id,
            expected_version,
            actual.as_u64()
        );
    }

    // ── Convenience: assert outbox / outbound queue ─────────────────────

    /// Publish an event to the outbound queue (simulating outbox processor drain).
    pub fn publish_to_outbound(&self, envelope: EventEnvelope) {
        self.outbound_queue
            .publish(envelope)
            .expect("publish_to_outbound: publish failed");
    }

    /// Publish an event to the publisher (simulating external publish).
    pub fn publish_external(&self, envelope: EventEnvelope, topic: &str) {
        self.publisher
            .publish(envelope, topic)
            .expect("publish_external: publish failed");
    }

    /// Return all externally published events.
    pub fn published_events(&self) -> Vec<(EventEnvelope, String)> {
        self.publisher
            .published_events()
            .expect("published_events: list failed")
    }

    // ── Convenience: assert dead letters ────────────────────────────────

    /// Return all dead letters, optionally filtered by handler_id.
    pub fn dead_letters(&self, handler_id: Option<&str>) -> Vec<canon_core::InMemoryDeadLetter> {
        self.dead_letter_store
            .list(handler_id)
            .expect("dead_letters: list failed")
    }

    /// Assert the dead letter count (optionally for a specific handler).
    pub fn assert_dead_letter_count(&self, handler_id: Option<&str>, expected: usize) {
        let letters = self.dead_letters(handler_id);
        assert_eq!(
            letters.len(),
            expected,
            "expected {} dead letters for handler {:?}, found {}",
            expected,
            handler_id,
            letters.len()
        );
    }

    // ── Convenience: snapshot ───────────────────────────────────────────

    /// Save a snapshot.
    pub fn save_snapshot(&self, snapshot: Snapshot) {
        self.snapshot_store
            .save(snapshot)
            .expect("save_snapshot: save failed");
    }

    /// Load the latest snapshot for an aggregate.
    pub fn load_snapshot(&self, aggregate_id: &AggregateId) -> Option<Snapshot> {
        self.snapshot_store
            .load(aggregate_id)
            .expect("load_snapshot: load failed")
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
