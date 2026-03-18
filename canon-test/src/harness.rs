use async_trait::async_trait;

use canon_core::{
    CommandDiff, CommandStoreError, CounterfactualReplay, CounterfactualRequest,
    CounterfactualResult, InMemoryAdaptor, InMemoryCommandStore, InMemoryDeadLetterStore,
    InMemoryEventStore, InMemoryInboundQueue, InMemoryInbox, InMemoryOutboundQueue,
    InMemoryProjectionStore, InMemoryPublisher, InMemorySnapshotStore,
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

    /// Create a `TestCounterfactualReplay` from the harness's stores.
    pub fn counterfactual_replay(&self) -> TestCounterfactualReplay {
        TestCounterfactualReplay {
            event_store: self.event_store.clone(),
            command_store: self.command_store.clone(),
        }
    }
}

impl Default for TestHarness {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for `TestHarness`. Currently builds with defaults;
/// provides a place to add configuration in the future.
pub struct TestHarnessBuilder;

impl TestHarnessBuilder {
    pub fn new() -> Self {
        Self
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

// ── TestCounterfactualReplay ────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("event store: {0}")]
    EventStore(String),
    #[error("command store: {0}")]
    CommandStore(String),
}

impl From<CommandStoreError> for ReplayError {
    fn from(e: CommandStoreError) -> Self {
        ReplayError::CommandStore(e.to_string())
    }
}

/// Counterfactual replay implementation for tests.
///
/// Substitutes a single command at `branch_version` index in the command history
/// and computes a `CommandDiff` by comparing payloads position-by-position.
pub struct TestCounterfactualReplay {
    pub event_store: InMemoryEventStore,
    pub command_store: InMemoryCommandStore,
}

#[async_trait]
impl CounterfactualReplay for TestCounterfactualReplay {
    type Error = ReplayError;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error> {
        let original_commands = self
            .command_store
            .load_range(&request.aggregate_id, None, None)?;

        let branch_idx = request.branch_version.as_u64() as usize;

        // Build counterfactual command list: replace command at branch_idx
        let mut counterfactual_commands = Vec::new();
        for (i, cmd) in original_commands.iter().enumerate() {
            if i == branch_idx {
                counterfactual_commands.push(request.substituted_command.clone());
            } else {
                counterfactual_commands.push(cmd.clone());
            }
        }
        if branch_idx >= original_commands.len() {
            counterfactual_commands.push(request.substituted_command.clone());
        }

        // Diff by positional payload comparison
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut unchanged = Vec::new();

        let max_len = original_commands.len().max(counterfactual_commands.len());
        for i in 0..max_len {
            match (original_commands.get(i), counterfactual_commands.get(i)) {
                (Some(orig), Some(cf)) if orig.payload == cf.payload => {
                    unchanged.push(orig.clone());
                }
                (Some(orig), Some(cf)) => {
                    removed.push(orig.clone());
                    added.push(cf.clone());
                }
                (Some(orig), None) => {
                    removed.push(orig.clone());
                }
                (None, Some(cf)) => {
                    added.push(cf.clone());
                }
                (None, None) => {}
            }
        }

        Ok(CounterfactualResult {
            original_commands,
            counterfactual_commands,
            diff: CommandDiff {
                added,
                removed,
                unchanged,
            },
        })
    }
}
