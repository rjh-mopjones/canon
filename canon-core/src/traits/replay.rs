use crate::{AggregateId, CounterfactualRequest, CounterfactualResult, EventEnvelope, Version};
use async_trait::async_trait;

/// Runs a what-if simulation. Substitutes a command at a branch point,
/// re-runs the full handler chain in dry-run mode (no writes), diffs
/// resulting downstream commands against the originals.
///
/// The replay engine operates on **commands, not events**:
/// 1. Loads command history for the aggregate from the command store.
/// 2. Hydrates aggregate state to the branch point using events from
///    `ReplayEventStore` (a read replica, separate from the live event store).
/// 3. Substitutes the specified command at the branch point.
/// 4. Re-runs command handlers forward from the branch point.
/// 5. Diffs original vs counterfactual commands via `CommandDiff`.
#[async_trait]
pub trait CounterfactualReplay: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error>;
}

/// Read-only event store pointing at a read replica. Provides the same
/// read interface as `EventStore` but is injected independently via
/// `ServiceBuilder`. This isolation ensures counterfactual replay never
/// reads from or writes to the live event store.
///
/// The `ReplayEventStore` is used exclusively by the counterfactual replay
/// engine to hydrate aggregate state up to a branch point.
#[async_trait]
pub trait ReplayEventStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Load all events for an aggregate in ascending version order.
    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error>;

    /// Load events for an aggregate where version >= `from_version`.
    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error>;
}
