use async_trait::async_trait;

use crate::traits::{CommandStore, CounterfactualReplay, ReplayEventStore};
use crate::{CommandDiff, CounterfactualRequest, CounterfactualResult};

#[derive(Debug, thiserror::Error)]
pub enum CounterfactualReplayError {
    #[error("command store error: {0}")]
    CommandStore(String),

    #[error("replay event store error: {0}")]
    ReplayEventStore(String),

    #[error("branch version {branch_version} exceeds command history length {history_len}")]
    BranchVersionOutOfRange {
        branch_version: u64,
        history_len: usize,
    },
}

/// Counterfactual replay engine, generic over `CommandStore` and `ReplayEventStore`.
///
/// The replay process:
/// 1. Load the full command history for the aggregate from the command store.
/// 2. Substitute the command at `branch_version` with the provided replacement.
/// 3. Diff original vs counterfactual command lists via positional payload comparison.
///
/// **Current limitation**: This implementation performs positional payload diffing
/// only. Full state hydration via `Aggregate::hydrate()` and re-execution of
/// commands through version-matched handlers will be added when the replay engine
/// is wired to macro-generated dispatch (requires the struct to be generic over
/// aggregate and handler types).
///
/// The `ReplayEventStore` is injected and available for that future hydration
/// step. It is separate from the live `EventStore`, ensuring replay never
/// touches the production write path.
#[derive(Clone)]
pub struct DefaultCounterfactualReplay<C: CommandStore, R: ReplayEventStore> {
    pub command_store: C,
    pub replay_event_store: R,
}

impl<C: CommandStore, R: ReplayEventStore> DefaultCounterfactualReplay<C, R> {
    pub fn new(command_store: C, replay_event_store: R) -> Self {
        Self {
            command_store,
            replay_event_store,
        }
    }
}

#[async_trait]
impl<C: CommandStore, R: ReplayEventStore> CounterfactualReplay
    for DefaultCounterfactualReplay<C, R>
{
    type Error = CounterfactualReplayError;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error> {
        // Step 1: Load full command history from the command store.
        let original_commands = self
            .command_store
            .load_for_aggregate(&request.aggregate_id)
            .await
            .map_err(|e| CounterfactualReplayError::CommandStore(e.to_string()))?;

        let branch_idx = request.branch_version.as_u64() as usize;

        // Validate that branch_version is within the command history.
        if branch_idx > original_commands.len() {
            return Err(CounterfactualReplayError::BranchVersionOutOfRange {
                branch_version: request.branch_version.as_u64(),
                history_len: original_commands.len(),
            });
        }

        // Step 2: Build counterfactual command list by substituting at branch_idx.
        // NOTE: Full hydration via Aggregate::hydrate() using events from
        // self.replay_event_store and re-execution through version-matched
        // command handlers will be added when the engine is wired to
        // macro-generated dispatch.
        let mut counterfactual_commands = Vec::with_capacity(original_commands.len());
        for (i, cmd) in original_commands.iter().enumerate() {
            if i == branch_idx {
                counterfactual_commands.push(request.substituted_command.clone());
            } else {
                counterfactual_commands.push(cmd.clone());
            }
        }
        // If branch_idx == original_commands.len(), the substitution is an append.
        if branch_idx == original_commands.len() {
            counterfactual_commands.push(request.substituted_command.clone());
        }

        // Step 3: Diff original vs counterfactual by positional payload comparison.
        // Commands at the same position with identical payloads are "unchanged".
        // Divergent positions produce "removed" (original) and "added" (counterfactual).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::command_store::InMemoryCommandStore;
    use crate::memory::replay_event_store::InMemoryReplayEventStore;
    use crate::{AggregateId, CommandEnvelope, Version};
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_command(aggregate_id: &AggregateId, payload: &[u8]) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::copy_from_slice(payload),
            command_version: 1,
        }
    }

    fn setup_stores() -> (InMemoryCommandStore, InMemoryReplayEventStore) {
        let command_store = InMemoryCommandStore::new();
        let replay_event_store = InMemoryReplayEventStore::new();
        (command_store, replay_event_store)
    }

    #[tokio::test]
    async fn same_payload_produces_unchanged() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        command_store
            .append(make_command(&id, b"place_order"))
            .unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"place_order");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::initial(),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.diff.unchanged.len(), 1);
        assert!(result.diff.added.is_empty());
        assert!(result.diff.removed.is_empty());
    }

    #[tokio::test]
    async fn different_payload_produces_added_and_removed() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        command_store.append(make_command(&id, b"place")).unwrap();
        command_store.append(make_command(&id, b"cancel")).unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"different");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::initial(),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.diff.added.len(), 1);
        assert_eq!(result.diff.removed.len(), 1);
        assert_eq!(result.diff.unchanged.len(), 1);
    }

    #[tokio::test]
    async fn branch_at_end_appends_command() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        command_store
            .append(make_command(&id, b"existing"))
            .unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"appended");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::from_u64(1),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.counterfactual_commands.len(), 2);
        assert_eq!(result.original_commands.len(), 1);
        assert_eq!(result.diff.added.len(), 1);
        assert_eq!(result.diff.unchanged.len(), 1);
        assert!(result.diff.removed.is_empty());
    }

    #[tokio::test]
    async fn branch_beyond_history_returns_error() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"new_cmd");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::from_u64(5),
                substituted_command: substitute,
            })
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CounterfactualReplayError::BranchVersionOutOfRange { .. }
            ),
            "expected BranchVersionOutOfRange, got: {err}"
        );
    }

    #[tokio::test]
    async fn no_change_produces_all_unchanged() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        command_store.append(make_command(&id, b"cmd_a")).unwrap();
        command_store.append(make_command(&id, b"cmd_b")).unwrap();
        command_store.append(make_command(&id, b"cmd_c")).unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);

        // Substitute at index 1 with the same payload as the original
        let substitute = make_command(&id, b"cmd_b");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::from_u64(1),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.diff.unchanged.len(), 3);
        assert!(result.diff.added.is_empty());
        assert!(result.diff.removed.is_empty());
    }

    #[tokio::test]
    async fn replay_uses_replay_event_store_not_live() {
        // Verify that the replay engine accepts a standalone ReplayEventStore.
        // The replay store is empty -- replay succeeds because the current
        // implementation performs positional payload diffing only.
        let command_store = InMemoryCommandStore::new();
        let replay_event_store = InMemoryReplayEventStore::new();
        let id = AggregateId::new();

        command_store.append(make_command(&id, b"cmd_a")).unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"cmd_b");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::initial(),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.diff.added.len(), 1);
        assert_eq!(result.diff.removed.len(), 1);
    }

    #[tokio::test]
    async fn empty_history_with_branch_at_zero_appends() {
        let (command_store, replay_event_store) = setup_stores();
        let id = AggregateId::new();

        let replay = DefaultCounterfactualReplay::new(command_store, replay_event_store);
        let substitute = make_command(&id, b"new_cmd");

        let result = replay
            .replay(CounterfactualRequest {
                aggregate_id: id,
                branch_version: Version::initial(),
                substituted_command: substitute,
            })
            .await
            .unwrap();

        assert_eq!(result.counterfactual_commands.len(), 1);
        assert_eq!(result.diff.added.len(), 1);
        assert!(result.diff.removed.is_empty());
        assert!(result.diff.unchanged.is_empty());
    }
}
