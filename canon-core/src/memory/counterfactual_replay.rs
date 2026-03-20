use async_trait::async_trait;

use crate::traits::{CommandStore, CounterfactualReplay};
use crate::{CommandDiff, CounterfactualRequest, CounterfactualResult};

#[derive(Debug, thiserror::Error)]
pub enum CounterfactualReplayError {
    #[error("command store: {0}")]
    CommandStore(String),
}

/// Default counterfactual replay implementation, generic over any `CommandStore`.
///
/// Substitutes a single command at `branch_version` index in the command history
/// and computes a `CommandDiff` by comparing payloads position-by-position.
#[derive(Clone)]
pub struct DefaultCounterfactualReplay<C: CommandStore> {
    pub command_store: C,
}

impl<C: CommandStore> DefaultCounterfactualReplay<C> {
    pub fn new(command_store: C) -> Self {
        Self { command_store }
    }
}

#[async_trait]
impl<C: CommandStore> CounterfactualReplay for DefaultCounterfactualReplay<C> {
    type Error = CounterfactualReplayError;

    async fn replay(
        &self,
        request: CounterfactualRequest,
    ) -> Result<CounterfactualResult, Self::Error> {
        let original_commands = self
            .command_store
            .load_range(&request.aggregate_id, None, None)
            .await
            .map_err(|e| CounterfactualReplayError::CommandStore(e.to_string()))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::command_store::InMemoryCommandStore;
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

    #[tokio::test]
    async fn same_payload_produces_unchanged() {
        let command_store = InMemoryCommandStore::new();
        let id = AggregateId::new();

        command_store
            .append(make_command(&id, b"place_order"))
            .unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store);
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
        let command_store = InMemoryCommandStore::new();
        let id = AggregateId::new();

        command_store.append(make_command(&id, b"place")).unwrap();
        command_store.append(make_command(&id, b"cancel")).unwrap();

        let replay = DefaultCounterfactualReplay::new(command_store);
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
    async fn branch_beyond_end_appends() {
        let command_store = InMemoryCommandStore::new();
        let id = AggregateId::new();

        let replay = DefaultCounterfactualReplay::new(command_store);
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
