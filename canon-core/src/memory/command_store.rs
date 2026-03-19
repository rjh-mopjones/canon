use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use uuid::Uuid;

use crate::traits::CommandStore;
use crate::{AggregateId, CommandEnvelope, CommandStatus};

#[derive(Debug, thiserror::Error)]
pub enum CommandStoreError {
    #[error("lock poisoned")]
    Poisoned,
}

struct CommandStoreState {
    commands: Vec<CommandEnvelope>,
    statuses: HashMap<Uuid, CommandStatus>,
}

#[derive(Clone)]
pub struct InMemoryCommandStore {
    inner: Arc<Mutex<CommandStoreState>>,
}

impl InMemoryCommandStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CommandStoreState {
                commands: Vec::new(),
                statuses: HashMap::new(),
            })),
        }
    }

    /// Append a command to the audit trail.
    pub fn append(&self, envelope: CommandEnvelope) -> Result<(), CommandStoreError> {
        let mut state = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        state.statuses.insert(envelope.command_id, CommandStatus::Pending);
        state.commands.push(envelope);
        Ok(())
    }

    /// Load a single command by its ID.
    pub fn load(&self, command_id: Uuid) -> Result<Option<CommandEnvelope>, CommandStoreError> {
        let state = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        Ok(state.commands.iter().find(|c| c.command_id == command_id).cloned())
    }

    /// Load all commands for an aggregate, ordered by timestamp.
    pub fn load_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        let state = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        let mut cmds: Vec<_> = state
            .commands
            .iter()
            .filter(|c| c.aggregate_id == *aggregate_id)
            .cloned()
            .collect();
        cmds.sort_by_key(|c| c.timestamp);
        Ok(cmds)
    }

    /// Load commands for an aggregate, optionally filtered by timestamp range.
    pub fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        let state = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        Ok(state
            .commands
            .iter()
            .filter(|c| {
                c.aggregate_id == *aggregate_id
                    && from.is_none_or(|f| c.timestamp >= f)
                    && to.is_none_or(|t| c.timestamp <= t)
            })
            .cloned()
            .collect())
    }

    /// Update the status of a command.
    pub fn update_status(
        &self,
        command_id: Uuid,
        status: CommandStatus,
    ) -> Result<(), CommandStoreError> {
        let mut state = self.inner.lock().map_err(|_| CommandStoreError::Poisoned)?;
        state.statuses.insert(command_id, status);
        Ok(())
    }
}

#[async_trait]
impl CommandStore for InMemoryCommandStore {
    type Error = CommandStoreError;

    async fn append(&self, envelope: CommandEnvelope) -> Result<(), Self::Error> {
        self.append(envelope)
    }

    async fn load(
        &self,
        command_id: Uuid,
    ) -> Result<Option<CommandEnvelope>, Self::Error> {
        self.load(command_id)
    }

    async fn load_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<CommandEnvelope>, Self::Error> {
        self.load_for_aggregate(aggregate_id)
    }

    async fn load_range(
        &self,
        aggregate_id: &AggregateId,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<CommandEnvelope>, Self::Error> {
        self.load_range(aggregate_id, from, to)
    }

    async fn update_status(
        &self,
        command_id: Uuid,
        status: CommandStatus,
    ) -> Result<(), Self::Error> {
        self.update_status(command_id, status)
    }
}

impl Default for InMemoryCommandStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_command(aggregate_id: &AggregateId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        }
    }

    #[test]
    fn append_and_load() {
        let store = InMemoryCommandStore::new();
        let id = AggregateId::new();
        store.append(make_command(&id)).unwrap();
        store.append(make_command(&id)).unwrap();

        let loaded = store.load_range(&id, None, None).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn load_range_filters_by_aggregate() {
        let store = InMemoryCommandStore::new();
        let id_a = AggregateId::new();
        let id_b = AggregateId::new();
        store.append(make_command(&id_a)).unwrap();
        store.append(make_command(&id_b)).unwrap();

        let loaded = store.load_range(&id_a, None, None).unwrap();
        assert_eq!(loaded.len(), 1);
    }

    #[test]
    fn load_by_id() {
        let store = InMemoryCommandStore::new();
        let id = AggregateId::new();
        let cmd = make_command(&id);
        let cmd_id = cmd.command_id;
        store.append(cmd).unwrap();

        let loaded = store.load(cmd_id).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().command_id, cmd_id);
    }

    #[test]
    fn load_for_aggregate_filters_correctly() {
        let store = InMemoryCommandStore::new();
        let id_a = AggregateId::new();
        let id_b = AggregateId::new();
        store.append(make_command(&id_a)).unwrap();
        store.append(make_command(&id_a)).unwrap();
        store.append(make_command(&id_b)).unwrap();

        let loaded = store.load_for_aggregate(&id_a).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn update_status_stores_status() {
        let store = InMemoryCommandStore::new();
        let id = AggregateId::new();
        let cmd = make_command(&id);
        let cmd_id = cmd.command_id;
        store.append(cmd).unwrap();

        store.update_status(cmd_id, CommandStatus::Executed).unwrap();
        let state = store.inner.lock().unwrap();
        assert_eq!(state.statuses.get(&cmd_id), Some(&CommandStatus::Executed));
    }
}
