use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::traits::inbox_port::{InboxPort, InboxPortError};
use crate::CommandEnvelope;

/// In-memory implementation of [`InboxPort`] for testing.
///
/// Stores submitted commands in a `Vec` behind a `Mutex`. Test code can
/// retrieve them with [`submitted`](InMemoryInboxPort::submitted) to
/// assert that event handlers produced the expected re-entry commands.
#[derive(Clone)]
pub struct InMemoryInboxPort {
    commands: Arc<Mutex<Vec<CommandEnvelope>>>,
}

impl InMemoryInboxPort {
    pub fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a snapshot of all commands submitted so far.
    pub fn submitted(&self) -> Result<Vec<CommandEnvelope>, InboxPortError> {
        let guard = self
            .commands
            .lock()
            .map_err(|_| InboxPortError::SubmitFailed {
                reason: "internal lock poisoned".to_owned(),
            })?;
        Ok(guard.clone())
    }

    /// Drain and return all submitted commands, clearing the internal buffer.
    pub fn drain(&self) -> Result<Vec<CommandEnvelope>, InboxPortError> {
        let mut guard = self
            .commands
            .lock()
            .map_err(|_| InboxPortError::SubmitFailed {
                reason: "internal lock poisoned".to_owned(),
            })?;
        Ok(guard.drain(..).collect())
    }
}

impl Default for InMemoryInboxPort {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl InboxPort for InMemoryInboxPort {
    async fn submit(&self, command: CommandEnvelope) -> Result<(), InboxPortError> {
        let mut guard = self
            .commands
            .lock()
            .map_err(|_| InboxPortError::SubmitFailed {
                reason: "internal lock poisoned".to_owned(),
            })?;
        guard.push(command);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    use crate::AggregateId;

    fn make_command_envelope() -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
            command_version: 1,
        }
    }

    #[tokio::test]
    async fn submit_stores_command() {
        let port = InMemoryInboxPort::new();
        let cmd = make_command_envelope();
        let cmd_id = cmd.command_id;

        port.submit(cmd).await.unwrap();

        let submitted = port.submitted().unwrap();
        assert_eq!(submitted.len(), 1);
        assert_eq!(submitted[0].command_id, cmd_id);
    }

    #[tokio::test]
    async fn submit_multiple_commands() {
        let port = InMemoryInboxPort::new();

        port.submit(make_command_envelope()).await.unwrap();
        port.submit(make_command_envelope()).await.unwrap();
        port.submit(make_command_envelope()).await.unwrap();

        let submitted = port.submitted().unwrap();
        assert_eq!(submitted.len(), 3);
    }

    #[tokio::test]
    async fn drain_clears_buffer() {
        let port = InMemoryInboxPort::new();

        port.submit(make_command_envelope()).await.unwrap();
        port.submit(make_command_envelope()).await.unwrap();

        let drained = port.drain().unwrap();
        assert_eq!(drained.len(), 2);

        let remaining = port.submitted().unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn default_starts_empty() {
        let port = InMemoryInboxPort::default();
        let submitted = port.submitted().unwrap();
        assert!(submitted.is_empty());
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let port = InMemoryInboxPort::new();
        let port_clone = port.clone();

        port.submit(make_command_envelope()).await.unwrap();

        let submitted = port_clone.submitted().unwrap();
        assert_eq!(submitted.len(), 1);
    }
}
