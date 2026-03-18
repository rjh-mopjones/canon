use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::CommandEnvelope;

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemoryQueue {
    inner: Arc<Mutex<VecDeque<CommandEnvelope>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Push a command onto the back of the queue.
    pub fn publish(&self, envelope: CommandEnvelope) -> Result<(), QueueError> {
        let mut queue = self.inner.lock().map_err(|_| QueueError::Poisoned)?;
        queue.push_back(envelope);
        Ok(())
    }

    /// Pop a command from the front of the queue, or None if empty.
    pub fn receive(&self) -> Result<Option<CommandEnvelope>, QueueError> {
        let mut queue = self.inner.lock().map_err(|_| QueueError::Poisoned)?;
        Ok(queue.pop_front())
    }

    /// Acknowledge delivery — no-op in the in-memory impl.
    pub fn ack(&self) -> Result<(), QueueError> {
        Ok(())
    }

    /// Negative acknowledge — no-op in the in-memory impl.
    pub fn nack(&self) -> Result<(), QueueError> {
        Ok(())
    }
}

impl Default for InMemoryQueue {
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
    use crate::AggregateId;

    fn make_command() -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"{}"),
        }
    }

    #[test]
    fn fifo_ordering() {
        let queue = InMemoryQueue::new();
        let cmd1 = make_command();
        let cmd2 = make_command();
        let id1 = cmd1.command_id;
        let id2 = cmd2.command_id;

        queue.publish(cmd1).unwrap();
        queue.publish(cmd2).unwrap();

        assert_eq!(queue.receive().unwrap().unwrap().command_id, id1);
        assert_eq!(queue.receive().unwrap().unwrap().command_id, id2);
        assert!(queue.receive().unwrap().is_none());
    }
}
