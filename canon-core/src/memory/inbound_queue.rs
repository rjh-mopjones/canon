use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::IncomingMessage;

#[derive(Debug, thiserror::Error)]
pub enum InboundQueueError {
    #[error("internal lock error")]
    Poisoned,
}

/// Carries assembled `IncomingMessage` batches from the inbox to handlers.
#[derive(Clone)]
pub struct InMemoryInboundQueue {
    inner: Arc<Mutex<VecDeque<Vec<IncomingMessage>>>>,
}

impl InMemoryInboundQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Push a batch of messages onto the back of the queue.
    pub fn publish(&self, batch: Vec<IncomingMessage>) -> Result<(), InboundQueueError> {
        let mut queue = self.inner.lock().map_err(|_| InboundQueueError::Poisoned)?;
        queue.push_back(batch);
        Ok(())
    }

    /// Pop a batch from the front of the queue, or None if empty.
    pub fn receive(&self) -> Result<Option<Vec<IncomingMessage>>, InboundQueueError> {
        let mut queue = self.inner.lock().map_err(|_| InboundQueueError::Poisoned)?;
        Ok(queue.pop_front())
    }
}

impl Default for InMemoryInboundQueue {
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
    use crate::{AggregateId, CommandEnvelope};

    fn make_batch(size: usize) -> Vec<IncomingMessage> {
        let id = AggregateId::new();
        (0..size)
            .map(|_| {
                IncomingMessage::Command(CommandEnvelope {
                    command_id: Uuid::new_v4(),
                    aggregate_id: id.clone(),
                    correlation_id: Uuid::new_v4(),
                    causation_id: Uuid::new_v4(),
                    timestamp: Utc::now(),
                    payload: Bytes::from_static(b"{}"),
                })
            })
            .collect()
    }

    #[test]
    fn fifo_ordering() {
        let queue = InMemoryInboundQueue::new();
        queue.publish(make_batch(1)).unwrap();
        queue.publish(make_batch(2)).unwrap();

        let first = queue.receive().unwrap().unwrap();
        assert_eq!(first.len(), 1);
        let second = queue.receive().unwrap().unwrap();
        assert_eq!(second.len(), 2);
        assert!(queue.receive().unwrap().is_none());
    }
}
