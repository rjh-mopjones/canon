use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::EventEnvelope;

type ConsumerQueue = Arc<Mutex<VecDeque<EventEnvelope>>>;

#[derive(Debug, thiserror::Error)]
pub enum OutboundQueueError {
    #[error("internal lock error")]
    Poisoned,
}

/// Handle returned by `register_consumer` that identifies a specific consumer's
/// queue within the outbound queue.
#[derive(Clone)]
pub struct ConsumerHandle {
    queue: ConsumerQueue,
}

/// Carries committed `EventEnvelope` events from the outbox processor to
/// downstream consumers. Supports multiple independent consumer groups.
#[derive(Clone)]
pub struct InMemoryOutboundQueue {
    consumers: Arc<Mutex<Vec<ConsumerQueue>>>,
}

impl InMemoryOutboundQueue {
    pub fn new() -> Self {
        Self {
            consumers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a new consumer. Returns a handle the caller uses to receive
    /// events independently from other consumers.
    pub fn register_consumer(&self) -> Result<ConsumerHandle, OutboundQueueError> {
        let mut consumers = self.consumers.lock().map_err(|_| OutboundQueueError::Poisoned)?;
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        consumers.push(Arc::clone(&queue));
        Ok(ConsumerHandle { queue })
    }

    /// Publish an event to all registered consumer queues.
    pub fn publish(&self, envelope: EventEnvelope) -> Result<(), OutboundQueueError> {
        let consumers = self.consumers.lock().map_err(|_| OutboundQueueError::Poisoned)?;
        for consumer_queue in consumers.iter() {
            let mut q = consumer_queue.lock().map_err(|_| OutboundQueueError::Poisoned)?;
            q.push_back(envelope.clone());
        }
        Ok(())
    }

    /// Pop the next event from a specific consumer's queue.
    pub fn receive(&self, handle: &ConsumerHandle) -> Result<Option<EventEnvelope>, OutboundQueueError> {
        let mut queue = handle.queue.lock().map_err(|_| OutboundQueueError::Poisoned)?;
        Ok(queue.pop_front())
    }
}

impl Default for InMemoryOutboundQueue {
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
    use crate::{AggregateId, Version};

    fn make_event() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn publish_delivers_to_all_registered_consumers() {
        let queue = InMemoryOutboundQueue::new();
        let consumer_a = queue.register_consumer().unwrap();
        let consumer_b = queue.register_consumer().unwrap();

        let event = make_event();
        let event_id = event.event_id;
        queue.publish(event).unwrap();

        let received_a = queue.receive(&consumer_a).unwrap().unwrap();
        let received_b = queue.receive(&consumer_b).unwrap().unwrap();
        assert_eq!(received_a.event_id, event_id);
        assert_eq!(received_b.event_id, event_id);
    }

    #[test]
    fn consumers_receive_independently() {
        let queue = InMemoryOutboundQueue::new();
        let consumer_a = queue.register_consumer().unwrap();
        let consumer_b = queue.register_consumer().unwrap();

        let event1 = make_event();
        let event2 = make_event();
        let id1 = event1.event_id;
        let id2 = event2.event_id;
        queue.publish(event1).unwrap();
        queue.publish(event2).unwrap();

        // Consumer A receives first event
        let a1 = queue.receive(&consumer_a).unwrap().unwrap();
        assert_eq!(a1.event_id, id1);

        // Consumer B still has both events — A's receive did not affect B
        let b1 = queue.receive(&consumer_b).unwrap().unwrap();
        let b2 = queue.receive(&consumer_b).unwrap().unwrap();
        assert_eq!(b1.event_id, id1);
        assert_eq!(b2.event_id, id2);

        // Consumer A can still receive second event
        let a2 = queue.receive(&consumer_a).unwrap().unwrap();
        assert_eq!(a2.event_id, id2);
    }
}
