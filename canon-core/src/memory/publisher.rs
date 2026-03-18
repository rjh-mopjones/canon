use std::sync::{Arc, Mutex};

use crate::EventEnvelope;

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemoryPublisher {
    inner: Arc<Mutex<Vec<(EventEnvelope, String)>>>,
}

impl InMemoryPublisher {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Publish an event to a topic. Appends the (envelope, topic) pair.
    pub fn publish(
        &self,
        envelope: EventEnvelope,
        topic: &str,
    ) -> Result<(), PublisherError> {
        let mut store = self.inner.lock().map_err(|_| PublisherError::Poisoned)?;
        store.push((envelope, topic.to_owned()));
        Ok(())
    }

    /// Return a clone of all published events. Used for test assertions.
    pub fn published_events(&self) -> Result<Vec<(EventEnvelope, String)>, PublisherError> {
        let store = self.inner.lock().map_err(|_| PublisherError::Poisoned)?;
        Ok(store.clone())
    }
}

impl Default for InMemoryPublisher {
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
    fn publish_and_read_back() {
        let publisher = InMemoryPublisher::new();
        publisher.publish(make_event(), "topic-a").unwrap();
        publisher.publish(make_event(), "topic-b").unwrap();

        let events = publisher.published_events().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].1, "topic-a");
        assert_eq!(events[1].1, "topic-b");
    }
}
