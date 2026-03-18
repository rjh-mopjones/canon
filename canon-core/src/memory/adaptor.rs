use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::EventEnvelope;

#[derive(Debug, thiserror::Error)]
pub enum AdaptorError {
    #[error("lock poisoned")]
    Poisoned,
}

#[derive(Clone)]
pub struct InMemoryAdaptor {
    inner: Arc<Mutex<VecDeque<EventEnvelope>>>,
}

impl InMemoryAdaptor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Inject an external event into the adaptor. Used in tests to simulate
    /// events arriving from other services.
    pub fn inject(&self, event: EventEnvelope) -> Result<(), AdaptorError> {
        let mut queue = self.inner.lock().map_err(|_| AdaptorError::Poisoned)?;
        queue.push_back(event);
        Ok(())
    }

    /// Drain the internal queue and return events as a stream.
    pub fn subscribe(
        &self,
    ) -> Result<impl futures::Stream<Item = EventEnvelope>, AdaptorError> {
        let mut queue = self.inner.lock().map_err(|_| AdaptorError::Poisoned)?;
        let events: Vec<EventEnvelope> = queue.drain(..).collect();
        Ok(futures::stream::iter(events))
    }
}

impl Default for InMemoryAdaptor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use futures::StreamExt;
    use uuid::Uuid;
    use crate::{AggregateId, Version};

    fn make_event() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::initial().next(),
            event_type: "ExternalEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn inject_and_subscribe() {
        let adaptor = InMemoryAdaptor::new();
        adaptor.inject(make_event()).unwrap();
        adaptor.inject(make_event()).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let events: Vec<_> = rt.block_on(async {
            let stream = adaptor.subscribe().unwrap();
            stream.collect().await
        });
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn subscribe_drains_queue() {
        let adaptor = InMemoryAdaptor::new();
        adaptor.inject(make_event()).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let events: Vec<_> = rt.block_on(async {
            adaptor.subscribe().unwrap().collect().await
        });
        assert_eq!(events.len(), 1);

        let events: Vec<_> = rt.block_on(async {
            adaptor.subscribe().unwrap().collect().await
        });
        assert!(events.is_empty());
    }
}
