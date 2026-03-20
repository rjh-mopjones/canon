//! Publisher consumer — publishes events to an external Kafka topic for cross-service consumption.
//!
//! Consumes `EventEnvelope` messages from the outbound queue and publishes them
//! to `canon.{service}.events` via the publisher. Other services consume these
//! via `canon-adaptor-kafka` into their inbox.
//!
//! Generic over the `Publisher` trait so the same consumer logic works with both
//! in-memory test impls and production Kafka publishers.

use crate::traits::Publisher;
use crate::EventEnvelope;

/// Errors emitted by the publisher consumer.
#[derive(Debug, thiserror::Error)]
pub enum PublisherConsumerError {
    /// The publisher failed to publish the event.
    #[error("publish error: {0}")]
    Publisher(String),

    /// The topic was not configured.
    #[error("no topic configured for publisher consumer")]
    NoTopic,
}

/// Consumes events from the outbound queue and publishes them to an external topic.
///
/// The topic is configured at construction time (e.g., `canon.fleet.events`).
/// Other services consume from this topic via `canon-adaptor-kafka`.
///
/// Generic over the `Publisher` trait.
pub struct PublisherConsumer<P>
where
    P: Publisher,
{
    publisher: P,
    topic: String,
}

impl<P> PublisherConsumer<P>
where
    P: Publisher,
{
    /// Create a new publisher consumer that publishes to the given topic.
    ///
    /// `topic` should be of the form `canon.{service}.events`.
    pub fn new(publisher: P, topic: impl Into<String>) -> Self {
        let topic = topic.into();
        tracing::info!(
            topic = %topic,
            "publisher consumer: created"
        );
        Self { publisher, topic }
    }

    /// Process a single event envelope by publishing it to the external topic.
    pub async fn process(&self, envelope: &EventEnvelope) -> Result<(), PublisherConsumerError> {
        tracing::debug!(
            event_id = %envelope.event_id,
            aggregate_id = ?envelope.aggregate_id,
            event_type = %envelope.event_type,
            topic = %self.topic,
            "publisher consumer: publishing event"
        );

        self.publisher
            .publish(envelope.clone(), &self.topic)
            .await
            .map_err(|e| PublisherConsumerError::Publisher(e.to_string()))?;

        tracing::debug!(
            event_id = %envelope.event_id,
            topic = %self.topic,
            "publisher consumer: event published"
        );

        Ok(())
    }

    /// Access the underlying publisher (for test assertions).
    pub fn publisher(&self) -> &P {
        &self.publisher
    }

    /// The topic this consumer publishes to.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryPublisher;
    use crate::{AggregateId, Version};
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    fn make_event() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::from_u64(1),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn process_publishes_event_to_configured_topic() {
        let publisher = InMemoryPublisher::new();
        let consumer = PublisherConsumer::new(publisher.clone(), "canon.fleet.events");

        let event = make_event();
        let event_id = event.event_id;
        consumer.process(&event).await.unwrap();

        let published = consumer.publisher().published_events().unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].0.event_id, event_id);
        assert_eq!(published[0].1, "canon.fleet.events");
    }

    #[tokio::test]
    async fn process_publishes_multiple_events() {
        let publisher = InMemoryPublisher::new();
        let consumer = PublisherConsumer::new(publisher.clone(), "canon.cargo.events");

        consumer.process(&make_event()).await.unwrap();
        consumer.process(&make_event()).await.unwrap();
        consumer.process(&make_event()).await.unwrap();

        let published = consumer.publisher().published_events().unwrap();
        assert_eq!(published.len(), 3);
        for (_, topic) in &published {
            assert_eq!(topic, "canon.cargo.events");
        }
    }

    #[tokio::test]
    async fn topic_returns_configured_topic() {
        let consumer = PublisherConsumer::new(InMemoryPublisher::new(), "canon.nav.events");
        assert_eq!(consumer.topic(), "canon.nav.events");
    }

    #[tokio::test]
    async fn different_consumers_publish_to_different_topics() {
        let publisher = InMemoryPublisher::new();
        let fleet_consumer = PublisherConsumer::new(publisher.clone(), "canon.fleet.events");
        let cargo_consumer = PublisherConsumer::new(publisher.clone(), "canon.cargo.events");

        fleet_consumer.process(&make_event()).await.unwrap();
        cargo_consumer.process(&make_event()).await.unwrap();

        let published = publisher.published_events().unwrap();
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].1, "canon.fleet.events");
        assert_eq!(published[1].1, "canon.cargo.events");
    }
}
