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

    /// Run the consumer loop. Receives events from the given
    /// [`ConsumerReceiver`](super::ConsumerReceiver), processes each one via
    /// [`Self::process`], commits offsets, and stops when `shutdown` fires.
    ///
    /// On receive or commit errors the `on_error` callback is invoked and the
    /// loop sleeps briefly before retrying.
    pub async fn run<R, F>(
        self,
        receiver: R,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        on_error: F,
    ) where
        R: super::ConsumerReceiver,
        F: Fn(&PublisherConsumerError) + Send + Sync,
    {
        loop {
            if *shutdown.borrow() {
                return;
            }

            let received = tokio::select! {
                r = receiver.receive() => r,
                _ = shutdown.changed() => return,
            };

            match received {
                Ok(Some(re)) => {
                    if let Err(e) = self.process(&re.envelope).await {
                        on_error(&e);
                    }
                    if let Err(commit_err) = receiver.commit().await {
                        tracing::warn!(error = %commit_err, "publisher consumer: commit failed");
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "publisher consumer: receive error");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumers::{ConsumerReceiver, ConsumerReceiverError, ReceivedEnvelope};
    use crate::memory::InMemoryPublisher;
    use crate::{AggregateId, Version};
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::Utc;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    struct MockReceiver {
        events: Arc<Mutex<VecDeque<ReceivedEnvelope>>>,
        committed: Arc<AtomicU32>,
    }

    impl MockReceiver {
        fn new(events: Vec<ReceivedEnvelope>) -> Self {
            Self {
                events: Arc::new(Mutex::new(VecDeque::from(events))),
                committed: Arc::new(AtomicU32::new(0)),
            }
        }

        #[allow(dead_code)]
        fn committed_count(&self) -> u32 {
            self.committed.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ConsumerReceiver for MockReceiver {
        async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
            let mut events = self.events.lock().unwrap();
            Ok(events.pop_front())
        }
        async fn commit(&self) -> Result<(), ConsumerReceiverError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

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

    #[tokio::test]
    async fn run_processes_and_commits() {
        let publisher = InMemoryPublisher::new();
        let consumer = PublisherConsumer::new(publisher.clone(), "canon.test.events");

        let events = vec![ReceivedEnvelope {
            envelope: make_event(),
            sequence_number: 1,
        }];

        let receiver = MockReceiver::new(events);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        assert_eq!(publisher.published_events().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_stops_on_immediate_shutdown() {
        let consumer = PublisherConsumer::new(InMemoryPublisher::new(), "canon.test.events");
        let receiver = MockReceiver::new(vec![]);
        let (_tx, shutdown_rx) = tokio::sync::watch::channel(true);

        consumer.run(receiver, shutdown_rx, |_| {}).await;
    }

    #[tokio::test]
    async fn run_handles_receive_error() {
        let consumer = PublisherConsumer::new(InMemoryPublisher::new(), "canon.test.events");

        struct FailingReceiver;
        #[async_trait]
        impl ConsumerReceiver for FailingReceiver {
            async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
                Err(ConsumerReceiverError::Receive("fail".into()))
            }
            async fn commit(&self) -> Result<(), ConsumerReceiverError> {
                Ok(())
            }
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(FailingReceiver, shutdown_rx, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn run_commit_error_does_not_stop_loop() {
        struct FailingCommitReceiver {
            events: Arc<Mutex<VecDeque<ReceivedEnvelope>>>,
        }

        #[async_trait]
        impl ConsumerReceiver for FailingCommitReceiver {
            async fn receive(&self) -> Result<Option<ReceivedEnvelope>, ConsumerReceiverError> {
                let mut events = self.events.lock().unwrap();
                Ok(events.pop_front())
            }
            async fn commit(&self) -> Result<(), ConsumerReceiverError> {
                Err(ConsumerReceiverError::Commit("commit failed".into()))
            }
        }

        let publisher = InMemoryPublisher::new();
        let consumer = PublisherConsumer::new(publisher.clone(), "canon.test.events");

        let receiver = FailingCommitReceiver {
            events: Arc::new(Mutex::new(VecDeque::from(vec![ReceivedEnvelope {
                envelope: make_event(),
                sequence_number: 1,
            }]))),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(receiver, shutdown_rx, |_| {}).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        // The event should have been published despite commit error
        assert_eq!(publisher.published_events().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn run_calls_on_error_for_publish_failure() {
        // A publisher that always fails
        struct FailingPublisher;

        #[async_trait]
        impl Publisher for FailingPublisher {
            type Error = PublisherConsumerError;

            async fn publish(
                &self,
                _event: EventEnvelope,
                _topic: &str,
            ) -> Result<(), Self::Error> {
                Err(PublisherConsumerError::Publisher("publish failed".into()))
            }
        }

        let consumer = PublisherConsumer::new(FailingPublisher, "canon.test.events");
        let receiver = MockReceiver::new(vec![ReceivedEnvelope {
            envelope: make_event(),
            sequence_number: 1,
        }]);

        let error_count = Arc::new(AtomicU32::new(0));
        let error_count_clone = error_count.clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer
                .run(receiver, shutdown_rx, move |_| {
                    error_count_clone.fetch_add(1, Ordering::SeqCst);
                })
                .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(true);
        handle.await.unwrap();

        assert!(error_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn error_display_messages() {
        let err = PublisherConsumerError::Publisher("kafka down".into());
        assert!(err.to_string().contains("publish error"));
        assert!(err.to_string().contains("kafka down"));

        let err = PublisherConsumerError::NoTopic;
        assert!(err.to_string().contains("no topic"));
    }
}
