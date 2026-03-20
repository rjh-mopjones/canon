use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use canon_adaptor::{AdaptorError, EventAdaptor, EventEnvelope};
use canon_core::IncomingMessage;
use canon_inbox::Inbox;

/// Kafka-backed [`EventAdaptor`]. Anti-corruption layer at the service boundary.
///
/// Consumes events from upstream services' `canon.{upstream}.events` topics and
/// submits them to the local inbox as [`IncomingMessage::ExternalEvent`].
pub struct KafkaEventAdaptor<I: Inbox> {
    brokers: String,
    local_service: String,
    inbox: Arc<I>,
}

impl<I: Inbox> KafkaEventAdaptor<I> {
    /// Create a new adaptor.
    ///
    /// # Arguments
    /// - `brokers` — comma-separated Kafka broker list (e.g. from `KAFKA_BROKERS`)
    /// - `local_service` — name of the consuming service (used in consumer group IDs)
    /// - `inbox` — the local inbox to submit external events to
    pub fn new(brokers: &str, local_service: &str, inbox: Arc<I>) -> Self {
        Self {
            brokers: brokers.to_owned(),
            local_service: local_service.to_owned(),
            inbox,
        }
    }

    /// Consume events from an upstream service, forwarding them to the local inbox.
    ///
    /// This is the inbox-forwarding path with offset commit guarantees. Creates a
    /// Kafka consumer for `canon.{upstream_service}.events` with consumer group
    /// `"{local_service}-{handler_id}"`. Spawns a background task that:
    /// 1. Deserialises each message as [`EventEnvelope`]
    /// 2. Submits it to the inbox as [`IncomingMessage::ExternalEvent`]
    /// 3. Commits the offset only after confirmed inbox submission
    ///
    /// For a raw event stream without inbox integration or offset commit
    /// guarantees, use the [`EventAdaptor::subscribe()`] trait method instead.
    ///
    /// Returns a [`JoinHandle`] for the consumer task.
    pub async fn consume_upstream(
        &self,
        upstream_service: &str,
        handler_id: &str,
    ) -> Result<JoinHandle<()>, AdaptorError> {
        let topic = format!("canon.{upstream_service}.events");
        let group_id = format!("{}-{handler_id}", self.local_service);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        consumer
            .subscribe(&[&topic])
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        info!(
            topic = %topic,
            group_id = %group_id,
            "subscribed to upstream events"
        );

        let inbox = Arc::clone(&self.inbox);
        let handler_id = handler_id.to_owned();
        let handle = tokio::spawn(async move {
            consume_loop(consumer, inbox, &topic, &handler_id).await;
        });

        Ok(handle)
    }
}

/// Internal consume loop. Runs until the consumer stream ends or the task is cancelled.
async fn consume_loop<I: Inbox>(
    consumer: StreamConsumer,
    inbox: Arc<I>,
    topic: &str,
    handler_id: &str,
) {
    use futures::StreamExt;

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                warn!(error = %e, topic = %topic, "kafka consumer error");
                continue;
            }
        };

        let payload = match msg.payload() {
            Some(p) => p,
            None => {
                warn!(topic = %topic, "received message with empty payload, skipping");
                continue;
            }
        };

        let envelope: EventEnvelope = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, topic = %topic, "failed to deserialise event envelope");
                continue;
            }
        };

        let event_id = envelope.event_id;
        let message = IncomingMessage::ExternalEvent(envelope);

        match inbox.submit(handler_id, event_id, message).await {
            Ok(()) => {
                if let Err(e) = consumer.commit_message(&msg, CommitMode::Sync) {
                    error!(
                        error = %e,
                        topic = %topic,
                        "failed to commit offset after inbox submission"
                    );
                }
            }
            Err(e) => {
                error!(
                    error = %e,
                    topic = %topic,
                    event_id = %event_id,
                    "inbox submission failed — offset not committed"
                );
            }
        }
    }
}

/// A stream of [`EventEnvelope`]s from a Kafka topic.
///
/// Wraps an rdkafka [`StreamConsumer`], deserialising each message into an
/// [`EventEnvelope`]. Offsets are not auto-committed — the caller is responsible
/// for committing after processing.
pub struct KafkaEventStream {
    /// Held to keep the consumer alive while the spawned stream task runs.
    _consumer: Arc<StreamConsumer>,
    inner: Pin<
        Box<
            dyn Stream<Item = Result<rdkafka::message::OwnedMessage, rdkafka::error::KafkaError>>
                + Send,
        >,
    >,
}

impl Stream for KafkaEventStream {
    type Item = Result<EventEnvelope, AdaptorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        return Poll::Ready(Some(Err(AdaptorError::Adaptor(
                            "received message with empty payload".into(),
                        ))));
                    }
                };
                let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        return Poll::Ready(Some(Err(AdaptorError::Adaptor(Box::new(e)))));
                    }
                };
                Poll::Ready(Some(Ok(envelope)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(AdaptorError::Adaptor(Box::new(e))))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Returns a stream of events from the given Kafka topic.
///
/// **Note:** This path does not provide offset commit guarantees. Offsets
/// are not committed after message delivery from this stream. This method
/// is intended for read-only/stateless consumers. For inbox-integrated
/// consumers with offset commit after confirmed processing, use
/// [`KafkaEventAdaptor::consume_upstream()`] instead.
#[async_trait]
impl<I: Inbox> EventAdaptor for KafkaEventAdaptor<I> {
    async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<
        Box<dyn Stream<Item = Result<EventEnvelope, AdaptorError>> + Send + Unpin>,
        AdaptorError,
    > {
        let group_id = format!("{}-stream", self.local_service);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        let consumer = Arc::new(consumer);

        // Bridge rdkafka's borrow-based stream into an owned channel so the
        // consumer Arc can be moved into the spawned task.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let c = Arc::clone(&consumer);
        let topic_owned = topic.to_owned();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut stream = c.stream();
            while let Some(result) = stream.next().await {
                let owned = match result {
                    Ok(msg) => Ok(msg.detach()),
                    Err(e) => Err(e),
                };
                if tx.send(owned).is_err() {
                    break;
                }
            }
            info!(topic = %topic_owned, "kafka consumer stream ended");
        });

        let inner = Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx));

        Ok(Box::new(KafkaEventStream {
            _consumer: consumer,
            inner,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use canon_core::{AggregateId, Version};
    use chrono::Utc;
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockInbox {
        submitted: Mutex<Vec<(String, Uuid, IncomingMessage)>>,
        fail_on_submit: Mutex<bool>,
    }

    impl MockInbox {
        fn new() -> Self {
            Self {
                submitted: Mutex::new(Vec::new()),
                fail_on_submit: Mutex::new(false),
            }
        }
    }

    #[async_trait]
    impl Inbox for MockInbox {
        async fn register_handler(
            &self,
            _registration: canon_inbox::HandlerRegistration,
        ) -> Result<(), canon_inbox::InboxError> {
            Ok(())
        }

        async fn submit(
            &self,
            handler_id: &str,
            message_id: Uuid,
            message: IncomingMessage,
        ) -> Result<(), canon_inbox::InboxError> {
            let fail = *self
                .fail_on_submit
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if fail {
                return Err(canon_inbox::InboxError::Store("simulated failure".into()));
            }
            self.submitted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((handler_id.to_owned(), message_id, message));
            Ok(())
        }
    }

    fn test_envelope() -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::initial().next(),
            event_type: "ShipDeparted".to_owned(),
            event_version: 1,
            payload: Bytes::from(r#"{"destination":"station-1"}"#),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn envelope_deserialises_from_json() {
        let envelope = test_envelope();
        let json = serde_json::to_vec(&envelope).expect("serialise");
        let roundtripped: EventEnvelope = serde_json::from_slice(&json).expect("deserialise");
        assert_eq!(roundtripped.event_id, envelope.event_id);
        assert_eq!(roundtripped.event_type, envelope.event_type);
    }

    #[test]
    fn external_event_wraps_envelope() {
        let envelope = test_envelope();
        let event_id = envelope.event_id;
        let message = IncomingMessage::ExternalEvent(envelope);
        assert_eq!(message.message_id(), event_id);
    }

    #[tokio::test]
    async fn mock_inbox_accepts_submission() {
        let inbox = MockInbox::new();
        let envelope = test_envelope();
        let event_id = envelope.event_id;
        let message = IncomingMessage::ExternalEvent(envelope);

        inbox
            .submit("handler-1", event_id, message)
            .await
            .expect("submit");

        let submissions = inbox.submitted.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].0, "handler-1");
        assert_eq!(submissions[0].1, event_id);
    }

    #[tokio::test]
    async fn mock_inbox_rejects_when_configured() {
        let inbox = MockInbox::new();
        *inbox
            .fail_on_submit
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = true;

        let envelope = test_envelope();
        let result = inbox
            .submit(
                "handler-1",
                envelope.event_id,
                IncomingMessage::ExternalEvent(envelope),
            )
            .await;

        assert!(result.is_err());
    }

    #[test]
    fn adaptor_constructs_without_error() {
        let inbox = Arc::new(MockInbox::new());
        let adaptor = KafkaEventAdaptor::new("localhost:9092", "cargo-service", inbox);
        assert_eq!(adaptor.brokers, "localhost:9092");
        assert_eq!(adaptor.local_service, "cargo-service");
    }
}
