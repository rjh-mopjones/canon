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

        async fn try_mark_window_processed(
            &self,
            _window_id: Uuid,
            _handler_id: &str,
        ) -> Result<bool, canon_inbox::InboxError> {
            Ok(true)
        }

        async fn sweep_expired_windows(&self) -> Result<u64, canon_inbox::InboxError> {
            Ok(0)
        }

        async fn collect_expired_windows(
            &self,
        ) -> Result<Vec<canon_inbox::ExpiredWindowEntry>, canon_inbox::InboxError> {
            Ok(Vec::new())
        }

        async fn requeue_expired_window(
            &self,
            _handler_id: &str,
            _correlation_key: Uuid,
            _messages: Vec<IncomingMessage>,
        ) -> Result<(), canon_inbox::InboxError> {
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

    // ── Testcontainer-based integration tests ──────────────────────────────

    mod testcontainer_tests {
        use super::*;
        use canon_adaptor::EventAdaptor;
        use futures::StreamExt;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::Consumer;
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use std::sync::OnceLock;
        use std::time::Duration;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::ContainerAsync;
        use testcontainers_modules::kafka::apache::Kafka;

        struct KafkaContainer {
            _container: ContainerAsync<Kafka>,
            brokers: String,
        }

        static KAFKA: OnceLock<tokio::sync::OnceCell<KafkaContainer>> = OnceLock::new();

        fn kafka_cell() -> &'static tokio::sync::OnceCell<KafkaContainer> {
            KAFKA.get_or_init(tokio::sync::OnceCell::new)
        }

        async fn get_kafka() -> &'static KafkaContainer {
            kafka_cell()
                .get_or_init(|| async {
                    let container = Kafka::default()
                        .start()
                        .await
                        .expect("failed to start Kafka container");

                    let host_port = container
                        .get_host_port_ipv4(9092)
                        .await
                        .expect("failed to get Kafka port");

                    let brokers = format!("127.0.0.1:{host_port}");

                    // Wait for Kafka to be ready
                    for attempt in 0..30 {
                        let probe: Result<rdkafka::consumer::BaseConsumer, _> =
                            ClientConfig::new()
                                .set("bootstrap.servers", &brokers)
                                .create();
                        match probe {
                            Ok(consumer) => {
                                match consumer.fetch_metadata(None, Duration::from_secs(2)) {
                                    Ok(_) => break,
                                    Err(e) => {
                                        if attempt >= 29 {
                                            panic!(
                                                "Kafka broker not ready after 30 attempts: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                if attempt >= 29 {
                                    panic!(
                                        "failed to create Kafka probe consumer after 30 attempts: {e}"
                                    );
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }

                    KafkaContainer {
                        _container: container,
                        brokers,
                    }
                })
                .await
        }

        /// Publish a serialised EventEnvelope to a Kafka topic using a FutureProducer.
        async fn publish_envelope(brokers: &str, topic: &str, envelope: &EventEnvelope) {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", "5000")
                .create()
                .expect("failed to create producer");

            let payload = serde_json::to_vec(envelope).expect("failed to serialise envelope");
            let key = envelope.aggregate_id.as_uuid().to_string();

            producer
                .send(
                    FutureRecord::to(topic).key(&key).payload(&payload),
                    Duration::from_secs(5),
                )
                .await
                .expect("failed to publish to Kafka");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn subscribe_returns_stream_of_events() {
            let kafka = get_kafka().await;
            let topic = format!("adaptor-subscribe-{}", Uuid::new_v4());
            let envelope = test_envelope();
            let expected_event_id = envelope.event_id;
            let expected_event_type = envelope.event_type.clone();
            let expected_aggregate_id = *envelope.aggregate_id.as_uuid();

            // Publish before subscribing so the message is ready
            publish_envelope(&kafka.brokers, &topic, &envelope).await;

            let inbox = Arc::new(MockInbox::new());
            let group_suffix = Uuid::new_v4().to_string();
            let adaptor =
                KafkaEventAdaptor::new(&kafka.brokers, &format!("test-{group_suffix}"), inbox);

            let mut stream = adaptor
                .subscribe(&topic)
                .await
                .expect("subscribe should succeed");

            let received = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .expect("timed out waiting for event from stream")
                .expect("stream should not be empty")
                .expect("event should deserialise successfully");

            assert_eq!(received.event_id, expected_event_id);
            assert_eq!(received.event_type, expected_event_type);
            assert_eq!(*received.aggregate_id.as_uuid(), expected_aggregate_id);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn consume_upstream_submits_to_inbox() {
            let kafka = get_kafka().await;
            let upstream_service = format!("upstream-{}", Uuid::new_v4());
            let topic = format!("canon.{upstream_service}.events");
            let handler_id = format!("handler-{}", Uuid::new_v4());

            let envelope = test_envelope();
            let expected_event_id = envelope.event_id;

            let inbox = Arc::new(MockInbox::new());
            let group_suffix = Uuid::new_v4().to_string();
            let adaptor = KafkaEventAdaptor::new(
                &kafka.brokers,
                &format!("consume-test-{group_suffix}"),
                Arc::clone(&inbox),
            );

            // Start consuming in background
            let handle = adaptor
                .consume_upstream(&upstream_service, &handler_id)
                .await
                .expect("consume_upstream should succeed");

            // Small delay to let consumer group join
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Publish the event
            publish_envelope(&kafka.brokers, &topic, &envelope).await;

            // Poll until the inbox receives the message (with timeout)
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                {
                    let submissions = inbox.submitted.lock().unwrap_or_else(|e| e.into_inner());
                    if !submissions.is_empty() {
                        assert_eq!(submissions.len(), 1);
                        assert_eq!(submissions[0].0, handler_id);
                        assert_eq!(submissions[0].1, expected_event_id);
                        // Verify it was wrapped as ExternalEvent
                        match &submissions[0].2 {
                            IncomingMessage::ExternalEvent(env) => {
                                assert_eq!(env.event_id, expected_event_id);
                            }
                            other => panic!("expected ExternalEvent, got {:?}", other),
                        }
                        break;
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("timed out waiting for inbox submission");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Clean up the background task
            handle.abort();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn envelope_roundtrip_via_stream() {
            let kafka = get_kafka().await;
            let topic = format!("adaptor-roundtrip-{}", Uuid::new_v4());

            let envelope = test_envelope();
            let expected_event_id = envelope.event_id;
            let expected_aggregate_id = *envelope.aggregate_id.as_uuid();
            let expected_version = envelope.version.as_u64();
            let expected_event_type = envelope.event_type.clone();
            let expected_event_version = envelope.event_version;
            let expected_payload = envelope.payload.clone();
            let expected_correlation_id = envelope.correlation_id;
            let expected_causation_id = envelope.causation_id;
            let expected_timestamp = envelope.timestamp;

            // Publish before subscribing
            publish_envelope(&kafka.brokers, &topic, &envelope).await;

            let inbox = Arc::new(MockInbox::new());
            let group_suffix = Uuid::new_v4().to_string();
            let adaptor =
                KafkaEventAdaptor::new(&kafka.brokers, &format!("rt-{group_suffix}"), inbox);

            let mut stream = adaptor
                .subscribe(&topic)
                .await
                .expect("subscribe should succeed");

            let received = tokio::time::timeout(Duration::from_secs(30), stream.next())
                .await
                .expect("timed out waiting for event from stream")
                .expect("stream should not be empty")
                .expect("event should deserialise successfully");

            assert_eq!(received.event_id, expected_event_id);
            assert_eq!(*received.aggregate_id.as_uuid(), expected_aggregate_id);
            assert_eq!(received.version.as_u64(), expected_version);
            assert_eq!(received.event_type, expected_event_type);
            assert_eq!(received.event_version, expected_event_version);
            assert_eq!(received.payload, expected_payload);
            assert_eq!(received.correlation_id, expected_correlation_id);
            assert_eq!(received.causation_id, expected_causation_id);
            assert_eq!(received.timestamp, expected_timestamp);
        }
    }
}
