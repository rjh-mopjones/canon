use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::ClientBuilder;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use canon_adaptor::{AdaptorError, EventAdaptor, EventEnvelope};
use canon_core::IncomingMessage;
use canon_inbox::Inbox;

/// Kafka-backed [`EventAdaptor`]. Anti-corruption layer at the service boundary.
///
/// Consumes events from upstream services' `canon.{upstream}.events` topics and
/// submits them to the local inbox as [`IncomingMessage::ExternalEvent`].
///
/// Uses rskafka with in-memory offset tracking. On restart, consumption resumes
/// from offset 0 -- the inbox deduplicates via `(handler_id, message_id)` PK.
pub struct KafkaEventAdaptor<I: Inbox> {
    brokers: String,
    local_service: String,
    inbox: Arc<I>,
}

impl<I: Inbox> KafkaEventAdaptor<I> {
    /// Create a new adaptor.
    ///
    /// # Arguments
    /// - `brokers` -- comma-separated Kafka broker list (e.g. from `KAFKA_BROKERS`)
    /// - `local_service` -- name of the consuming service (used in logging)
    /// - `inbox` -- the local inbox to submit external events to
    pub fn new(brokers: &str, local_service: &str, inbox: Arc<I>) -> Self {
        Self {
            brokers: brokers.to_owned(),
            local_service: local_service.to_owned(),
            inbox,
        }
    }

    /// Consume events from an upstream service, forwarding them to the local inbox.
    ///
    /// Creates an rskafka partition client for `canon.{upstream_service}.events` and
    /// spawns a background polling task that:
    /// 1. Fetches records from partition 0 starting at offset 0
    /// 2. Deserialises each message as [`EventEnvelope`]
    /// 3. Submits it to the inbox as [`IncomingMessage::ExternalEvent`]
    ///
    /// Offset is tracked in-memory. On restart, re-reads from 0; the inbox
    /// deduplicates via `(handler_id, message_id)` PK.
    ///
    /// Returns a [`JoinHandle`] for the consumer task.
    pub async fn consume_upstream(
        &self,
        upstream_service: &str,
        handler_id: &str,
    ) -> Result<JoinHandle<()>, AdaptorError> {
        let topic = format!("canon.{upstream_service}.events");

        let broker_list: Vec<String> = self
            .brokers
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let client = ClientBuilder::new(broker_list)
            .build()
            .await
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        let partition_client = Arc::new(
            client
                .partition_client(&topic, 0, UnknownTopicHandling::Error)
                .await
                .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?,
        );

        info!(
            topic = %topic,
            service = %self.local_service,
            "subscribed to upstream events (rskafka)"
        );

        let inbox = Arc::clone(&self.inbox);
        let handler_id = handler_id.to_owned();
        let topic_owned = topic.clone();
        let handle = tokio::spawn(async move {
            consume_loop(partition_client, inbox, &topic_owned, &handler_id).await;
        });

        Ok(handle)
    }
}

/// Internal consume loop. Runs until the task is cancelled.
async fn consume_loop<I: Inbox>(
    partition_client: Arc<rskafka::client::partition::PartitionClient>,
    inbox: Arc<I>,
    topic: &str,
    handler_id: &str,
) {
    let mut next_offset: i64 = 0;

    loop {
        match partition_client
            .fetch_records(next_offset, 1..1_048_576, 1_000)
            .await
        {
            Ok((records, _watermark)) => {
                if records.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }

                for record_and_offset in &records {
                    next_offset = record_and_offset.offset + 1;

                    let payload = match record_and_offset.record.value.as_ref() {
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

                    if let Err(e) = inbox.submit(handler_id, event_id, message).await {
                        error!(
                            error = %e,
                            topic = %topic,
                            event_id = %event_id,
                            "inbox submission failed"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, topic = %topic, "kafka fetch failed, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// A stream of [`EventEnvelope`]s from a Kafka topic.
///
/// Wraps an rskafka polling loop via an mpsc channel, deserialising each
/// record into an [`EventEnvelope`].
pub struct KafkaEventStream {
    rx: Pin<Box<dyn Stream<Item = Result<EventEnvelope, AdaptorError>> + Send>>,
}

impl Stream for KafkaEventStream {
    type Item = Result<EventEnvelope, AdaptorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.as_mut().poll_next(cx)
    }
}

/// Returns a stream of events from the given Kafka topic.
///
/// **Note:** This path does not integrate with the inbox. It is intended for
/// read-only/stateless consumers. For inbox-integrated consumers, use
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
        let broker_list: Vec<String> = self
            .brokers
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect();

        let client = ClientBuilder::new(broker_list)
            .build()
            .await
            .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?;

        let partition_client = Arc::new(
            client
                .partition_client(topic, 0, UnknownTopicHandling::Error)
                .await
                .map_err(|e| AdaptorError::Adaptor(Box::new(e)))?,
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let topic_owned = topic.to_owned();
        tokio::spawn(async move {
            let mut next_offset: i64 = 0;
            loop {
                match partition_client
                    .fetch_records(next_offset, 1..1_048_576, 1_000)
                    .await
                {
                    Ok((records, _watermark)) => {
                        if records.is_empty() {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            continue;
                        }
                        for record_and_offset in &records {
                            next_offset = record_and_offset.offset + 1;

                            let payload = match record_and_offset.record.value.as_ref() {
                                Some(p) => p,
                                None => {
                                    if tx
                                        .send(Err(AdaptorError::Adaptor(
                                            "received message with empty payload".into(),
                                        )))
                                        .is_err()
                                    {
                                        return;
                                    }
                                    continue;
                                }
                            };

                            let envelope: EventEnvelope = match serde_json::from_slice(payload) {
                                Ok(e) => e,
                                Err(e) => {
                                    if tx.send(Err(AdaptorError::Adaptor(Box::new(e)))).is_err() {
                                        return;
                                    }
                                    continue;
                                }
                            };

                            if tx.send(Ok(envelope)).is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, topic = %topic_owned, "kafka fetch failed, retrying");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        let inner = Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx));

        Ok(Box::new(KafkaEventStream { rx: inner }))
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

    // -- Testcontainer-based integration tests --

    mod testcontainer_tests {
        use super::*;
        use canon_adaptor::EventAdaptor;
        use futures::StreamExt;
        use rskafka::client::partition::Compression;
        use rskafka::client::ClientBuilder;
        use rskafka::record::Record;
        use std::collections::BTreeMap;
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

                    // Wait for Kafka to be ready by attempting to connect via rskafka
                    for attempt in 0..30 {
                        match ClientBuilder::new(vec![brokers.clone()]).build().await {
                            Ok(_) => break,
                            Err(e) => {
                                if attempt >= 29 {
                                    panic!("Kafka broker not ready after 30 attempts: {e}");
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

        /// Publish a serialised EventEnvelope to a Kafka topic using rskafka.
        async fn publish_envelope(brokers: &str, topic: &str, envelope: &EventEnvelope) {
            let client = ClientBuilder::new(vec![brokers.to_owned()])
                .build()
                .await
                .expect("failed to create client");

            let partition_client = client
                .partition_client(
                    topic,
                    0,
                    rskafka::client::partition::UnknownTopicHandling::Retry,
                )
                .await
                .expect("failed to create partition client");

            let payload = serde_json::to_vec(envelope).expect("failed to serialise envelope");
            let key = envelope.aggregate_id.as_uuid().to_string();

            partition_client
                .produce(
                    vec![Record {
                        key: Some(key.into_bytes()),
                        value: Some(payload),
                        headers: BTreeMap::new(),
                        timestamp: chrono::Utc::now(),
                    }],
                    Compression::NoCompression,
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

            // Small delay to let consumer start polling
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
