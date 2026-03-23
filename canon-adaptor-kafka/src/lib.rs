use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;
use samsa::prelude::{
    BrokerAddress, ConsumerGroup, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder,
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use canon_adaptor::{AdaptorError, EventAdaptor, EventEnvelope};
use canon_core::IncomingMessage;
use canon_inbox::Inbox;

/// Parse a comma-separated broker string into samsa `BrokerAddress` list.
fn parse_brokers(brokers: &str) -> Vec<BrokerAddress> {
    brokers
        .split(',')
        .filter_map(|addr| {
            let addr = addr.trim();
            let (host, port_str) = addr.rsplit_once(':')?;
            let port = port_str.parse::<u16>().ok()?;
            Some(BrokerAddress {
                host: host.to_owned(),
                port,
            })
        })
        .collect()
}

/// Kafka-backed [`EventAdaptor`]. Anti-corruption layer at the service boundary.
pub struct KafkaEventAdaptor<I: Inbox> {
    brokers: String,
    local_service: String,
    inbox: Arc<I>,
}

impl<I: Inbox> KafkaEventAdaptor<I> {
    pub fn new(brokers: &str, local_service: &str, inbox: Arc<I>) -> Self {
        Self {
            brokers: brokers.to_owned(),
            local_service: local_service.to_owned(),
            inbox,
        }
    }

    /// Consume events from an upstream service, forwarding them to the local inbox.
    pub async fn consume_upstream(
        &self,
        upstream_service: &str,
        handler_id: &str,
    ) -> Result<JoinHandle<()>, AdaptorError> {
        let topic = format!("canon.{upstream_service}.events");
        let group_id = format!("{}-{handler_id}", self.local_service);
        let addrs = parse_brokers(&self.brokers);

        let assignment = TopicPartitionsBuilder::new()
            .assign(topic.clone(), vec![0])
            .build();

        let consumer =
            ConsumerGroupBuilder::<TcpConnection>::new(addrs, group_id.clone(), assignment)
                .await
                .map_err(|e| AdaptorError::Adaptor(e.to_string().into()))?
                .build()
                .await
                .map_err(|e| AdaptorError::Adaptor(e.to_string().into()))?;

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

/// Internal consume loop.
async fn consume_loop<I: Inbox>(
    mut consumer: ConsumerGroup<TcpConnection>,
    inbox: Arc<I>,
    topic: &str,
    handler_id: &str,
) {
    use futures::StreamExt;

    let stream = consumer.into_stream();
    tokio::pin!(stream);

    while let Some(Ok(batch)) = stream.next().await {
        for msg in batch {
            if msg.value.is_empty() {
                warn!(topic = %topic, "received message with empty payload, skipping");
                continue;
            }

            let envelope: EventEnvelope = match serde_json::from_slice(&msg.value) {
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
}

/// A stream of [`EventEnvelope`]s from a Kafka topic.
pub struct KafkaEventStream {
    inner: Pin<Box<dyn Stream<Item = Result<EventEnvelope, AdaptorError>> + Send>>,
}

impl Stream for KafkaEventStream {
    type Item = Result<EventEnvelope, AdaptorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

#[async_trait]
impl<I: Inbox> EventAdaptor for KafkaEventAdaptor<I> {
    async fn subscribe(
        &self,
        topic: &str,
    ) -> Result<
        Box<dyn Stream<Item = Result<EventEnvelope, AdaptorError>> + Send + Unpin>,
        AdaptorError,
    > {
        let addrs = parse_brokers(&self.brokers);
        let group_id = format!("{}-stream", self.local_service);

        let assignment = TopicPartitionsBuilder::new()
            .assign(topic.to_owned(), vec![0])
            .build();

        let consumer = ConsumerGroupBuilder::<TcpConnection>::new(addrs, group_id, assignment)
            .await
            .map_err(|e| AdaptorError::Adaptor(e.to_string().into()))?
            .build()
            .await
            .map_err(|e| AdaptorError::Adaptor(e.to_string().into()))?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut consumer = consumer;
            let stream = consumer.into_stream();
            tokio::pin!(stream);

            while let Some(Ok(batch)) = stream.next().await {
                for msg in batch {
                    if msg.value.is_empty() {
                        continue;
                    }
                    let result: Result<EventEnvelope, AdaptorError> =
                        serde_json::from_slice(&msg.value)
                            .map_err(|e| AdaptorError::Adaptor(Box::new(e)));
                    if tx.send(result).is_err() {
                        return;
                    }
                }
            }
        });

        let recv_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

        Ok(Box::new(KafkaEventStream {
            inner: Box::pin(recv_stream),
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

    #[test]
    fn parse_brokers_works() {
        let addrs = parse_brokers("localhost:9092,kafka:9093");
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].host, "localhost");
        assert_eq!(addrs[0].port, 9092);
    }
}
