//! Internal event consumer — routes a service's own events back to the inbox
//! for event handler dispatch.
//!
//! This is the 4th outbound queue consumer. It reads events produced by this
//! service's command handlers and checks if any registered `#[event_handler]`
//! handles that event type/version. For each match, it submits the event to
//! the inbox as an `InternalEvent`, where the inbox handles dedup, windowing,
//! and oversight.
//!
//! Generic over an inbox submit function so the same consumer logic works with
//! both in-memory test impls and production infrastructure.

use std::sync::Arc;

use tokio::sync::Notify;

use crate::memory::inbound_queue::InMemoryInboundQueue;
use crate::memory::inbox::InMemoryInbox;
use crate::registration::__event_handler_registrations_for_event;
use crate::{EventEnvelope, IncomingMessage};

/// Errors emitted by the internal event consumer.
#[derive(Debug, thiserror::Error)]
pub enum InternalEventConsumerError {
    /// Failed to submit an event to the inbox.
    #[error("inbox submit error: {0}")]
    InboxSubmit(String),
}

/// Consumes events from the outbound queue and routes matching events to the
/// inbox as `InternalEvent` for event handler dispatch.
///
/// Uses the in-memory inbox directly. For production, a similar consumer would
/// use the `Inbox` trait from `canon-inbox`.
pub struct InternalEventConsumer {
    inbox: InMemoryInbox,
    inbound_queue: InMemoryInboundQueue,
}

impl InternalEventConsumer {
    /// Create a new internal event consumer.
    pub fn new(inbox: InMemoryInbox, inbound_queue: InMemoryInboundQueue) -> Self {
        tracing::info!("internal event consumer: created");
        Self {
            inbox,
            inbound_queue,
        }
    }

    /// Process a single event envelope. Checks registered event handlers and
    /// submits the event to the inbox for each matching handler.
    pub fn process(&self, envelope: &EventEnvelope) -> Result<(), InternalEventConsumerError> {
        let registrations =
            __event_handler_registrations_for_event(&envelope.event_type, envelope.event_version);

        if registrations.is_empty() {
            return Ok(());
        }

        let message = IncomingMessage::InternalEvent(envelope.clone());

        for reg in registrations {
            tracing::debug!(
                handler = reg.handler_type_name,
                event_type = %envelope.event_type,
                "internal event consumer: routing to handler"
            );

            self.inbox
                .submit(reg.handler_type_name, message.clone(), &self.inbound_queue)
                .map_err(|e| InternalEventConsumerError::InboxSubmit(e.to_string()))?;
        }

        Ok(())
    }

    /// Run the consumer loop. Receives events from the given
    /// [`ConsumerReceiver`](super::ConsumerReceiver), processes each one via
    /// [`Self::process`], commits offsets, and stops when `shutdown` fires.
    pub async fn run<R, F>(
        self,
        receiver: R,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        outbound_notify: Option<Arc<Notify>>,
        on_error: F,
    ) where
        R: super::ConsumerReceiver,
        F: Fn(&InternalEventConsumerError) + Send + Sync,
    {
        loop {
            if *shutdown.borrow() {
                return;
            }

            let received = tokio::select! {
                r = receiver.receive() => r,
                _ = async {
                    match outbound_notify.as_ref() {
                        Some(notify) => notify.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    // Woken by outbound notify — immediately try to receive.
                    receiver.receive().await
                }
                _ = shutdown.changed() => return,
            };

            match received {
                Ok(Some(re)) => {
                    if let Err(e) = self.process(&re.envelope) {
                        on_error(&e);
                    }
                    if let Err(commit_err) = receiver.commit().await {
                        tracing::warn!(error = %commit_err, "internal event consumer: commit failed");
                    }
                }
                Ok(None) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "internal event consumer: receive error");
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
            }
        }
    }
}
