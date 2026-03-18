use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{
    Aggregate, AggregateId, CommandEnvelope, EventEnvelope, EventHandler,
    InMemoryProjectionStore, Projection, Version,
};

// ── Test aggregate ──────────────────────────────────────────────────────────

#[derive(Default, Clone, Debug, PartialEq)]
pub struct OrderState {
    pub placed: bool,
    pub cancelled: bool,
}

pub enum OrderCommand {
    Place { order_id: Uuid },
    Cancel { reason: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderEvent {
    Placed { order_id: Uuid },
    Cancelled { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order already cancelled")]
    AlreadyCancelled,
    #[error("upcast failed: {0}")]
    UpcastFailed(String),
}

pub struct TestAggregate;

#[async_trait]
impl Aggregate for TestAggregate {
    type State = OrderState;
    type Command = OrderCommand;
    type Event = OrderEvent;
    type Error = TestError;

    fn apply(state: &mut Self::State, event: &Self::Event) {
        match event {
            OrderEvent::Placed { .. } => state.placed = true,
            OrderEvent::Cancelled { .. } => state.cancelled = true,
        }
    }

    async fn handle(
        state: &Self::State,
        command: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            OrderCommand::Place { order_id } => {
                if state.placed {
                    return Err(TestError::AlreadyPlaced);
                }
                Ok(vec![OrderEvent::Placed { order_id }])
            }
            OrderCommand::Cancel { reason } => {
                if state.cancelled {
                    return Err(TestError::AlreadyCancelled);
                }
                Ok(vec![OrderEvent::Cancelled { reason }])
            }
        }
    }

    fn upcast(raw: EventEnvelope) -> Result<Self::Event, Self::Error> {
        match raw.event_type.as_str() {
            "Placed" => {
                let bytes: [u8; 16] = raw
                    .payload
                    .as_ref()
                    .try_into()
                    .map_err(|_| TestError::UpcastFailed("invalid uuid bytes".into()))?;
                let order_id = Uuid::from_bytes(bytes);
                Ok(OrderEvent::Placed { order_id })
            }
            "Cancelled" => {
                let reason = String::from_utf8(raw.payload.to_vec())
                    .map_err(|e| TestError::UpcastFailed(e.to_string()))?;
                Ok(OrderEvent::Cancelled { reason })
            }
            other => Err(TestError::UpcastFailed(format!(
                "unknown event type: {other}"
            ))),
        }
    }
}

// ── Test event handlers ─────────────────────────────────────────────────────

pub struct ProducingHandler {
    pub aggregate_id: AggregateId,
}

#[async_trait]
impl EventHandler for ProducingHandler {
    type Event = OrderEvent;
    type Error = TestError;

    async fn handle(
        &self,
        _events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error> {
        Ok(Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: self.aggregate_id.clone(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"produced"),
        }))
    }
}

pub struct SilentHandler;

#[async_trait]
impl EventHandler for SilentHandler {
    type Event = OrderEvent;
    type Error = TestError;

    async fn handle(
        &self,
        _events: Vec<Self::Event>,
    ) -> Result<Option<CommandEnvelope>, Self::Error> {
        Ok(None)
    }
}

// ── Test projection ─────────────────────────────────────────────────────────

pub struct OrderProjection;

#[async_trait]
impl Projection for OrderProjection {
    type Event = OrderEvent;
    type Store = InMemoryProjectionStore;
    type Error = TestError;

    async fn apply(
        &self,
        event: &Self::Event,
        store: &Self::Store,
    ) -> Result<(), Self::Error> {
        // Idempotent: set a deterministic version based on event type
        let version = match event {
            OrderEvent::Placed { .. } => Version::initial().next(),
            OrderEvent::Cancelled { .. } => Version::initial().next().next(),
        };
        store
            .set_checkpoint(self.projection_id(), version)
            .map_err(|e| TestError::UpcastFailed(e.to_string()))?;
        Ok(())
    }

    async fn rebuild(
        &self,
        events: impl futures::Stream<Item = Self::Event> + Send,
        store: &Self::Store,
    ) -> Result<(), Self::Error> {
        use futures::StreamExt;
        let mut stream = std::pin::pin!(events);
        while let Some(event) = stream.next().await {
            self.apply(&event, store).await?;
        }
        Ok(())
    }

    fn projection_id(&self) -> &str {
        "order-projection"
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Create an EventEnvelope from an OrderEvent.
pub fn make_event_envelope(aggregate_id: &AggregateId, event: &OrderEvent) -> EventEnvelope {
    let (event_type, payload) = match event {
        OrderEvent::Placed { order_id } => (
            "Placed".to_string(),
            Bytes::copy_from_slice(order_id.as_bytes()),
        ),
        OrderEvent::Cancelled { reason } => (
            "Cancelled".to_string(),
            Bytes::from(reason.clone()),
        ),
    };

    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial(), // set by event store on append
        event_type,
        event_version: 1,
        payload,
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Create a CommandEnvelope with the given payload.
pub fn make_command_envelope(aggregate_id: &AggregateId, payload: &[u8]) -> CommandEnvelope {
    CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::copy_from_slice(payload),
    }
}

/// Build a Version from a u64, chaining `next()` from `initial()`.
pub fn version(n: u64) -> Version {
    let mut v = Version::initial();
    for _ in 0..n {
        v = v.next();
    }
    v
}
