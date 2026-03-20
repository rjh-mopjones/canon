use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

// ── Test aggregate ──────────────────────────────────────────────────────────

#[aggregate]
pub struct OrderAggregate {
    pub placed: bool,
    pub cancelled: bool,
    pub priority: Option<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum OrderError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order already cancelled")]
    AlreadyCancelled,
}

// ── Commands (v1) ───────────────────────────────────────────────────────────

#[command(OrderAggregate, version = 1, produces = [OrderPlaced])]
pub struct PlaceOrder {
    pub order_id: Uuid,
}

#[command(OrderAggregate, version = 1, produces = [OrderCancelled])]
pub struct CancelOrder {
    pub reason: String,
}

// ── Events (v1) ─────────────────────────────────────────────────────────────

#[event(OrderAggregate, version = 1)]
pub struct OrderPlaced {
    pub order_id: Uuid,
}

#[event(OrderAggregate, version = 1)]
pub struct OrderCancelled {
    pub reason: String,
}

// ── Event combiners (v1) ────────────────────────────────────────────────────

#[event_combiner(OrderAggregate, version = 1)]
impl OrderPlaced {
    fn combine(&self, state: &mut OrderAggregate) {
        state.placed = true;
    }
}

#[event_combiner(OrderAggregate, version = 1)]
impl OrderCancelled {
    fn combine(&self, state: &mut OrderAggregate) {
        state.cancelled = true;
    }
}

// ── Command handlers (v1) ───────────────────────────────────────────────────

#[command_handler(OrderAggregate, version = 1)]
impl PlaceOrderHandler {
    type Error = OrderError;

    fn handle(&self, state: &OrderAggregate, cmd: PlaceOrder) -> Result<OrderPlaced, OrderError> {
        if state.placed {
            return Err(OrderError::AlreadyPlaced);
        }
        Ok(OrderPlaced {
            order_id: cmd.order_id,
        })
    }
}

#[command_handler(OrderAggregate, version = 1)]
impl CancelOrderHandler {
    type Error = OrderError;

    fn handle(
        &self,
        state: &OrderAggregate,
        cmd: CancelOrder,
    ) -> Result<OrderCancelled, OrderError> {
        if state.cancelled {
            return Err(OrderError::AlreadyCancelled);
        }
        Ok(OrderCancelled { reason: cmd.reason })
    }
}

// ── V2 types for versioning tests ───────────────────────────────────────────

#[command(OrderAggregate, version = 2, produces = [OrderPlacedV2])]
pub struct PlaceOrderV2 {
    pub order_id: Uuid,
    pub priority: u8,
}

#[event(OrderAggregate, version = 2)]
pub struct OrderPlacedV2 {
    pub order_id: Uuid,
    pub priority: u8,
}

#[event_combiner(OrderAggregate, version = 2)]
impl OrderPlacedV2 {
    fn combine(&self, state: &mut OrderAggregate) {
        state.placed = true;
        state.priority = Some(self.priority);
    }
}

#[command_handler(OrderAggregate, version = 2)]
impl PlaceOrderV2Handler {
    type Error = OrderError;

    fn handle(
        &self,
        state: &OrderAggregate,
        cmd: PlaceOrderV2,
    ) -> Result<OrderPlacedV2, OrderError> {
        if state.placed {
            return Err(OrderError::AlreadyPlaced);
        }
        Ok(OrderPlacedV2 {
            order_id: cmd.order_id,
            priority: cmd.priority,
        })
    }
}

// ── Event handlers ──────────────────────────────────────────────────────────

#[event_handler]
impl ProducingHandler {
    #[handles(OrderPlaced, version = 1)]
    fn handle(&self, _events: Vec<OrderPlaced>) -> Option<CommandEnvelope> {
        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            command_type: "ProducedCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from_static(b"produced"),
            command_version: 1,
        })
    }
}

#[event_handler]
impl SilentHandler {
    #[handles(OrderPlaced, version = 1)]
    fn handle(&self, _events: Vec<OrderPlaced>) -> Option<CommandEnvelope> {
        None
    }
}

// ── Test projection ─────────────────────────────────────────────────────────

pub struct OrderProjection;

#[async_trait::async_trait]
impl Projection for OrderProjection {
    type Event = OrderPlaced;
    type Store = InMemoryProjectionStore;
    type Error = MacroError;

    async fn apply(&self, _event: &Self::Event, store: &Self::Store) -> Result<(), Self::Error> {
        store
            .set_checkpoint(self.projection_id(), Version::initial().next())
            .map_err(|e| MacroError(e.to_string()))?;
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

/// Create an EventEnvelope for an OrderPlaced event (v1).
pub fn make_placed_envelope(aggregate_id: &AggregateId, order_id: Uuid) -> EventEnvelope {
    let payload = serde_json::to_vec(&OrderPlaced { order_id }).expect("serialize OrderPlaced");
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial(), // set by event store on append
        event_type: "OrderPlaced".to_string(),
        event_version: 1,
        payload: Bytes::from(payload),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Create an EventEnvelope for an OrderCancelled event (v1).
pub fn make_cancelled_envelope(aggregate_id: &AggregateId, reason: &str) -> EventEnvelope {
    let payload = serde_json::to_vec(&OrderCancelled {
        reason: reason.to_string(),
    })
    .expect("serialize OrderCancelled");
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial(),
        event_type: "OrderCancelled".to_string(),
        event_version: 1,
        payload: Bytes::from(payload),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Create an EventEnvelope for an OrderPlacedV2 event (v2).
pub fn make_placed_v2_envelope(
    aggregate_id: &AggregateId,
    order_id: Uuid,
    priority: u8,
) -> EventEnvelope {
    let payload =
        serde_json::to_vec(&OrderPlacedV2 { order_id, priority }).expect("serialize OrderPlacedV2");
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial(),
        event_type: "OrderPlacedV2".to_string(),
        event_version: 2,
        payload: Bytes::from(payload),
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
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::copy_from_slice(payload),
        command_version: 1,
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
