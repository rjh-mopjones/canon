use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::commands::ScheduleResupply;
use crate::inbound::InboundResupplyDispatched as ResupplyDispatched;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

#[event_handler]
impl ResupplyHandler {
    #[handles(ResupplyDispatched, version = 1)]
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> {
        // Process the last event in the batch — it represents the most recent
        // resupply dispatch. The EventHandler trait allows at most one command
        // per batch, so we pick the latest.
        let event = events.last()?;

        let command = ScheduleResupply {
            ship_id: event.ship_id,
            fuel_kg: event.fuel_kg,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        // NOTE: correlation_id and causation_id are set to fresh UUIDs because
        // the #[event_handler] macro delivers deserialized event structs without
        // envelope metadata. When the framework supports passing envelope context
        // to event handlers, these should chain from the source event's
        // correlation_id (for end-to-end tracing) and event_id (as causation).
        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.ship_id),
            command_type: "ScheduleResupply".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
