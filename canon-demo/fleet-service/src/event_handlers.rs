use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{event_handler, AggregateId, CommandEnvelope};
use canon_demo_shared::commands::ScheduleResupply;
use canon_demo_shared::events::ResupplyDispatched;

#[event_handler]
impl ResupplyHandler {
    #[handles(ResupplyDispatched, version = 1)]
    fn handle(&self, events: Vec<ResupplyDispatched>) -> Option<CommandEnvelope> {
        for event in &events {
            let command = ScheduleResupply {
                ship_id: event.ship_id,
                fuel_kg: event.fuel_kg,
            };
            let payload = match serde_json::to_vec(&command) {
                Ok(p) => p,
                Err(_) => continue,
            };
            return Some(CommandEnvelope {
                command_id: Uuid::new_v4(),
                aggregate_id: AggregateId::from_uuid(event.ship_id),
                command_type: "ScheduleResupply".into(),
                correlation_id: Uuid::new_v4(),
                causation_id: Uuid::new_v4(),
                timestamp: Utc::now(),
                payload: Bytes::from(payload),
                command_version: 1,
            });
        }
        None
    }
}
