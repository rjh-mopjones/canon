use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::commands::{DockShip, ScheduleResupply};
use crate::inbound::{InboundResupplyDispatched, InboundShipArrivedAtStation};
use canon_core::{event_handler, AggregateId, CommandEnvelope};

// ---------------------------------------------------------------------------
// ResupplyHandler — Supply:ResupplyDispatched → Fleet:ScheduleResupply
// ---------------------------------------------------------------------------

#[event_handler]
impl ResupplyHandler {
    #[handles(
        InboundResupplyDispatched,
        version = 1,
        event_type = "ResupplyDispatched"
    )]
    fn handle(&self, events: Vec<InboundResupplyDispatched>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let command = ScheduleResupply {
            ship_id: event.ship_id,
            fuel_kg: event.fuel_kg,
        };
        let payload = serde_json::to_vec(&command).ok()?;

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

// ---------------------------------------------------------------------------
// DockingHandler — Navigation:ShipArrivedAtStation → Fleet:DockShip
// ---------------------------------------------------------------------------

#[event_handler]
impl DockingHandler {
    #[handles(
        InboundShipArrivedAtStation,
        version = 1,
        event_type = "ShipArrivedAtStation"
    )]
    fn handle(&self, events: Vec<InboundShipArrivedAtStation>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let command = DockShip {
            ship_id: event.ship_id,
            station_id: event.station_id,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.ship_id),
            command_type: "DockShip".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
