use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::commands::{PlanRoute, RecordArrival};
use crate::events::RoutePlanned;
use crate::inbound::InboundShipDeparted;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

// ---------------------------------------------------------------------------
// ShipDepartedHandler — Fleet:ShipDeparted → Navigation:PlanRoute
// ---------------------------------------------------------------------------

#[event_handler]
impl ShipDepartedHandler {
    #[handles(InboundShipDeparted, version = 1, event_type = "ShipDeparted")]
    fn handle(&self, events: Vec<InboundShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        // Deterministic aggregate ID so Kafka replays produce the same route.
        let route_aggregate_id =
            canon_demo_shared::deterministic_command_id(Uuid::new_v4(), "RouteAggregate");

        let command = PlanRoute {
            route_id: route_aggregate_id,
            ship_id: event.ship_id,
            waypoints: vec![event.destination],
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(route_aggregate_id),
            command_type: "PlanRoute".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}

// ---------------------------------------------------------------------------
// RoutePlannedHandler — Navigation:RoutePlanned → Navigation:RecordArrival
// (internal event — service reacts to its own event)
// ---------------------------------------------------------------------------

#[event_handler]
impl RoutePlannedHandler {
    #[handles(RoutePlanned, version = 1)]
    fn handle(&self, events: Vec<RoutePlanned>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        let station_id = *event.waypoints.last()?;

        let command = RecordArrival {
            route_id: event.route_id,
            station_id,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.route_id),
            command_type: "RecordArrival".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
