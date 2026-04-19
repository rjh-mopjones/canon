//! Event handlers for the navigation service.
//!
//! `DepartureHandler` converts a `ShipDeparted` event into a `PlanRoute` command.

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use crate::commands::PlanRoute;
use crate::inbound::InboundShipDeparted as ShipDeparted;
use canon_core::{event_handler, AggregateId, CommandEnvelope};

#[event_handler]
impl DepartureHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        // Each departure carries a voyage_id that becomes the route aggregate
        // identity, so repeated trips to the same destination stay distinct
        // while Kafka replays still deduplicate safely.
        let route_id = event.voyage_id;
        let command = PlanRoute {
            route_id,
            ship_id: event.ship_id,
            waypoints: vec![event.destination],
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: canon_demo_shared::deterministic_command_id(route_id, "PlanRoute"),
            aggregate_id: AggregateId::from_uuid(route_id),
            command_type: "PlanRoute".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
