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
        let route_aggregate_id = event.voyage_id;
        let command_id =
            canon_demo_shared::deterministic_command_id(route_aggregate_id, "PlanRoute");

        let command = PlanRoute {
            route_id: route_aggregate_id,
            ship_id: event.ship_id,
            waypoints: vec![event.destination],
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id,
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
        let command_id = canon_demo_shared::deterministic_command_id_from_key(
            &format!("route:{}:station:{}", event.route_id, station_id),
            "RecordArrival",
        );

        let command = RecordArrival {
            route_id: event.route_id,
            ship_id: event.ship_id,
            station_id,
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::EventHandler;

    #[tokio::test]
    async fn ship_departed_handler_uses_voyage_id_for_route_identity() {
        let handler = ShipDepartedHandler;
        let ship_id = Uuid::new_v4();
        let destination = Uuid::new_v4();
        let first_voyage_id = Uuid::new_v4();
        let second_voyage_id = Uuid::new_v4();

        let first = handler
            .handle(vec![InboundShipDeparted {
                ship_id,
                voyage_id: first_voyage_id,
                destination,
            }])
            .await
            .expect("first plan route result")
            .expect("first plan route command");
        let second = handler
            .handle(vec![InboundShipDeparted {
                ship_id,
                voyage_id: second_voyage_id,
                destination,
            }])
            .await
            .expect("second plan route result")
            .expect("second plan route command");

        assert_eq!(first.aggregate_id.as_uuid(), &first_voyage_id);
        assert_eq!(second.aggregate_id.as_uuid(), &second_voyage_id);
        assert_ne!(first.command_id, second.command_id);
    }
}
