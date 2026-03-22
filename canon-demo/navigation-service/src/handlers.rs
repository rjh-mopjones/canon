//! Event handlers for the navigation service.
//!
//! Cross-service event handling (Fleet:ShipDeparted → PlanRoute, and
//! Nav:RoutePlanned → RecordArrival) is implemented in `cross_service.rs`
//! as direct Kafka consumers rather than via the `EventHandler` trait,
//! because the shared crate owns the event types (Rust orphan rules).
//!
//! `DepartureHandler` is the `EventHandler` trait implementation that
//! converts a `ShipDeparted` event into a `PlanRoute` command. It is used
//! in unit/integration tests and can be wired into the Canon pipeline
//! alongside the Kafka-based cross-service consumer.

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{event_handler, AggregateId, CommandEnvelope};
use canon_demo_shared::commands::PlanRoute;
use canon_demo_shared::events::ShipDeparted;

#[event_handler]
impl DepartureHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.last()?;

        let command = PlanRoute {
            route_id: event.destination,
            ship_id: event.ship_id,
            waypoints: vec![event.destination],
        };
        let payload = serde_json::to_vec(&command).ok()?;

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.destination),
            command_type: "PlanRoute".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}
