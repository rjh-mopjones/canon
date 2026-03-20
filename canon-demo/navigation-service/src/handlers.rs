//! Event handlers for cross-service event consumption.
//!
//! DepartureHandler consumes `ShipDeparted` from fleet-service
//! (`canon.fleet.events`) and produces a `RecordDeparture` command
//! targeting the navigation Route aggregate.
//!
//! Traits are implemented manually because the shared crate owns the
//! event types (Rust orphan rules).

use async_trait::async_trait;
use bytes::Bytes;
use canon_core::{
    AggregateId, CommandEnvelope, EventHandler, IncomingMessage, MacroError, Oversight,
};
use canon_demo_shared::commands::RecordDeparture;
use canon_demo_shared::events::ShipDeparted;
use chrono::Utc;
use uuid::Uuid;

/// Handles `ShipDeparted` events from fleet-service.
///
/// When a ship departs, this handler builds a `RecordDeparture` command
/// targeting the route aggregate. The route_id is derived from the ship's
/// destination (in a real system this would use a lookup/projection; here
/// we use the destination as a stand-in for the route aggregate id).
pub struct DepartureHandler;

impl DepartureHandler {
    fn __canon_handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let event = events.into_iter().next()?;

        tracing::info!(
            ship_id = %event.ship_id,
            destination = %event.destination,
            "handling fleet ShipDeparted, producing RecordDeparture command"
        );

        let cmd = RecordDeparture {
            route_id: event.destination, // route lookup by destination
            ship_id: event.ship_id,
        };

        let payload = match serde_json::to_vec(&cmd) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize RecordDeparture command");
                return None;
            }
        };

        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::from_uuid(event.destination),
            command_type: "RecordDeparture".to_string(),
            correlation_id: Uuid::new_v4(), // in real use: propagate from incoming event envelope
            causation_id: event.ship_id,    // the ship departure caused this
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}

#[async_trait]
impl EventHandler for DepartureHandler {
    type Event = ShipDeparted;
    type Error = MacroError;

    async fn handle(
        &self,
        events: Vec<ShipDeparted>,
    ) -> Result<Option<CommandEnvelope>, Self::Error> {
        Ok(self.__canon_handle(events))
    }

    fn oversight(&self, _accumulated: &[IncomingMessage]) -> Oversight {
        Oversight::Ready
    }
}

canon_core::__submit! {
    canon_core::EventHandlerRegistration {
        handler_type_name: "DepartureHandler",
        event_type_name: "ShipDeparted",
        event_version: 1,
        window_ttl_secs: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn departure_handler_produces_command() {
        let handler = DepartureHandler;
        let ship_id = Uuid::new_v4();
        let destination = Uuid::new_v4();

        let events = vec![ShipDeparted {
            ship_id,
            destination,
            fuel_at_departure: 80.0,
        }];

        let result = EventHandler::handle(&handler, events)
            .await
            .expect("handle");
        assert!(result.is_some());

        let cmd_envelope = result.expect("command");
        assert_eq!(cmd_envelope.command_type, "RecordDeparture");
        assert_eq!(*cmd_envelope.aggregate_id.as_uuid(), destination);

        let cmd: RecordDeparture =
            serde_json::from_slice(&cmd_envelope.payload).expect("deserialize");
        assert_eq!(cmd.route_id, destination);
        assert_eq!(cmd.ship_id, ship_id);
    }

    #[tokio::test]
    async fn departure_handler_returns_none_for_empty_events() {
        let handler = DepartureHandler;
        let events: Vec<ShipDeparted> = vec![];

        let result = EventHandler::handle(&handler, events)
            .await
            .expect("handle");
        assert!(result.is_none());
    }

    #[test]
    fn departure_handler_oversight_is_ready() {
        let handler = DepartureHandler;
        assert_eq!(handler.oversight(&[]), Oversight::Ready);
    }
}
