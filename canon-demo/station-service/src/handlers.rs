//! Cross-service event handlers.
//!
//! - `ShipArrivedHandler`: consumes `ShipArrivedAtStation` from `canon.navigation.events`
//!   and produces a `RecordDocking` command.
//! - `CargoUnloadedHandler`: consumes `CargoUnloaded` from `canon.cargo.events`
//!   and produces a `RecordCargoReceived` command.
//! - `StockLevelMonitorHandler`: internal event handler that consumes `CargoReceived`
//!   and produces a `CheckStockLevel` command to evaluate the 20% low-stock threshold.

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::CommandEnvelope;
use canon_demo_shared::events::{
    CargoUnloaded as SharedCargoUnloaded, ShipArrivedAtStation as SharedShipArrivedAtStation,
};

use crate::events::CargoReceived;

/// Handles `ShipArrivedAtStation` events from the navigation service.
/// Produces a `RecordDocking` command for the station aggregate.
#[canon_core::event_handler]
impl ShipArrivedHandler {
    #[handles(SharedShipArrivedAtStation, version = 1)]
    fn handle(&self, events: Vec<SharedShipArrivedAtStation>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        let payload = serde_json::to_vec(&serde_json::json!({
            "station_id": event.station_id,
            "ship_id": event.ship_id,
        }));
        let payload = match payload {
            Ok(p) => p,
            Err(_) => return None,
        };
        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: canon_core::AggregateId::from_uuid(event.station_id),
            command_type: "RecordDocking".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: event.route_id,
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}

/// Handles `CargoUnloaded` events from the cargo service.
/// Produces a `RecordCargoReceived` command for the station aggregate.
#[canon_core::event_handler]
impl CargoUnloadedHandler {
    #[handles(SharedCargoUnloaded, version = 1)]
    fn handle(&self, events: Vec<SharedCargoUnloaded>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        // CargoUnloaded does not carry weight_kg or station_id directly.
        // In a real system, the handler would resolve these from the manifest.
        // For the demo, we produce a command with the manifest_id and a nominal weight.
        let payload = serde_json::to_vec(&serde_json::json!({
            "station_id": Uuid::nil(),
            "manifest_id": event.manifest_id,
            "weight_kg": 100.0_f32,
        }));
        let payload = match payload {
            Ok(p) => p,
            Err(_) => return None,
        };
        // aggregate_id uses the nil station_id placeholder — in production the
        // handler would resolve the station from the manifest via a lookup.
        let station_id = Uuid::nil();
        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: canon_core::AggregateId::from_uuid(station_id),
            command_type: "RecordCargoReceived".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: event.manifest_id,
            timestamp: Utc::now(),
            payload: Bytes::from(payload),
            command_version: 1,
        })
    }
}

/// Internal event handler that monitors `CargoReceived` events and triggers
/// a stock level check. The actual threshold evaluation happens in the
/// `CheckStockLevelHandler` command handler, which has access to aggregate state.
#[canon_core::event_handler]
impl StockLevelMonitorHandler {
    #[handles(CargoReceived, version = 1)]
    fn handle(&self, events: Vec<CargoReceived>) -> Option<CommandEnvelope> {
        let event = events.last()?;
        let payload = serde_json::to_vec(&serde_json::json!({
            "station_id": event.station_id,
        }));
        let payload = match payload {
            Ok(p) => p,
            Err(_) => return None,
        };
        Some(CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: canon_core::AggregateId::from_uuid(event.station_id),
            command_type: "CheckStockLevel".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: event.station_id,
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
    async fn ship_arrived_handler_produces_record_docking_command() {
        let handler = ShipArrivedHandler;
        let station_id = Uuid::new_v4();
        let ship_id = Uuid::new_v4();
        let route_id = Uuid::new_v4();
        let events = vec![SharedShipArrivedAtStation {
            route_id,
            ship_id,
            station_id,
        }];
        let result = handler.handle(events).await;
        assert!(result.is_ok());
        let cmd = result.unwrap();
        assert!(cmd.is_some());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.command_type, "RecordDocking");
        assert_eq!(*cmd.aggregate_id.as_uuid(), station_id);
    }

    #[tokio::test]
    async fn ship_arrived_handler_returns_none_for_empty_events() {
        let handler = ShipArrivedHandler;
        let events: Vec<SharedShipArrivedAtStation> = vec![];
        let result = handler.handle(events).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn cargo_unloaded_handler_produces_record_cargo_received_command() {
        let handler = CargoUnloadedHandler;
        let manifest_id = Uuid::new_v4();
        let item_id = Uuid::new_v4();
        let events = vec![SharedCargoUnloaded {
            manifest_id,
            item_id,
        }];
        let result = handler.handle(events).await;
        assert!(result.is_ok());
        let cmd = result.unwrap();
        assert!(cmd.is_some());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.command_type, "RecordCargoReceived");
    }

    #[tokio::test]
    async fn stock_level_monitor_produces_check_stock_level_command() {
        let handler = StockLevelMonitorHandler;
        let station_id = Uuid::new_v4();
        let manifest_id = Uuid::new_v4();
        let events = vec![CargoReceived {
            station_id,
            manifest_id,
            weight_kg: 50.0,
        }];
        let result = handler.handle(events).await;
        assert!(result.is_ok());
        let cmd = result.unwrap();
        assert!(cmd.is_some());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.command_type, "CheckStockLevel");
        assert_eq!(*cmd.aggregate_id.as_uuid(), station_id);
    }
}
