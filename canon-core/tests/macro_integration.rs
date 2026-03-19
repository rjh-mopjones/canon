//! Integration tests for all eight canon-core proc-macros.
//!
//! These tests verify that:
//! 1. All macros compile and generate correct trait impls
//! 2. #[aggregate] hydrate() dispatches via inventory-registered combiners
//! 3. #[command] generates the per-command event enum
//! 4. #[event_combiner] register and run correctly
//! 5. #[command_handler] async handler wraps sync user code
//! 6. #[event_handler] works with and without oversight
//! 7. #[projection] generates projection_id
//! 8. #[projection_handler] generates ProjectionHandler impl

use canon_core::*;
use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

// ── Domain types for testing ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum ShipStatus {
    #[default]
    Docked,
    InFlight,
    Decommissioned,
}

pub type StationId = Uuid;

#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error("ship is not docked")]
    ShipNotDocked,
}

// ── #[aggregate] ────────────────────────────────────────────────────────────

#[aggregate(snapshot_every = 50)]
pub struct Ship {
    pub status: ShipStatus,
    pub fuel_level: f32,
}

#[test]
fn aggregate_has_default() {
    let ship = Ship::default();
    assert_eq!(ship.status, ShipStatus::Docked);
    assert_eq!(ship.fuel_level, 0.0);
}

#[test]
fn aggregate_snapshot_every() {
    assert_eq!(Ship::SNAPSHOT_EVERY, Some(50));
}

#[test]
fn aggregate_serde_roundtrip() {
    let ship = Ship {
        status: ShipStatus::InFlight,
        fuel_level: 75.5,
    };
    let json = serde_json::to_vec(&ship).expect("serialize");
    let back: Ship = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(back.status, ShipStatus::InFlight);
    assert_eq!(back.fuel_level, 75.5);
}

// ── #[event] ────────────────────────────────────────────────────────────────

#[event(Ship, version = 1)]
pub struct ShipDeparted {
    pub destination: StationId,
    pub fuel_at_departure: f32,
}

#[event(Ship, version = 1)]
pub struct ShipRegistered {
    pub name: String,
    pub initial_fuel: f32,
}

#[test]
fn event_serde_roundtrip() {
    let dest = Uuid::new_v4();
    let event = ShipDeparted {
        destination: dest,
        fuel_at_departure: 100.0,
    };
    let json = serde_json::to_vec(&event).expect("serialize");
    let back: ShipDeparted = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(back.destination, dest);
    assert_eq!(back.fuel_at_departure, 100.0);
}

// ── #[command] ──────────────────────────────────────────────────────────────

#[command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub destination: StationId,
}

#[command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub initial_fuel: f32,
}

#[test]
fn command_event_enum_generated() {
    let dest = Uuid::new_v4();
    let event = DepartForStationEvent::ShipDeparted(ShipDeparted {
        destination: dest,
        fuel_at_departure: 100.0,
    });
    // Just verify it compiles and the enum variant exists
    match event {
        DepartForStationEvent::ShipDeparted(e) => {
            assert_eq!(e.destination, dest);
        }
    }
}

// ── #[event_combiner] ──────────────────────────────────────────────────────

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InFlight;
        state.fuel_level -= self.fuel_at_departure * 0.1;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipRegistered {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Docked;
        state.fuel_level = self.initial_fuel;
    }
}

#[test]
fn event_combiner_trait_impl() {
    let event = ShipDeparted {
        destination: Uuid::new_v4(),
        fuel_at_departure: 50.0,
    };
    let mut ship = Ship {
        status: ShipStatus::Docked,
        fuel_level: 100.0,
    };
    <ShipDeparted as EventCombiner<Ship>>::combine(&event, &mut ship);
    assert_eq!(ship.status, ShipStatus::InFlight);
    assert_eq!(ship.fuel_level, 95.0); // 100 - 50 * 0.1
}

#[test]
fn aggregate_hydrate_dispatches_to_combiners() {
    let mut ship = Ship::default();

    let register_payload = serde_json::to_vec(&ShipRegistered {
        name: "USS Test".to_string(),
        initial_fuel: 100.0,
    })
    .expect("serialize");

    let depart_payload = serde_json::to_vec(&ShipDeparted {
        destination: Uuid::new_v4(),
        fuel_at_departure: 100.0,
    })
    .expect("serialize");

    let agg_id = AggregateId::new();
    let corr = Uuid::new_v4();

    let events = vec![
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: agg_id.clone(),
            version: Version::initial(),
            event_type: "ShipRegistered".to_string(),
            event_version: 1,
            payload: Bytes::from(register_payload),
            correlation_id: corr,
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        },
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: agg_id.clone(),
            version: Version::initial().next(),
            event_type: "ShipDeparted".to_string(),
            event_version: 1,
            payload: Bytes::from(depart_payload),
            correlation_id: corr,
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        },
    ];

    Ship::hydrate(&mut ship, events.into_iter()).expect("hydrate");

    assert_eq!(ship.status, ShipStatus::InFlight);
    assert_eq!(ship.fuel_level, 90.0); // 100.0 - 100.0 * 0.1
}

// ── #[command_handler] ─────────────────────────────────────────────────────

#[command_handler(Ship, version = 1)]
impl DepartForStationHandler {
    type Error = FleetError;

    fn handle(
        &self,
        state: &Ship,
        cmd: DepartForStation,
    ) -> Result<Vec<DepartForStationEvent>, FleetError> {
        if state.status != ShipStatus::Docked {
            return Err(FleetError::ShipNotDocked);
        }
        Ok(vec![DepartForStationEvent::ShipDeparted(ShipDeparted {
            destination: cmd.destination,
            fuel_at_departure: state.fuel_level,
        })])
    }
}

#[tokio::test]
async fn command_handler_produces_events() {
    let handler = DepartForStationHandler;
    let ship = Ship {
        status: ShipStatus::Docked,
        fuel_level: 80.0,
    };
    let dest = Uuid::new_v4();
    let cmd = DepartForStation { destination: dest };

    let events = CommandHandler::<Ship>::handle(&handler, &ship, cmd)
        .await
        .expect("handle");
    assert_eq!(events.len(), 1);
    match &events[0] {
        DepartForStationEvent::ShipDeparted(e) => {
            assert_eq!(e.destination, dest);
            assert_eq!(e.fuel_at_departure, 80.0);
        }
    }
}

#[tokio::test]
async fn command_handler_rejects_invalid_state() {
    let handler = DepartForStationHandler;
    let ship = Ship {
        status: ShipStatus::InFlight,
        fuel_level: 80.0,
    };
    let cmd = DepartForStation {
        destination: Uuid::new_v4(),
    };

    let result = CommandHandler::<Ship>::handle(&handler, &ship, cmd).await;
    assert!(result.is_err());
}

// ── #[event_handler] ───────────────────────────────────────────────────────

// Simple event handler without window_ttl
#[event_handler]
impl SimpleEventHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let _ = events;
        None
    }
}

#[tokio::test]
async fn event_handler_simple() {
    let handler = SimpleEventHandler;
    let events = vec![ShipDeparted {
        destination: Uuid::new_v4(),
        fuel_at_departure: 50.0,
    }];
    let result = EventHandler::handle(&handler, events).await.expect("handle");
    assert!(result.is_none());
}

// Event handler with window_ttl and oversight
#[event_handler(window_ttl = "30m")]
impl WindowedEventHandler {
    #[handles(ShipDeparted, version = 1)]
    fn handle(&self, events: Vec<ShipDeparted>) -> Option<CommandEnvelope> {
        let _ = events;
        None
    }

    fn oversight(&self, accumulated: &[IncomingMessage]) -> Oversight {
        if accumulated.is_empty() {
            Oversight::NotReady
        } else {
            Oversight::Ready
        }
    }
}

#[tokio::test]
async fn event_handler_with_oversight() {
    let handler = WindowedEventHandler;
    let result = EventHandler::oversight(&handler, &[]);
    assert_eq!(result, Oversight::NotReady);
}

// ── #[projection] ──────────────────────────────────────────────────────────

#[projection]
pub struct StationInventory {
    pub station_id: Uuid,
    pub stock_levels: std::collections::HashMap<String, u32>,
}

#[test]
fn projection_id_is_snake_case() {
    let inv = StationInventory {
        station_id: Uuid::new_v4(),
        stock_levels: Default::default(),
    };
    assert_eq!(inv.projection_id(), "station_inventory");
}

#[test]
fn projection_serde_roundtrip() {
    let mut levels = std::collections::HashMap::new();
    levels.insert("fuel".to_string(), 42u32);
    let inv = StationInventory {
        station_id: Uuid::new_v4(),
        stock_levels: levels,
    };
    let json = serde_json::to_vec(&inv).expect("serialize");
    let back: StationInventory = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(back.stock_levels.get("fuel"), Some(&42));
}

// ── #[projection_handler] ──────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CargoReceived {
    pub cargo_type: String,
    pub quantity: u32,
}

#[projection_handler(StationInventory)]
impl CargoReceivedProjectionHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        *store
            .stock_levels
            .entry(event.cargo_type.clone())
            .or_insert(0) += event.quantity;
    }
}

#[test]
fn projection_handler_applies_event() {
    let handler = CargoReceivedProjectionHandler;
    let mut inv = StationInventory {
        station_id: Uuid::new_v4(),
        stock_levels: Default::default(),
    };
    let event = CargoReceived {
        cargo_type: "fuel".to_string(),
        quantity: 10,
    };

    ProjectionHandler::<StationInventory>::apply(&handler, &event, &mut inv);
    assert_eq!(inv.stock_levels.get("fuel"), Some(&10));

    ProjectionHandler::<StationInventory>::apply(&handler, &event, &mut inv);
    assert_eq!(inv.stock_levels.get("fuel"), Some(&20));
}

// ── Inventory registration verification ─────────────────────────────────────

#[test]
fn inventory_has_event_combiner_registrations() {
    let count = inventory::iter::<EventCombinerRegistration>
        .into_iter()
        .count();
    assert!(count >= 2, "expected at least 2 event combiners, got {count}");
}

#[test]
fn inventory_has_command_registrations() {
    let count = inventory::iter::<CommandRegistration>
        .into_iter()
        .count();
    assert!(count >= 2, "expected at least 2 commands, got {count}");
}

#[test]
fn inventory_has_event_registrations() {
    let count = inventory::iter::<EventRegistration>
        .into_iter()
        .count();
    assert!(count >= 2, "expected at least 2 events, got {count}");
}
