//! Station inventory projection — the showcase read-ready projection in the demo.
//!
//! Materialised view of station state, updated idempotently from events.
//! Schema:
//! ```sql
//! CREATE TABLE station_inventory (
//!     station_id       UUID PRIMARY KEY,
//!     name             TEXT NOT NULL,
//!     capacity_kg      INT NOT NULL,
//!     current_stock_kg INT NOT NULL DEFAULT 0,
//!     last_docking     TIMESTAMPTZ,
//!     updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! ```

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::events::{CapacityUpdated, CargoReceived, ShipDocked, StationRegistered};

/// Read model for station inventory.
#[canon_core::projection]
pub struct StationInventory {
    /// In-memory representation: station_id -> row.
    pub stations: HashMap<Uuid, StationInventoryRow>,
}

/// A single row in the station inventory materialised view.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StationInventoryRow {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
    pub last_docking: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[allow(clippy::derivable_impls)]
impl Default for StationInventory {
    fn default() -> Self {
        Self {
            stations: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Projection handlers — idempotent apply for each event type
// ---------------------------------------------------------------------------

#[canon_core::projection_handler(StationInventory)]
impl StationRegisteredProjectionHandler {
    fn apply(&self, event: &StationRegistered, store: &mut StationInventory) {
        let now = Utc::now();
        store.stations.insert(
            event.station_id,
            StationInventoryRow {
                station_id: event.station_id,
                name: event.name.clone(),
                capacity_kg: event.capacity_kg,
                current_stock_kg: 0.0,
                last_docking: None,
                updated_at: now,
            },
        );
    }
}

#[canon_core::projection_handler(StationInventory)]
impl ShipDockedProjectionHandler {
    fn apply(&self, event: &ShipDocked, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.last_docking = Some(Utc::now());
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl CargoReceivedProjectionHandler {
    fn apply(&self, event: &CargoReceived, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.current_stock_kg += event.weight_kg;
            row.updated_at = Utc::now();
        }
    }
}

#[canon_core::projection_handler(StationInventory)]
impl CapacityUpdatedProjectionHandler {
    fn apply(&self, event: &CapacityUpdated, store: &mut StationInventory) {
        if let Some(row) = store.stations.get_mut(&event.station_id) {
            row.capacity_kg = event.capacity_kg;
            row.updated_at = Utc::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::ProjectionHandler;

    #[test]
    fn station_registered_creates_row() {
        let handler = StationRegisteredProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let event = StationRegistered {
            station_id,
            name: "Alpha Station".to_string(),
            capacity_kg: 1000.0,
        };
        handler.apply(&event, &mut store);
        assert!(store.stations.contains_key(&station_id));
        let row = &store.stations[&station_id];
        assert_eq!(row.name, "Alpha Station");
        assert!((row.capacity_kg - 1000.0).abs() < f32::EPSILON);
        assert!((row.current_stock_kg - 0.0).abs() < f32::EPSILON);
        assert!(row.last_docking.is_none());
    }

    #[test]
    fn station_registered_is_idempotent() {
        let handler = StationRegisteredProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let event = StationRegistered {
            station_id,
            name: "Alpha Station".to_string(),
            capacity_kg: 1000.0,
        };
        handler.apply(&event, &mut store);
        handler.apply(&event, &mut store);
        assert_eq!(store.stations.len(), 1);
    }

    #[test]
    fn ship_docked_updates_last_docking() {
        let handler_reg = StationRegisteredProjectionHandler;
        let handler_dock = ShipDockedProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let reg_event = StationRegistered {
            station_id,
            name: "Alpha".to_string(),
            capacity_kg: 1000.0,
        };
        handler_reg.apply(&reg_event, &mut store);
        assert!(store.stations[&station_id].last_docking.is_none());

        let dock_event = ShipDocked {
            station_id,
            ship_id: Uuid::new_v4(),
        };
        handler_dock.apply(&dock_event, &mut store);
        assert!(store.stations[&station_id].last_docking.is_some());
    }

    #[test]
    fn cargo_received_increases_stock() {
        let handler_reg = StationRegisteredProjectionHandler;
        let handler_cargo = CargoReceivedProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let reg_event = StationRegistered {
            station_id,
            name: "Alpha".to_string(),
            capacity_kg: 1000.0,
        };
        handler_reg.apply(&reg_event, &mut store);

        let cargo_event = CargoReceived {
            station_id,
            manifest_id: Uuid::new_v4(),
            weight_kg: 250.0,
        };
        handler_cargo.apply(&cargo_event, &mut store);
        assert!((store.stations[&station_id].current_stock_kg - 250.0).abs() < f32::EPSILON);

        // Apply again — stock accumulates (idempotency is at the framework level via dedup)
        handler_cargo.apply(&cargo_event, &mut store);
        assert!((store.stations[&station_id].current_stock_kg - 500.0).abs() < f32::EPSILON);
    }

    #[test]
    fn capacity_updated_changes_capacity() {
        let handler_reg = StationRegisteredProjectionHandler;
        let handler_cap = CapacityUpdatedProjectionHandler;
        let mut store = StationInventory::default();
        let station_id = Uuid::new_v4();
        let reg_event = StationRegistered {
            station_id,
            name: "Alpha".to_string(),
            capacity_kg: 1000.0,
        };
        handler_reg.apply(&reg_event, &mut store);

        let cap_event = CapacityUpdated {
            station_id,
            capacity_kg: 2000.0,
        };
        handler_cap.apply(&cap_event, &mut store);
        assert!((store.stations[&station_id].capacity_kg - 2000.0).abs() < f32::EPSILON);
    }

    #[test]
    fn ship_docked_on_unknown_station_is_noop() {
        let handler = ShipDockedProjectionHandler;
        let mut store = StationInventory::default();
        let event = ShipDocked {
            station_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
        };
        handler.apply(&event, &mut store);
        assert!(store.stations.is_empty());
    }
}
