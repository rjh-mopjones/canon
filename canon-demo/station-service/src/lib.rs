//! # station-service
//!
//! Station aggregate implementation for the canon-demo. Owns the primary
//! read-ready projection — the station inventory materialised view.
//!
//! ## Aggregate: Station
//!
//! Commands: `RegisterStation`, `RecordDocking`, `RecordCargoReceived`, `UpdateCapacity`
//! Events: `StationRegistered`, `ShipDocked`, `CargoReceived`, `StationStockLow`, `CapacityUpdated`, `StationOffline`
//!
//! ## Cross-service flows
//!
//! - **Consume:** `canon.navigation.events` — `ShipArrivedAtStation` → RecordDocking command
//! - **Consume:** `canon.cargo.events` — `CargoUnloaded` → RecordCargoReceived command
//! - **Publish:** `StationStockLow` → `canon.station.events` → consumed by supply-service

pub mod aggregate;
pub mod commands;
pub mod error;
pub mod event_handlers;
pub mod events;
pub mod handlers;
pub mod inbound;
pub mod projection;
