//! Anti-corruption layer types for foreign events consumed by supply-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields supply-service cares about.

use uuid::Uuid;

/// Inbound representation of station-service's StationStockLow event.
/// Supply-service uses station_id + current_stock_kg + threshold_kg
/// to submit a RequestResupply command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundStationStockLow {
    pub station_id: Uuid,
    pub current_stock_kg: f32,
    pub threshold_kg: f32,
}
