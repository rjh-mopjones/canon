//! Anti-corruption layer types for foreign events consumed by station-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields station-service cares about.

use uuid::Uuid;

/// Inbound representation of navigation-service's ShipArrivedAtStation event.
/// Station-service uses route_id so repeat arrivals at the same station remain distinct.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipArrivedAtStation {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub station_id: Uuid,
}
