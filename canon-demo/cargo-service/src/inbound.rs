//! Anti-corruption layer types for foreign events consumed by cargo-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields cargo-service cares about.

use uuid::Uuid;

/// Inbound representation of navigation-service's ShipArrivedAtStation event.
/// Cargo-service uses ship_id + station_id + route_id to submit a CreateManifest command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipArrivedAtStation {
    pub ship_id: Uuid,
    pub station_id: Uuid,
    pub route_id: Uuid,
}
