//! Anti-corruption layer types for foreign events consumed by station-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields station-service cares about.

use uuid::Uuid;

/// Inbound representation of navigation-service's ShipArrivedAtStation event.
/// Station-service uses ship_id + station_id to submit a RecordDocking command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipArrivedAtStation {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}
