//! Anti-corruption layer types for foreign events consumed by fleet-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields fleet-service cares about.

use uuid::Uuid;

/// Inbound representation of navigation-service's ShipArrivedAtStation event.
/// Fleet-service uses ship_id + station_id to submit a DockShip command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipArrivedAtStation {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}

/// Inbound representation of supply-service's ResupplyDispatched event.
/// Fleet-service uses ship_id + fuel_kg to submit a ScheduleResupply command.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundResupplyDispatched {
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}
