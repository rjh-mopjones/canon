//! Anti-corruption layer types for foreign events consumed by navigation-service.
//!
//! These are simple deserialization targets — no Canon macros needed.
//! They contain only the fields navigation-service cares about.

use uuid::Uuid;

/// Inbound representation of fleet-service's ShipDeparted event.
/// Each departure carries a distinct voyage_id so repeat visits to the same
/// destination still fan out through the pipeline.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct InboundShipDeparted {
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
    pub destination: Uuid,
}
