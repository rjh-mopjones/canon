use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fleet events (aggregate: Ship)
// ---------------------------------------------------------------------------

#[canon_core::event(Ship, version = 1)]
pub struct ShipRegistered {
    pub ship_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    #[serde(default)]
    pub home_station: Option<Uuid>,
}

#[canon_core::event(Ship, version = 1)]
pub struct RouteAssigned {
    pub ship_id: Uuid,
    pub route_id: Uuid,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDeparted {
    pub ship_id: Uuid,
    pub destination: Uuid,
    pub fuel_at_departure: f32,
}

#[canon_core::event(Ship, version = 1)]
pub struct ResupplyScheduled {
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDecommissioned {
    pub ship_id: Uuid,
}

#[canon_core::event(Ship, version = 1)]
pub struct ShipDockedAtStation {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum FleetEvent {
    ShipRegistered(ShipRegistered),
    RouteAssigned(RouteAssigned),
    ShipDeparted(ShipDeparted),
    ResupplyScheduled(ResupplyScheduled),
    ShipDecommissioned(ShipDecommissioned),
    ShipDockedAtStation(ShipDockedAtStation),
}
