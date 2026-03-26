use uuid::Uuid;

// ---------------------------------------------------------------------------
// Navigation events (aggregate: Route)
// ---------------------------------------------------------------------------

#[canon_core::event(Route, version = 1)]
pub struct RoutePlanned {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub waypoints: Vec<Uuid>,
}

#[canon_core::event(Route, version = 1)]
pub struct PositionUpdated {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub waypoint_id: Uuid,
}

#[canon_core::event(Route, version = 1)]
pub struct ShipArrivedAtStation {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub station_id: Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum NavigationEvent {
    RoutePlanned(RoutePlanned),
    PositionUpdated(PositionUpdated),
    ShipArrivedAtStation(ShipArrivedAtStation),
}
