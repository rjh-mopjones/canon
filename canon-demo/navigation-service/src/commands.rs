use uuid::Uuid;

// ---------------------------------------------------------------------------
// Navigation commands (aggregate: Route)
// ---------------------------------------------------------------------------

#[canon_core::command(Route, version = 1, produces = [RoutePlanned])]
pub struct PlanRoute {
    pub route_id: Uuid,
    pub ship_id: Uuid,
    pub waypoints: Vec<Uuid>,
}

#[canon_core::command(Route, version = 1, produces = [PositionUpdated])]
pub struct RecordDeparture {
    pub route_id: Uuid,
    pub ship_id: Uuid,
}

#[canon_core::command(Route, version = 1, produces = [PositionUpdated])]
pub struct UpdatePosition {
    pub route_id: Uuid,
    pub waypoint_id: Uuid,
}

#[canon_core::command(Route, version = 1, produces = [ShipArrivedAtStation])]
pub struct RecordArrival {
    pub route_id: Uuid,
    pub station_id: Uuid,
}
