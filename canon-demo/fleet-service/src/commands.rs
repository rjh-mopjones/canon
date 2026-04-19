use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fleet commands (aggregate: Ship)
// ---------------------------------------------------------------------------

#[canon_core::command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub capacity_kg: f32,
    #[serde(default)]
    pub home_station: Option<Uuid>,
}

#[canon_core::command(Ship, version = 1, produces = [RouteAssigned])]
pub struct AssignRoute {
    pub ship_id: Uuid,
    pub route_id: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
    pub destination: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ResupplyScheduled])]
pub struct ScheduleResupply {
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDecommissioned])]
pub struct DecommissionShip {
    pub ship_id: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDockedAtStation])]
pub struct DockShip {
    pub ship_id: Uuid,
    pub station_id: Uuid,
}
