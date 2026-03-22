use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fleet commands (aggregate: Ship)
// ---------------------------------------------------------------------------

#[canon_core::command(Ship, version = 1, produces = [ShipRegistered])]
pub struct RegisterShip {
    pub name: String,
    pub capacity_kg: f32,
}

#[canon_core::command(Ship, version = 1, produces = [RouteAssigned])]
pub struct AssignRoute {
    pub ship_id: Uuid,
    pub route_id: Uuid,
}

#[canon_core::command(Ship, version = 1, produces = [ShipDeparted])]
pub struct DepartForStation {
    pub ship_id: Uuid,
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

// ---------------------------------------------------------------------------
// Cargo commands (aggregate: ManifestState)
// ---------------------------------------------------------------------------

#[canon_core::command(ManifestState, version = 1, produces = [ManifestCreated])]
pub struct CreateManifest {
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [CargoLoaded])]
pub struct LoadCargo {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
    pub weight_kg: f32,
    pub description: String,
}

#[canon_core::command(ManifestState, version = 1, produces = [UnloadingStarted])]
pub struct BeginUnloading {
    pub manifest_id: Uuid,
    pub station_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [CargoUnloaded])]
pub struct RecordUnloaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
}

#[canon_core::command(ManifestState, version = 1, produces = [ManifestClosed])]
pub struct CloseManifest {
    pub manifest_id: Uuid,
}

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

// ---------------------------------------------------------------------------
// Supply commands (aggregate: Inventory)
// ---------------------------------------------------------------------------

#[canon_core::command(Inventory, version = 1, produces = [StockRecorded])]
pub struct RecordStock {
    pub station_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Inventory, version = 1, produces = [ResupplyRequested])]
pub struct RequestResupply {
    pub station_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::command(Inventory, version = 1, produces = [ResupplyDispatched])]
pub struct DispatchResupply {
    pub inventory_id: Uuid,
    pub ship_id: Uuid,
}

#[canon_core::command(Inventory, version = 1, produces = [DeliveryConfirmed])]
pub struct ConfirmDelivery {
    pub inventory_id: Uuid,
}

// ---------------------------------------------------------------------------
// Station commands (aggregate: Station)
// ---------------------------------------------------------------------------

#[canon_core::command(Station, version = 1, produces = [StationRegistered])]
pub struct RegisterStation {
    pub name: String,
    pub capacity_kg: f32,
}

#[canon_core::command(Station, version = 1, produces = [ShipDocked])]
pub struct RecordDocking {
    pub station_id: Uuid,
    pub ship_id: Uuid,
}

#[canon_core::command(Station, version = 1, produces = [CargoReceived])]
pub struct RecordCargoReceived {
    pub station_id: Uuid,
    pub manifest_id: Uuid,
    pub weight_kg: f32,
}

#[canon_core::command(Station, version = 1, produces = [CapacityUpdated])]
pub struct UpdateCapacity {
    pub station_id: Uuid,
    pub capacity_kg: f32,
}
