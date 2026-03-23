use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fleet events (aggregate: Ship)
// ---------------------------------------------------------------------------

#[canon_core::event(Ship, version = 1)]
pub struct ShipRegistered {
    pub ship_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
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

// ---------------------------------------------------------------------------
// Cargo events (aggregate: ManifestState)
// ---------------------------------------------------------------------------

#[canon_core::event(ManifestState, version = 1)]
pub struct ManifestCreated {
    pub manifest_id: Uuid,
    pub ship_id: Uuid,
    pub voyage_id: Uuid,
}

#[canon_core::event(ManifestState, version = 2)]
pub struct CargoLoaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
    pub weight_kg: f32,
    pub description: String,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct UnloadingStarted {
    pub manifest_id: Uuid,
    pub station_id: Uuid,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct CargoUnloaded {
    pub manifest_id: Uuid,
    pub item_id: Uuid,
}

#[canon_core::event(ManifestState, version = 1)]
pub struct ManifestClosed {
    pub manifest_id: Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum CargoEvent {
    ManifestCreated(ManifestCreated),
    CargoLoaded(CargoLoaded),
    UnloadingStarted(UnloadingStarted),
    CargoUnloaded(CargoUnloaded),
    ManifestClosed(ManifestClosed),
}

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

// ---------------------------------------------------------------------------
// Supply events (aggregate: Inventory)
// ---------------------------------------------------------------------------

#[canon_core::event(Inventory, version = 1)]
pub struct StockRecorded {
    pub inventory_id: Uuid,
    pub station_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct ResupplyRequested {
    pub inventory_id: Uuid,
    pub station_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct ResupplyDispatched {
    pub inventory_id: Uuid,
    pub ship_id: Uuid,
    pub fuel_kg: f32,
}

#[canon_core::event(Inventory, version = 1)]
pub struct DeliveryConfirmed {
    pub inventory_id: Uuid,
    pub ship_id: Uuid,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum SupplyEvent {
    StockRecorded(StockRecorded),
    ResupplyRequested(ResupplyRequested),
    ResupplyDispatched(ResupplyDispatched),
    DeliveryConfirmed(DeliveryConfirmed),
}

// ---------------------------------------------------------------------------
// Station events (aggregate: Station)
// ---------------------------------------------------------------------------

#[canon_core::event(Station, version = 1)]
pub struct StationRegistered {
    pub station_id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
}

#[canon_core::event(Station, version = 1)]
pub struct ShipDocked {
    pub station_id: Uuid,
    pub ship_id: Uuid,
}

#[canon_core::event(Station, version = 1)]
pub struct CargoReceived {
    pub station_id: Uuid,
    pub manifest_id: Uuid,
    pub weight_kg: f32,
}

#[canon_core::event(Station, version = 1)]
pub struct StationStockLow {
    pub station_id: Uuid,
    pub current_stock_kg: f32,
    pub threshold_kg: f32,
}

#[canon_core::event(Station, version = 1)]
pub struct CapacityUpdated {
    pub station_id: Uuid,
    pub capacity_kg: f32,
}

#[canon_core::event(Station, version = 1)]
pub struct StationOffline {
    pub station_id: Uuid,
}

#[canon_core::event(Station, version = 1)]
pub struct StockDrained {
    pub station_id: Uuid,
    pub drain_kg: f32,
    pub remaining_kg: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum StationEvent {
    StationRegistered(StationRegistered),
    ShipDocked(ShipDocked),
    CargoReceived(CargoReceived),
    StationStockLow(StationStockLow),
    CapacityUpdated(CapacityUpdated),
    StationOffline(StationOffline),
    StockDrained(StockDrained),
}

// ---------------------------------------------------------------------------
// Top-level demo event
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "domain")]
pub enum DemoEvent {
    Fleet(FleetEvent),
    Cargo(CargoEvent),
    Navigation(NavigationEvent),
    Supply(SupplyEvent),
    Station(StationEvent),
}
