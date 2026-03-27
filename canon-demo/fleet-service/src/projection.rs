use crate::events::{
    ResupplyScheduled, RouteAssigned, ShipDecommissioned, ShipDeparted, ShipDockedAtStation,
    ShipRegistered,
};

/// Read model for fleet ship state.
#[canon_core::projection]
pub struct ShipReadModel {
    pub ship_id: uuid::Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub status: String,
    pub fuel_kg: f32,
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipRegisteredProjectionHandler {
    fn apply(&self, event: &ShipRegistered, store: &mut ShipReadModel) {
        store.ship_id = event.ship_id;
        store.name = event.name.clone();
        store.capacity_kg = event.capacity_kg;
        store.status = "Docked".to_string();
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl RouteAssignedProjectionHandler {
    fn apply(&self, event: &RouteAssigned, store: &mut ShipReadModel) {
        let _ = event;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDepartedProjectionHandler {
    fn apply(&self, event: &ShipDeparted, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "InTransit".to_string();
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ResupplyScheduledProjectionHandler {
    fn apply(&self, event: &ResupplyScheduled, store: &mut ShipReadModel) {
        store.fuel_kg = event.fuel_kg;
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDecommissionedProjectionHandler {
    fn apply(&self, event: &ShipDecommissioned, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "Decommissioned".to_string();
    }
}

#[canon_core::projection_handler(ShipReadModel)]
impl ShipDockedAtStationProjectionHandler {
    fn apply(&self, event: &ShipDockedAtStation, store: &mut ShipReadModel) {
        let _ = event;
        store.status = "Docked".to_string();
    }
}
