use canon_core::event_combiner;
use canon_demo_shared::events::*;

use crate::aggregate::{Ship, ShipStatus};

#[event_combiner(Ship, version = 1)]
impl ShipRegistered {
    fn combine(&self, state: &mut Ship) {
        state.name = self.name.clone();
        state.capacity_kg = self.capacity_kg;
        state.status = ShipStatus::Docked;
    }
}

#[event_combiner(Ship, version = 1)]
impl RouteAssigned {
    fn combine(&self, state: &mut Ship) {
        state.assigned_route = Some(self.route_id);
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDeparted {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::InTransit;
    }
}

#[event_combiner(Ship, version = 1)]
impl ResupplyScheduled {
    fn combine(&self, state: &mut Ship) {
        state.fuel_kg = self.fuel_kg;
    }
}

#[event_combiner(Ship, version = 1)]
impl ShipDecommissioned {
    fn combine(&self, state: &mut Ship) {
        state.status = ShipStatus::Decommissioned;
    }
}
