use uuid::Uuid;

use crate::aggregate::Station;

// ---------------------------------------------------------------------------
// Local event types for the Station aggregate.
//
// These mirror the shared event definitions in canon-demo-shared but are
// defined locally so that the orphan rule allows implementing EventCombiner<Station>
// for each event type in this crate.
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
    pub current_fuel_kg: f32,
    pub threshold_kg: f32,
}

#[canon_core::event(Station, version = 1)]
pub struct CapacityUpdated {
    pub station_id: Uuid,
    pub capacity_kg: f32,
}

// ---------------------------------------------------------------------------
// Event combiners — synchronous, pure state folding
// ---------------------------------------------------------------------------

#[canon_core::event_combiner(Station, version = 1)]
impl StationRegistered {
    fn combine(&self, state: &mut Station) {
        state.name = self.name.clone();
        state.capacity_kg = self.capacity_kg;
        state.registered = true;
    }
}

#[canon_core::event_combiner(Station, version = 1)]
impl ShipDocked {
    fn combine(&self, state: &mut Station) {
        if !state.docked_ships.contains(&self.ship_id) {
            state.docked_ships.push(self.ship_id);
        }
    }
}

#[canon_core::event_combiner(Station, version = 1)]
impl CargoReceived {
    fn combine(&self, state: &mut Station) {
        state.current_stock_kg += self.weight_kg;
    }
}

#[canon_core::event_combiner(Station, version = 1)]
impl StationStockLow {
    fn combine(&self, _state: &mut Station) {
        // StationStockLow is a notification event — no state mutation required.
        // The stock level is already updated by CargoReceived.
    }
}

#[canon_core::event_combiner(Station, version = 1)]
impl CapacityUpdated {
    fn combine(&self, state: &mut Station) {
        state.capacity_kg = self.capacity_kg;
    }
}
