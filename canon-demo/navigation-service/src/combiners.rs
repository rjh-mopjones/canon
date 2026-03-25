use crate::aggregate::Route;
use crate::events::{
    PositionUpdated, PositionUpdatedV1HasCombiner, RoutePlanned, RoutePlannedV1HasCombiner,
    ShipArrivedAtStation, ShipArrivedAtStationV1HasCombiner,
};

// ---------------------------------------------------------------------------
// Event combiners — synchronous, pure state folding
// ---------------------------------------------------------------------------

#[canon_core::event_combiner(Route, version = 1)]
impl RoutePlanned {
    fn combine(&self, state: &mut Route) {
        state.ship_id = Some(self.ship_id);
        state.waypoints = self.waypoints.clone();
        state.current_waypoint_index = 0;
        state.arrived = false;
    }
}

#[canon_core::event_combiner(Route, version = 1)]
impl PositionUpdated {
    fn combine(&self, state: &mut Route) {
        // Advance to the waypoint matching the update, or increment index
        if let Some(idx) = state.waypoints.iter().position(|w| *w == self.waypoint_id) {
            state.current_waypoint_index = idx;
        } else {
            state.current_waypoint_index += 1;
        }
    }
}

#[canon_core::event_combiner(Route, version = 1)]
impl ShipArrivedAtStation {
    fn combine(&self, state: &mut Route) {
        state.arrived = true;
    }
}
