//! Route read model projection.
//!
//! Maintains a denormalized view of route state for query access.
//! The projection is idempotent: applying the same event twice produces
//! the same result.

use crate::events::{PositionUpdated, RoutePlanned, ShipArrivedAtStation};

/// Route read model -- denormalized view for queries.
#[canon_core::projection]
pub struct RouteReadModel {
    pub route_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
    pub current_waypoint: Option<uuid::Uuid>,
    pub arrived: bool,
}

// ---------------------------------------------------------------------------
// Projection handlers -- apply events to the read model
// ---------------------------------------------------------------------------

#[canon_core::projection_handler(RouteReadModel)]
impl RoutePlannedProjectionHandler {
    fn apply(&self, event: &RoutePlanned, store: &mut RouteReadModel) {
        store.route_id = event.route_id;
        store.ship_id = event.ship_id;
        store.current_waypoint = event.waypoints.first().copied();
        store.arrived = false;
    }
}

#[canon_core::projection_handler(RouteReadModel)]
impl PositionUpdatedProjectionHandler {
    fn apply(&self, event: &PositionUpdated, store: &mut RouteReadModel) {
        store.current_waypoint = Some(event.waypoint_id);
    }
}

#[canon_core::projection_handler(RouteReadModel)]
impl ShipArrivedProjectionHandler {
    fn apply(&self, event: &ShipArrivedAtStation, store: &mut RouteReadModel) {
        store.current_waypoint = Some(event.station_id);
        store.arrived = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::ProjectionHandler;
    use uuid::Uuid;

    fn default_read_model() -> RouteReadModel {
        RouteReadModel {
            route_id: Uuid::nil(),
            ship_id: Uuid::nil(),
            current_waypoint: None,
            arrived: false,
        }
    }

    #[test]
    fn route_planned_sets_initial_state() {
        let handler = RoutePlannedProjectionHandler;
        let mut model = default_read_model();
        let route_id = Uuid::new_v4();
        let ship_id = Uuid::new_v4();
        let wp1 = Uuid::new_v4();

        let event = RoutePlanned {
            route_id,
            ship_id,
            waypoints: vec![wp1, Uuid::new_v4()],
        };

        ProjectionHandler::<RouteReadModel>::apply(&handler, &event, &mut model);

        assert_eq!(model.route_id, route_id);
        assert_eq!(model.ship_id, ship_id);
        assert_eq!(model.current_waypoint, Some(wp1));
        assert!(!model.arrived);
    }

    #[test]
    fn position_updated_advances_waypoint() {
        let handler = PositionUpdatedProjectionHandler;
        let mut model = default_read_model();
        let wp = Uuid::new_v4();

        let event = PositionUpdated {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            waypoint_id: wp,
        };

        ProjectionHandler::<RouteReadModel>::apply(&handler, &event, &mut model);

        assert_eq!(model.current_waypoint, Some(wp));
    }

    #[test]
    fn ship_arrived_marks_arrival() {
        let handler = ShipArrivedProjectionHandler;
        let mut model = default_read_model();
        let station_id = Uuid::new_v4();

        let event = ShipArrivedAtStation {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            station_id,
        };

        ProjectionHandler::<RouteReadModel>::apply(&handler, &event, &mut model);

        assert_eq!(model.current_waypoint, Some(station_id));
        assert!(model.arrived);
    }

    #[test]
    fn projection_is_idempotent() {
        let handler = ShipArrivedProjectionHandler;
        let mut model = default_read_model();
        let station_id = Uuid::new_v4();

        let event = ShipArrivedAtStation {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            station_id,
        };

        // Apply twice -- must produce same result
        ProjectionHandler::<RouteReadModel>::apply(&handler, &event, &mut model);
        let first_state = model.clone();
        ProjectionHandler::<RouteReadModel>::apply(&handler, &event, &mut model);

        assert_eq!(model.current_waypoint, first_state.current_waypoint);
        assert_eq!(model.arrived, first_state.arrived);
    }

    #[test]
    fn read_model_serde_roundtrip() {
        let model = RouteReadModel {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            current_waypoint: Some(Uuid::new_v4()),
            arrived: true,
        };
        let json = serde_json::to_vec(&model).expect("serialize");
        let back: RouteReadModel = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(back.route_id, model.route_id);
        assert_eq!(back.ship_id, model.ship_id);
        assert_eq!(back.current_waypoint, model.current_waypoint);
        assert_eq!(back.arrived, model.arrived);
    }
}
