//! Route read model projection.
//!
//! Maintains a denormalized view of route state for query access.
//! The projection is idempotent: applying the same event twice produces
//! the same result.
//!
//! Traits are implemented manually because the shared crate owns the
//! event types (Rust orphan rules).
//!
//! Schema (YugabyteDB):
//! ```sql
//! CREATE TABLE route_read_models (
//!     route_id         UUID PRIMARY KEY,
//!     ship_id          UUID NOT NULL,
//!     current_waypoint UUID,
//!     arrived          BOOLEAN NOT NULL DEFAULT FALSE,
//!     updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! ```

use canon_core::ProjectionHandler;
use canon_demo_shared::events::{PositionUpdated, RoutePlanned, ShipArrivedAtStation};

/// Route read model -- denormalized view for queries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RouteReadModel {
    pub route_id: uuid::Uuid,
    pub ship_id: uuid::Uuid,
    pub current_waypoint: Option<uuid::Uuid>,
    pub arrived: bool,
}

impl RouteReadModel {
    /// Returns the snake_case projection identifier for checkpoint tracking.
    pub fn projection_id(&self) -> &str {
        "route_read_model"
    }
}

canon_core::__submit! {
    canon_core::ProjectionRegistration {
        projection_type_name: "RouteReadModel",
        projection_id: "route_read_model",
    }
}

// ---------------------------------------------------------------------------
// Projection handlers -- apply events to the read model
// ---------------------------------------------------------------------------

pub struct RoutePlannedProjectionHandler;

impl ProjectionHandler<RouteReadModel> for RoutePlannedProjectionHandler {
    type Event = RoutePlanned;

    fn apply(&self, event: &Self::Event, store: &mut RouteReadModel) {
        store.route_id = event.route_id;
        store.ship_id = event.ship_id;
        store.current_waypoint = event.waypoints.first().copied();
        store.arrived = false;
    }
}

canon_core::__submit! {
    canon_core::ProjectionHandlerRegistration {
        projection_type_name: "RouteReadModel",
        handler_type_name: "RoutePlannedProjectionHandler",
    }
}

pub struct PositionUpdatedProjectionHandler;

impl ProjectionHandler<RouteReadModel> for PositionUpdatedProjectionHandler {
    type Event = PositionUpdated;

    fn apply(&self, event: &Self::Event, store: &mut RouteReadModel) {
        store.current_waypoint = Some(event.waypoint_id);
    }
}

canon_core::__submit! {
    canon_core::ProjectionHandlerRegistration {
        projection_type_name: "RouteReadModel",
        handler_type_name: "PositionUpdatedProjectionHandler",
    }
}

pub struct ShipArrivedProjectionHandler;

impl ProjectionHandler<RouteReadModel> for ShipArrivedProjectionHandler {
    type Event = ShipArrivedAtStation;

    fn apply(&self, event: &Self::Event, store: &mut RouteReadModel) {
        store.current_waypoint = Some(event.station_id);
        store.arrived = true;
    }
}

canon_core::__submit! {
    canon_core::ProjectionHandlerRegistration {
        projection_type_name: "RouteReadModel",
        handler_type_name: "ShipArrivedProjectionHandler",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn projection_id_is_correct() {
        let model = default_read_model();
        assert_eq!(model.projection_id(), "route_read_model");
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
