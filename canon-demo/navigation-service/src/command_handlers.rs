//! Command handlers for the Route aggregate.
//!
//! Each command handler validates against the current aggregate state and
//! produces a single event on success or returns an error on rejection.

use crate::aggregate::Route;
use crate::commands::{
    PlanRoute, PlanRouteV1HasHandler, RecordArrival, RecordArrivalV1HasHandler, RecordDeparture,
    RecordDepartureV1HasHandler, UpdatePosition, UpdatePositionV1HasHandler,
};
use crate::error::NavigationError;
use crate::events::{PositionUpdated, RoutePlanned, ShipArrivedAtStation};

// ---------------------------------------------------------------------------
// PlanRoute -> RoutePlanned
// ---------------------------------------------------------------------------

#[canon_core::command_handler(Route, version = 1)]
impl PlanRouteHandler {
    type Error = NavigationError;

    fn handle(&self, state: &Route, cmd: PlanRoute) -> Result<RoutePlanned, NavigationError> {
        if cmd.waypoints.is_empty() {
            return Err(NavigationError::EmptyWaypoints);
        }
        let _ = state;
        Ok(RoutePlanned {
            route_id: cmd.route_id,
            ship_id: cmd.ship_id,
            waypoints: cmd.waypoints,
        })
    }
}

// ---------------------------------------------------------------------------
// RecordDeparture -> PositionUpdated
// ---------------------------------------------------------------------------

#[canon_core::command_handler(Route, version = 1)]
impl RecordDepartureHandler {
    type Error = NavigationError;

    fn handle(
        &self,
        state: &Route,
        cmd: RecordDeparture,
    ) -> Result<PositionUpdated, NavigationError> {
        let ship_id = state.ship_id.ok_or(NavigationError::NoShipAssigned)?;
        if cmd.ship_id != ship_id {
            return Err(NavigationError::ShipMismatch);
        }
        let first_waypoint = state
            .waypoints
            .first()
            .copied()
            .ok_or(NavigationError::EmptyWaypoints)?;
        Ok(PositionUpdated {
            route_id: cmd.route_id,
            ship_id,
            waypoint_id: first_waypoint,
        })
    }
}

// ---------------------------------------------------------------------------
// UpdatePosition -> PositionUpdated
// ---------------------------------------------------------------------------

#[canon_core::command_handler(Route, version = 1)]
impl UpdatePositionHandler {
    type Error = NavigationError;

    fn handle(
        &self,
        state: &Route,
        cmd: UpdatePosition,
    ) -> Result<PositionUpdated, NavigationError> {
        if state.arrived {
            return Err(NavigationError::PositionUpdateAfterArrival);
        }
        let ship_id = state.ship_id.ok_or(NavigationError::NoShipAssigned)?;
        Ok(PositionUpdated {
            route_id: cmd.route_id,
            ship_id,
            waypoint_id: cmd.waypoint_id,
        })
    }
}

// ---------------------------------------------------------------------------
// RecordArrival -> ShipArrivedAtStation
// ---------------------------------------------------------------------------

#[canon_core::command_handler(Route, version = 1)]
impl RecordArrivalHandler {
    type Error = NavigationError;

    fn handle(
        &self,
        state: &Route,
        cmd: RecordArrival,
    ) -> Result<ShipArrivedAtStation, NavigationError> {
        if state.arrived {
            return Err(NavigationError::AlreadyArrived);
        }
        let ship_id = state.ship_id.ok_or(NavigationError::NoShipAssigned)?;
        Ok(ShipArrivedAtStation {
            route_id: cmd.route_id,
            ship_id,
            station_id: cmd.station_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canon_core::CommandHandler;
    use uuid::Uuid;

    #[tokio::test]
    async fn plan_route_produces_route_planned() {
        let handler = PlanRouteHandler;
        let state = Route::default();
        let wp1 = Uuid::new_v4();
        let wp2 = Uuid::new_v4();
        let ship_id = Uuid::new_v4();
        let route_id = Uuid::new_v4();

        let cmd = PlanRoute {
            route_id,
            ship_id,
            waypoints: vec![wp1, wp2],
        };

        let event = CommandHandler::<Route>::handle(&handler, &state, cmd)
            .await
            .expect("handle");
        assert_eq!(event.route_id, route_id);
        assert_eq!(event.ship_id, ship_id);
        assert_eq!(event.waypoints, vec![wp1, wp2]);
    }

    #[tokio::test]
    async fn plan_route_rejects_empty_waypoints() {
        let handler = PlanRouteHandler;
        let state = Route::default();
        let cmd = PlanRoute {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
            waypoints: vec![],
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NavigationError::EmptyWaypoints
        ));
    }

    #[tokio::test]
    async fn record_departure_fails_without_ship() {
        let handler = RecordDepartureHandler;
        let state = Route::default(); // no ship assigned
        let cmd = RecordDeparture {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(),
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NavigationError::NoShipAssigned
        ));
    }

    #[tokio::test]
    async fn record_departure_succeeds_with_ship() {
        let handler = RecordDepartureHandler;
        let ship_id = Uuid::new_v4();
        let wp1 = Uuid::new_v4();
        let state = Route {
            ship_id: Some(ship_id),
            waypoints: vec![wp1, Uuid::new_v4()],
            current_waypoint_index: 0,
            arrived: false,
        };

        let cmd = RecordDeparture {
            route_id: Uuid::new_v4(),
            ship_id,
        };

        let event = CommandHandler::<Route>::handle(&handler, &state, cmd)
            .await
            .expect("handle");
        assert_eq!(event.ship_id, ship_id);
        assert_eq!(event.waypoint_id, wp1);
    }

    #[tokio::test]
    async fn record_departure_fails_with_ship_mismatch() {
        let handler = RecordDepartureHandler;
        let state = Route {
            ship_id: Some(Uuid::new_v4()),
            waypoints: vec![Uuid::new_v4()],
            current_waypoint_index: 0,
            arrived: false,
        };

        let cmd = RecordDeparture {
            route_id: Uuid::new_v4(),
            ship_id: Uuid::new_v4(), // different ship
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), NavigationError::ShipMismatch));
    }

    #[tokio::test]
    async fn update_position_fails_after_arrival() {
        let handler = UpdatePositionHandler;
        let state = Route {
            ship_id: Some(Uuid::new_v4()),
            waypoints: vec![Uuid::new_v4()],
            current_waypoint_index: 0,
            arrived: true,
        };

        let cmd = UpdatePosition {
            route_id: Uuid::new_v4(),
            waypoint_id: Uuid::new_v4(),
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NavigationError::PositionUpdateAfterArrival
        ));
    }

    #[tokio::test]
    async fn update_position_succeeds() {
        let handler = UpdatePositionHandler;
        let ship_id = Uuid::new_v4();
        let wp = Uuid::new_v4();
        let state = Route {
            ship_id: Some(ship_id),
            waypoints: vec![Uuid::new_v4(), wp],
            current_waypoint_index: 0,
            arrived: false,
        };

        let cmd = UpdatePosition {
            route_id: Uuid::new_v4(),
            waypoint_id: wp,
        };

        let event = CommandHandler::<Route>::handle(&handler, &state, cmd)
            .await
            .expect("handle");
        assert_eq!(event.waypoint_id, wp);
        assert_eq!(event.ship_id, ship_id);
    }

    #[tokio::test]
    async fn record_arrival_fails_when_already_arrived() {
        let handler = RecordArrivalHandler;
        let state = Route {
            ship_id: Some(Uuid::new_v4()),
            waypoints: vec![Uuid::new_v4()],
            current_waypoint_index: 0,
            arrived: true,
        };

        let cmd = RecordArrival {
            route_id: Uuid::new_v4(),
            station_id: Uuid::new_v4(),
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NavigationError::AlreadyArrived
        ));
    }

    #[tokio::test]
    async fn record_arrival_succeeds() {
        let handler = RecordArrivalHandler;
        let ship_id = Uuid::new_v4();
        let station_id = Uuid::new_v4();
        let state = Route {
            ship_id: Some(ship_id),
            waypoints: vec![station_id],
            current_waypoint_index: 0,
            arrived: false,
        };

        let cmd = RecordArrival {
            route_id: Uuid::new_v4(),
            station_id,
        };

        let event = CommandHandler::<Route>::handle(&handler, &state, cmd)
            .await
            .expect("handle");
        assert_eq!(event.ship_id, ship_id);
        assert_eq!(event.station_id, station_id);
    }

    #[tokio::test]
    async fn record_arrival_fails_without_ship() {
        let handler = RecordArrivalHandler;
        let state = Route::default(); // no ship assigned
        let cmd = RecordArrival {
            route_id: Uuid::new_v4(),
            station_id: Uuid::new_v4(),
        };

        let result = CommandHandler::<Route>::handle(&handler, &state, cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NavigationError::NoShipAssigned
        ));
    }
}
