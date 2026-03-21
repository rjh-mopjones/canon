//! Command handlers for the Route aggregate.
//!
//! Each command handler validates against the current aggregate state and
//! produces a single event on success or returns an error on rejection.
//! Traits are implemented manually because the shared crate owns the
//! command and event types (Rust orphan rules).

use async_trait::async_trait;
use canon_core::CommandHandler;
use canon_demo_shared::commands::{PlanRoute, RecordArrival, RecordDeparture, UpdatePosition};
use canon_demo_shared::events::{PositionUpdated, RoutePlanned, ShipArrivedAtStation};

use crate::aggregate::Route;
use crate::error::NavigationError;

// ---------------------------------------------------------------------------
// PlanRoute -> RoutePlanned
// ---------------------------------------------------------------------------

pub struct PlanRouteHandler;

impl PlanRouteHandler {
    fn __canon_handle(
        &self,
        state: &Route,
        cmd: PlanRoute,
    ) -> Result<RoutePlanned, NavigationError> {
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

#[async_trait]
impl CommandHandler<Route> for PlanRouteHandler {
    type Command = PlanRoute;
    type Event = RoutePlanned;
    type Error = NavigationError;

    async fn handle(&self, state: &Route, cmd: PlanRoute) -> Result<RoutePlanned, NavigationError> {
        self.__canon_handle(state, cmd)
    }
}

fn __canon_dispatch_planroute_v1(
    command_payload: &[u8],
    events: &[canon_core::EventEnvelope],
    aggregate_type_id: std::any::TypeId,
) -> Result<canon_core::HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut state = Route::default();
    for envelope in events {
        canon_core::__apply_event_combiner(
            aggregate_type_id,
            envelope,
            &mut state as &mut dyn std::any::Any,
        )?;
    }
    let command: PlanRoute = canon_core::__deserialize(command_payload)?;
    let handler = PlanRouteHandler;
    let event = handler
        .__canon_handle(&state, command)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    let event_payload = canon_core::__serialize(&event)?;
    Ok(canon_core::HandlerDispatchResult {
        event_payload,
        event_type: "RoutePlanned",
        event_version: 1,
    })
}

canon_core::__submit! {
    canon_core::CommandHandlerRegistration {
        aggregate_type_name: "Route",
        command_type_name: "PlanRoute",
        command_version: 1,
        handler_type_name: "PlanRouteHandler",
        dispatch_fn: __canon_dispatch_planroute_v1,
        produces_event_type: "RoutePlanned",
        produces_event_version: 1,
    }
}

// ---------------------------------------------------------------------------
// RecordDeparture -> PositionUpdated
//
// RecordDeparture does not produce a navigation-specific ShipDeparted
// event in the shared crate. The departure is already published by
// fleet-service. This handler records the departure on the route's state
// by producing a PositionUpdated event for the first waypoint.
// ---------------------------------------------------------------------------

pub struct RecordDepartureHandler;

impl RecordDepartureHandler {
    fn __canon_handle(
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

#[async_trait]
impl CommandHandler<Route> for RecordDepartureHandler {
    type Command = RecordDeparture;
    type Event = PositionUpdated;
    type Error = NavigationError;

    async fn handle(
        &self,
        state: &Route,
        cmd: RecordDeparture,
    ) -> Result<PositionUpdated, NavigationError> {
        self.__canon_handle(state, cmd)
    }
}

fn __canon_dispatch_recorddeparture_v1(
    command_payload: &[u8],
    events: &[canon_core::EventEnvelope],
    aggregate_type_id: std::any::TypeId,
) -> Result<canon_core::HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut state = Route::default();
    for envelope in events {
        canon_core::__apply_event_combiner(
            aggregate_type_id,
            envelope,
            &mut state as &mut dyn std::any::Any,
        )?;
    }
    let command: RecordDeparture = canon_core::__deserialize(command_payload)?;
    let handler = RecordDepartureHandler;
    let event = handler
        .__canon_handle(&state, command)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    let event_payload = canon_core::__serialize(&event)?;
    Ok(canon_core::HandlerDispatchResult {
        event_payload,
        event_type: "PositionUpdated",
        event_version: 1,
    })
}

canon_core::__submit! {
    canon_core::CommandHandlerRegistration {
        aggregate_type_name: "Route",
        command_type_name: "RecordDeparture",
        command_version: 1,
        handler_type_name: "RecordDepartureHandler",
        dispatch_fn: __canon_dispatch_recorddeparture_v1,
        produces_event_type: "PositionUpdated",
        produces_event_version: 1,
    }
}

// ---------------------------------------------------------------------------
// UpdatePosition -> PositionUpdated
// ---------------------------------------------------------------------------

pub struct UpdatePositionHandler;

impl UpdatePositionHandler {
    fn __canon_handle(
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

#[async_trait]
impl CommandHandler<Route> for UpdatePositionHandler {
    type Command = UpdatePosition;
    type Event = PositionUpdated;
    type Error = NavigationError;

    async fn handle(
        &self,
        state: &Route,
        cmd: UpdatePosition,
    ) -> Result<PositionUpdated, NavigationError> {
        self.__canon_handle(state, cmd)
    }
}

fn __canon_dispatch_updateposition_v1(
    command_payload: &[u8],
    events: &[canon_core::EventEnvelope],
    aggregate_type_id: std::any::TypeId,
) -> Result<canon_core::HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut state = Route::default();
    for envelope in events {
        canon_core::__apply_event_combiner(
            aggregate_type_id,
            envelope,
            &mut state as &mut dyn std::any::Any,
        )?;
    }
    let command: UpdatePosition = canon_core::__deserialize(command_payload)?;
    let handler = UpdatePositionHandler;
    let event = handler
        .__canon_handle(&state, command)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    let event_payload = canon_core::__serialize(&event)?;
    Ok(canon_core::HandlerDispatchResult {
        event_payload,
        event_type: "PositionUpdated",
        event_version: 1,
    })
}

canon_core::__submit! {
    canon_core::CommandHandlerRegistration {
        aggregate_type_name: "Route",
        command_type_name: "UpdatePosition",
        command_version: 1,
        handler_type_name: "UpdatePositionHandler",
        dispatch_fn: __canon_dispatch_updateposition_v1,
        produces_event_type: "PositionUpdated",
        produces_event_version: 1,
    }
}

// ---------------------------------------------------------------------------
// RecordArrival -> ShipArrivedAtStation
// ---------------------------------------------------------------------------

pub struct RecordArrivalHandler;

impl RecordArrivalHandler {
    fn __canon_handle(
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

#[async_trait]
impl CommandHandler<Route> for RecordArrivalHandler {
    type Command = RecordArrival;
    type Event = ShipArrivedAtStation;
    type Error = NavigationError;

    async fn handle(
        &self,
        state: &Route,
        cmd: RecordArrival,
    ) -> Result<ShipArrivedAtStation, NavigationError> {
        self.__canon_handle(state, cmd)
    }
}

fn __canon_dispatch_recordarrival_v1(
    command_payload: &[u8],
    events: &[canon_core::EventEnvelope],
    aggregate_type_id: std::any::TypeId,
) -> Result<canon_core::HandlerDispatchResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut state = Route::default();
    for envelope in events {
        canon_core::__apply_event_combiner(
            aggregate_type_id,
            envelope,
            &mut state as &mut dyn std::any::Any,
        )?;
    }
    let command: RecordArrival = canon_core::__deserialize(command_payload)?;
    let handler = RecordArrivalHandler;
    let event = handler
        .__canon_handle(&state, command)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    let event_payload = canon_core::__serialize(&event)?;
    Ok(canon_core::HandlerDispatchResult {
        event_payload,
        event_type: "ShipArrivedAtStation",
        event_version: 1,
    })
}

canon_core::__submit! {
    canon_core::CommandHandlerRegistration {
        aggregate_type_name: "Route",
        command_type_name: "RecordArrival",
        command_version: 1,
        handler_type_name: "RecordArrivalHandler",
        dispatch_fn: __canon_dispatch_recordarrival_v1,
        produces_event_type: "ShipArrivedAtStation",
        produces_event_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
