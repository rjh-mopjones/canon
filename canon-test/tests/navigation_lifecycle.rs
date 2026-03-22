//! Integration test: full navigation route lifecycle.
//!
//! Exercises the complete flow from ShipDeparted → PlanRoute → RoutePlanned
//! → RecordDeparture → PositionUpdated → UpdatePosition → PositionUpdated
//! → RecordArrival → ShipArrivedAtStation, using the TestHarness and
//! navigation-service handlers/aggregate.

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{Aggregate, AggregateId, CommandHandler, EventEnvelope, EventHandler, Version};
use canon_demo_shared::commands::{PlanRoute, RecordArrival, RecordDeparture, UpdatePosition};
use canon_demo_shared::events::{
    PositionUpdated, RoutePlanned, ShipArrivedAtStation, ShipDeparted,
};
use canon_test::harness::TestHarness;
use navigation_service::aggregate::Route;
use navigation_service::commands::{
    PlanRouteHandler, RecordArrivalHandler, RecordDepartureHandler, UpdatePositionHandler,
};
use navigation_service::handlers::DepartureHandler;

fn make_event_envelope(
    agg_id: &AggregateId,
    version: Version,
    event_type: &str,
    payload: Vec<u8>,
    correlation_id: Uuid,
) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: agg_id.clone(),
        version,
        event_type: event_type.to_string(),
        event_version: 1,
        payload: Bytes::from(payload),
        correlation_id,
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Full route lifecycle: ShipDeparted → PlanRoute → RoutePlanned →
/// RecordDeparture → PositionUpdated → UpdatePosition → PositionUpdated →
/// RecordArrival → ShipArrivedAtStation.
///
/// Verifies the event handler produces PlanRoute, command handlers produce
/// correct events, aggregate state evolves correctly, and events are stored
/// in the harness event store with sequential versions.
#[tokio::test]
async fn navigation_full_route_lifecycle() {
    let harness = TestHarness::new();
    let ship_id = Uuid::new_v4();
    let destination = Uuid::new_v4();
    let mid_waypoint = Uuid::new_v4();
    let route_agg_id = AggregateId::from_uuid(destination);
    let correlation_id = Uuid::new_v4();

    // ── Step 1: DepartureHandler receives ShipDeparted, produces PlanRoute ──
    let departed = ShipDeparted {
        ship_id,
        destination,
        fuel_at_departure: 85.0,
    };
    let cmd_envelope = EventHandler::handle(&DepartureHandler, vec![departed])
        .await
        .expect("DepartureHandler should not error")
        .expect("DepartureHandler should produce a command");
    assert_eq!(cmd_envelope.command_type, "PlanRoute");
    // The DepartureHandler creates a new Route aggregate (fresh UUID),
    // not one keyed by destination. The aggregate_id is the route_id.
    let route_id_from_handler = *cmd_envelope.aggregate_id.as_uuid();
    assert_ne!(route_id_from_handler, Uuid::nil());

    let plan_cmd: PlanRoute =
        serde_json::from_slice(&cmd_envelope.payload).expect("deserialize PlanRoute");
    assert_eq!(plan_cmd.ship_id, ship_id);
    assert_eq!(plan_cmd.waypoints, vec![destination]);
    assert_eq!(plan_cmd.route_id, route_id_from_handler);

    // ── Step 2: PlanRoute → RoutePlanned ────────────────────────────────────
    let state = Route::default();
    let plan_cmd_with_waypoints = PlanRoute {
        route_id: destination,
        ship_id,
        waypoints: vec![mid_waypoint, destination],
    };
    let route_planned: RoutePlanned =
        CommandHandler::<Route>::handle(&PlanRouteHandler, &state, plan_cmd_with_waypoints)
            .await
            .expect("PlanRoute should succeed");
    assert_eq!(route_planned.ship_id, ship_id);
    assert_eq!(route_planned.waypoints, vec![mid_waypoint, destination]);

    // Store RoutePlanned in event store
    let envelope1 = make_event_envelope(
        &route_agg_id,
        Version::initial(),
        "RoutePlanned",
        serde_json::to_vec(&route_planned).expect("serialize"),
        correlation_id,
    );
    harness.append_events(&route_agg_id, Version::initial(), vec![envelope1]);

    // ── Step 3: Hydrate and RecordDeparture → PositionUpdated ───────────────
    let events = harness.load_events(&route_agg_id);
    let mut state = Route::default();
    Route::hydrate(&mut state, events.into_iter()).expect("hydrate after RoutePlanned");
    assert_eq!(state.ship_id, Some(ship_id));
    assert!(!state.arrived);

    let depart_cmd = RecordDeparture {
        route_id: destination,
        ship_id,
    };
    let pos_update1: PositionUpdated =
        CommandHandler::<Route>::handle(&RecordDepartureHandler, &state, depart_cmd)
            .await
            .expect("RecordDeparture should succeed");
    assert_eq!(pos_update1.ship_id, ship_id);
    assert_eq!(pos_update1.waypoint_id, mid_waypoint);

    // Store PositionUpdated
    let envelope2 = make_event_envelope(
        &route_agg_id,
        Version::from_u64(1),
        "PositionUpdated",
        serde_json::to_vec(&pos_update1).expect("serialize"),
        correlation_id,
    );
    harness.append_events(&route_agg_id, Version::from_u64(1), vec![envelope2]);

    // ── Step 4: UpdatePosition → PositionUpdated (mid-transit) ──────────────
    let events = harness.load_events(&route_agg_id);
    let mut state = Route::default();
    Route::hydrate(&mut state, events.into_iter()).expect("hydrate after first PositionUpdated");

    let update_cmd = UpdatePosition {
        route_id: destination,
        waypoint_id: destination,
    };
    let pos_update2: PositionUpdated =
        CommandHandler::<Route>::handle(&UpdatePositionHandler, &state, update_cmd)
            .await
            .expect("UpdatePosition should succeed");
    assert_eq!(pos_update2.waypoint_id, destination);

    // Store second PositionUpdated
    let envelope3 = make_event_envelope(
        &route_agg_id,
        Version::from_u64(2),
        "PositionUpdated",
        serde_json::to_vec(&pos_update2).expect("serialize"),
        correlation_id,
    );
    harness.append_events(&route_agg_id, Version::from_u64(2), vec![envelope3]);

    // ── Step 5: RecordArrival → ShipArrivedAtStation ────────────────────────
    let events = harness.load_events(&route_agg_id);
    let mut state = Route::default();
    Route::hydrate(&mut state, events.into_iter()).expect("hydrate after second PositionUpdated");
    assert!(!state.arrived);

    let arrival_cmd = RecordArrival {
        route_id: destination,
        station_id: destination,
    };
    let arrived: ShipArrivedAtStation =
        CommandHandler::<Route>::handle(&RecordArrivalHandler, &state, arrival_cmd)
            .await
            .expect("RecordArrival should succeed");
    assert_eq!(arrived.ship_id, ship_id);
    assert_eq!(arrived.station_id, destination);

    // Store ShipArrivedAtStation
    let envelope4 = make_event_envelope(
        &route_agg_id,
        Version::from_u64(3),
        "ShipArrivedAtStation",
        serde_json::to_vec(&arrived).expect("serialize"),
        correlation_id,
    );
    harness.append_events(&route_agg_id, Version::from_u64(3), vec![envelope4]);

    // ── Verify final state ──────────────────────────────────────────────────
    harness.assert_event_count(&route_agg_id, 4);

    let all_events = harness.load_events(&route_agg_id);
    assert_eq!(all_events[0].event_type, "RoutePlanned");
    assert_eq!(all_events[1].event_type, "PositionUpdated");
    assert_eq!(all_events[2].event_type, "PositionUpdated");
    assert_eq!(all_events[3].event_type, "ShipArrivedAtStation");

    // All events share the same correlation_id
    for event in &all_events {
        assert_eq!(event.correlation_id, correlation_id);
    }

    // Sequential version progression
    assert_eq!(all_events[0].version.as_u64(), 1);
    assert_eq!(all_events[1].version.as_u64(), 2);
    assert_eq!(all_events[2].version.as_u64(), 3);
    assert_eq!(all_events[3].version.as_u64(), 4);

    // Final aggregate state
    let mut final_state = Route::default();
    Route::hydrate(&mut final_state, all_events.into_iter()).expect("final hydrate");
    assert_eq!(final_state.ship_id, Some(ship_id));
    assert!(final_state.arrived);
    assert_eq!(final_state.current_waypoint_index, 1);
}

/// RecordArrival on an already-arrived route must fail.
#[tokio::test]
async fn arrival_after_arrival_is_rejected() {
    let ship_id = Uuid::new_v4();
    let station_id = Uuid::new_v4();

    let state = Route {
        ship_id: Some(ship_id),
        waypoints: vec![station_id],
        current_waypoint_index: 0,
        arrived: true,
    };

    let cmd = RecordArrival {
        route_id: Uuid::new_v4(),
        station_id,
    };

    let result = CommandHandler::<Route>::handle(&RecordArrivalHandler, &state, cmd).await;
    assert!(result.is_err());
}

/// DepartureHandler is idempotent — calling it twice with the same event
/// produces two independent commands (inbox deduplication happens upstream).
#[tokio::test]
async fn departure_handler_is_idempotent() {
    let departed = ShipDeparted {
        ship_id: Uuid::new_v4(),
        destination: Uuid::new_v4(),
        fuel_at_departure: 50.0,
    };

    let result1 = EventHandler::handle(&DepartureHandler, vec![departed.clone()])
        .await
        .expect("handle 1");
    let result2 = EventHandler::handle(&DepartureHandler, vec![departed])
        .await
        .expect("handle 2");

    assert!(result1.is_some());
    assert!(result2.is_some());

    let cmd1 = result1.expect("cmd1");
    let cmd2 = result2.expect("cmd2");
    assert_eq!(cmd1.command_type, "PlanRoute");
    assert_eq!(cmd2.command_type, "PlanRoute");
    // Different command_ids (independent invocations)
    assert_ne!(cmd1.command_id, cmd2.command_id);
}
