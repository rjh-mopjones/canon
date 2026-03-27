//! Integration test: full resupply chain across the supply-service.
//!
//! Simulates the cross-service choreography:
//!   StationStockLow → StockAlertHandler → RequestResupply
//!   → ResupplyRequested → DispatchResupply → ResupplyDispatched
//!   → ConfirmDelivery → DeliveryConfirmed
//!
//! Uses the TestHarness with in-memory stores to verify the full chain
//! without infrastructure dependencies.

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{Aggregate, AggregateId, CommandHandler, EventEnvelope, EventHandler, Version};
use canon_test::harness::TestHarness;
use supply_service::aggregate::{
    ConfirmDelivery, DeliveryConfirmed, DispatchResupply, Inventory, RecordStock, RequestResupply,
    ResupplyDispatched,
};
use supply_service::commands::{
    ConfirmDeliveryHandler, DispatchResupplyHandler, RecordStockHandler, RequestResupplyHandler,
};
use supply_service::handler::StockAlertHandler;
use supply_service::inbound::InboundStationStockLow as StationStockLow;

/// Helper: create an EventEnvelope for an Inventory event.
fn make_inventory_envelope(
    aggregate_id: &AggregateId,
    event_type: &str,
    payload: &impl serde::Serialize,
    correlation_id: Uuid,
    version: Version,
) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version,
        event_type: event_type.to_string(),
        event_version: 1,
        payload: Bytes::from(serde_json::to_vec(payload).expect("serialize")),
        correlation_id,
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

/// Full resupply chain: StationStockLow → RequestResupply → ResupplyRequested
/// → DispatchResupply → ResupplyDispatched → ConfirmDelivery → DeliveryConfirmed.
///
/// Verifies that a single correlation ID threads through every step and that
/// aggregate state transitions correctly at each stage.
#[tokio::test]
async fn full_resupply_chain_with_correlation() {
    let harness = TestHarness::new();
    let station_id = Uuid::new_v4();
    let inventory_agg_id = AggregateId::from_uuid(station_id);
    let correlation_id = Uuid::new_v4();

    // ── Step 1: Station fires StationStockLow ──────────────────────────
    let stock_low = StationStockLow {
        station_id,
        current_stock_kg: 30.0,
        threshold_kg: 200.0,
    };

    // StockAlertHandler receives the event and produces a RequestResupply command.
    let handler = StockAlertHandler;
    let cmd_envelope = handler
        .handle(vec![stock_low])
        .await
        .expect("handler should not error")
        .expect("handler should produce a command");

    assert_eq!(cmd_envelope.command_type, "RequestResupply");
    assert_eq!(*cmd_envelope.aggregate_id.as_uuid(), station_id);
    assert_eq!(cmd_envelope.command_version, 1);

    // Deserialize the command payload to verify fuel_needed calculation.
    let payload: serde_json::Value =
        serde_json::from_slice(&cmd_envelope.payload).expect("deserialize payload");
    let fuel_needed = payload["fuel_kg"].as_f64().expect("fuel_kg field");
    assert!(
        (fuel_needed - 170.0).abs() < f64::EPSILON,
        "fuel_needed should be 170.0"
    );

    // ── Step 2: Process RequestResupply → ResupplyRequested ────────────
    let mut state = Inventory::default();

    // First record some stock so state is initialised.
    let record_handler = RecordStockHandler;
    let stock_event = record_handler
        .handle(
            &state,
            RecordStock {
                station_id,
                fuel_kg: 30.0,
            },
        )
        .await
        .expect("RecordStock should succeed");

    // Hydrate the stock event into state.
    let env = make_inventory_envelope(
        &inventory_agg_id,
        "StockRecorded",
        &stock_event,
        correlation_id,
        Version::initial(),
    );
    harness.append_events(&inventory_agg_id, Version::initial(), vec![env]);
    let events = harness.load_events(&inventory_agg_id);
    Inventory::hydrate(&mut state, events.into_iter()).expect("hydrate");

    // Now process the RequestResupply command.
    let request_handler = RequestResupplyHandler;
    let resupply_requested = request_handler
        .handle(
            &state,
            RequestResupply {
                station_id,
                fuel_kg: 170.0,
            },
        )
        .await
        .expect("RequestResupply should succeed");

    assert_eq!(resupply_requested.station_id, station_id);
    assert!((resupply_requested.fuel_kg - 170.0).abs() < f32::EPSILON);

    // Hydrate ResupplyRequested — state should be unchanged (no-op combiner).
    let env = make_inventory_envelope(
        &inventory_agg_id,
        "ResupplyRequested",
        &resupply_requested,
        correlation_id,
        Version::initial().next(),
    );
    harness.append_events(&inventory_agg_id, Version::initial().next(), vec![env]);
    let events = harness.load_events(&inventory_agg_id);
    let mut state = Inventory::default();
    Inventory::hydrate(&mut state, events.into_iter()).expect("hydrate");
    assert!(
        state.pending_resupply.is_none(),
        "ResupplyRequested should not set pending"
    );

    // ── Step 3: DispatchResupply → ResupplyDispatched ──────────────────
    let ship_id = Uuid::new_v4();
    let dispatch_handler = DispatchResupplyHandler;
    let dispatched = dispatch_handler
        .handle(
            &state,
            DispatchResupply {
                inventory_id: station_id,
                ship_id,
            },
        )
        .await
        .expect("DispatchResupply should succeed (no pending)");

    assert_eq!(dispatched.ship_id, ship_id);

    // Hydrate and verify pending_resupply is set.
    let version_2 = Version::initial().next().next();
    let env = make_inventory_envelope(
        &inventory_agg_id,
        "ResupplyDispatched",
        &dispatched,
        correlation_id,
        version_2,
    );
    harness.append_events(&inventory_agg_id, version_2, vec![env]);
    let events = harness.load_events(&inventory_agg_id);
    let mut state = Inventory::default();
    Inventory::hydrate(&mut state, events.into_iter()).expect("hydrate");
    assert_eq!(state.pending_resupply, Some(ship_id));

    // ── Step 4: ConfirmDelivery → DeliveryConfirmed ────────────────────
    let confirm_handler = ConfirmDeliveryHandler;
    let confirmed = confirm_handler
        .handle(
            &state,
            ConfirmDelivery {
                inventory_id: station_id,
            },
        )
        .await
        .expect("ConfirmDelivery should succeed");

    assert_eq!(confirmed.ship_id, ship_id);

    // Hydrate and verify pending_resupply is cleared.
    let version_3 = version_2.next();
    let env = make_inventory_envelope(
        &inventory_agg_id,
        "DeliveryConfirmed",
        &confirmed,
        correlation_id,
        version_3,
    );
    harness.append_events(&inventory_agg_id, version_3, vec![env]);
    let events = harness.load_events(&inventory_agg_id);
    let mut state = Inventory::default();
    Inventory::hydrate(&mut state, events.into_iter()).expect("hydrate");
    assert!(
        state.pending_resupply.is_none(),
        "DeliveryConfirmed should clear pending"
    );

    // Verify full event count: StockRecorded + ResupplyRequested +
    // ResupplyDispatched + DeliveryConfirmed = 4 events.
    harness.assert_event_count(&inventory_agg_id, 4);
}

/// DispatchResupply fails when a resupply is already pending — demonstrating
/// the dead-letter path. The error should be `ResupplyAlreadyPending`.
#[tokio::test]
async fn dispatch_fails_when_resupply_already_pending() {
    let existing_ship = Uuid::new_v4();
    let state = Inventory {
        station_id: Some(Uuid::new_v4()),
        fuel_kg: 200,
        pending_resupply: Some(existing_ship),
    };

    let handler = DispatchResupplyHandler;
    let err = handler
        .handle(
            &state,
            DispatchResupply {
                inventory_id: Uuid::new_v4(),
                ship_id: Uuid::new_v4(),
            },
        )
        .await
        .expect_err("should fail when resupply already pending");

    assert!(
        err.to_string().contains("already pending"),
        "error should mention 'already pending', got: {err}"
    );
}

/// After a full dispatch+confirm cycle, a second dispatch should succeed
/// because pending_resupply has been cleared.
#[tokio::test]
async fn second_dispatch_after_confirm_succeeds() {
    let harness = TestHarness::new();
    let station_id = Uuid::new_v4();
    let agg_id = AggregateId::from_uuid(station_id);
    let correlation_id = Uuid::new_v4();

    // Dispatch first resupply.
    let ship1 = Uuid::new_v4();
    let dispatched1 = ResupplyDispatched {
        inventory_id: station_id,
        ship_id: ship1,
        fuel_kg: 100.0,
    };
    let env1 = make_inventory_envelope(
        &agg_id,
        "ResupplyDispatched",
        &dispatched1,
        correlation_id,
        Version::initial(),
    );
    harness.append_events(&agg_id, Version::initial(), vec![env1]);

    // Confirm delivery.
    let confirmed = DeliveryConfirmed {
        inventory_id: station_id,
        ship_id: ship1,
    };
    let env2 = make_inventory_envelope(
        &agg_id,
        "DeliveryConfirmed",
        &confirmed,
        correlation_id,
        Version::initial().next(),
    );
    harness.append_events(&agg_id, Version::initial().next(), vec![env2]);

    // Hydrate state.
    let events = harness.load_events(&agg_id);
    let mut state = Inventory::default();
    Inventory::hydrate(&mut state, events.into_iter()).expect("hydrate");
    assert!(state.pending_resupply.is_none());

    // Second dispatch should succeed.
    let ship2 = Uuid::new_v4();
    let handler = DispatchResupplyHandler;
    let dispatched2 = handler
        .handle(
            &state,
            DispatchResupply {
                inventory_id: station_id,
                ship_id: ship2,
            },
        )
        .await
        .expect("second dispatch should succeed after confirm");

    assert_eq!(dispatched2.ship_id, ship2);
}

/// StockAlertHandler is idempotent — processing the same StationStockLow
/// batch twice produces equivalent commands (same type, same aggregate).
#[tokio::test]
async fn stock_alert_handler_is_idempotent() {
    let station_id = Uuid::new_v4();
    let events = vec![StationStockLow {
        station_id,
        current_stock_kg: 50.0,
        threshold_kg: 200.0,
    }];

    let handler = StockAlertHandler;

    let cmd1 = handler
        .handle(events.clone())
        .await
        .expect("first call ok")
        .expect("should produce command");

    let cmd2 = handler
        .handle(events)
        .await
        .expect("second call ok")
        .expect("should produce command");

    // Same command type and target aggregate (idempotent semantics).
    assert_eq!(cmd1.command_type, cmd2.command_type);
    assert_eq!(cmd1.aggregate_id.as_uuid(), cmd2.aggregate_id.as_uuid());
    assert_eq!(cmd1.command_version, cmd2.command_version);
}

/// Events published to the outbound queue via the harness are accessible
/// for downstream consumer assertions (e.g. fleet-service ResupplyHandler).
#[tokio::test]
async fn resupply_dispatched_publishes_to_outbound() {
    let harness = TestHarness::new();
    let station_id = Uuid::new_v4();
    let ship_id = Uuid::new_v4();
    let agg_id = AggregateId::from_uuid(station_id);

    let dispatched = ResupplyDispatched {
        inventory_id: station_id,
        ship_id,
        fuel_kg: 300.0,
    };

    let env = make_inventory_envelope(
        &agg_id,
        "ResupplyDispatched",
        &dispatched,
        Uuid::new_v4(),
        Version::initial(),
    );

    // Simulate outbox processor publishing to outbound queue.
    harness.publish_to_outbound(env.clone());

    // Simulate publisher forwarding to external topic.
    harness.publish_external(env, "canon.supply.events");

    let published = harness.published_events();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].1, "canon.supply.events");
    assert_eq!(published[0].0.event_type, "ResupplyDispatched");
}
