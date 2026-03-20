use canon_core::{CommandHandler, EventCombiner, EventHandler, ProjectionHandler};
use canon_demo_shared::events::StationStockLow;
use uuid::Uuid;

use crate::aggregate::{
    ConfirmDelivery, DeliveryConfirmed, DispatchResupply, Inventory, RecordStock, RequestResupply,
    ResupplyDispatched, ResupplyRequested, StockRecorded,
};
use crate::commands::{
    ConfirmDeliveryHandler, DispatchResupplyHandler, RecordStockHandler, RequestResupplyHandler,
};
use crate::error::SupplyError;
use crate::handler::StockAlertHandler;
use crate::projection::{
    DeliveryConfirmedProjectionHandler, InventoryReadModel, ResupplyDispatchedProjectionHandler,
    StockRecordedProjectionHandler,
};

// ---------------------------------------------------------------------------
// Aggregate hydration
// ---------------------------------------------------------------------------

#[test]
fn default_inventory_state() {
    let state = Inventory::default();
    assert!(state.station_id.is_none());
    assert_eq!(state.fuel_kg, 0);
    assert!(state.pending_resupply.is_none());
}

#[test]
fn snapshot_every_is_50() {
    assert_eq!(Inventory::SNAPSHOT_EVERY, Some(50));
}

// ---------------------------------------------------------------------------
// Event combiners
// ---------------------------------------------------------------------------

#[test]
fn stock_recorded_sets_station_and_fuel() {
    let mut state = Inventory::default();
    let station = Uuid::new_v4();
    let event = StockRecorded {
        inventory_id: station,
        station_id: station,
        fuel_kg: 500.0,
    };
    event.combine(&mut state);
    assert_eq!(state.station_id, Some(station));
    assert_eq!(state.fuel_kg, 500);
}

#[test]
fn resupply_requested_does_not_change_state() {
    let mut state = Inventory {
        fuel_kg: 100,
        ..Inventory::default()
    };
    let event = ResupplyRequested {
        inventory_id: Uuid::new_v4(),
        station_id: Uuid::new_v4(),
        fuel_kg: 200.0,
    };
    event.combine(&mut state);
    assert_eq!(state.fuel_kg, 100);
    assert!(state.pending_resupply.is_none());
}

#[test]
fn resupply_dispatched_sets_pending() {
    let mut state = Inventory::default();
    let ship = Uuid::new_v4();
    let event = ResupplyDispatched {
        inventory_id: Uuid::new_v4(),
        ship_id: ship,
        fuel_kg: 300.0,
    };
    event.combine(&mut state);
    assert_eq!(state.pending_resupply, Some(ship));
}

#[test]
fn delivery_confirmed_clears_pending() {
    let mut state = Inventory::default();
    let ship = Uuid::new_v4();
    state.pending_resupply = Some(ship);
    let event = DeliveryConfirmed {
        inventory_id: Uuid::new_v4(),
        ship_id: ship,
    };
    event.combine(&mut state);
    assert!(state.pending_resupply.is_none());
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_stock_produces_event() {
    let handler = RecordStockHandler;
    let state = Inventory::default();
    let station = Uuid::new_v4();
    let cmd = RecordStock {
        station_id: station,
        fuel_kg: 1000.0,
    };
    let event = handler.handle(&state, cmd).await.expect("should succeed");
    assert_eq!(event.station_id, station);
    assert_eq!(event.fuel_kg, 1000.0);
}

#[tokio::test]
async fn request_resupply_produces_event() {
    let handler = RequestResupplyHandler;
    let state = Inventory::default();
    let station = Uuid::new_v4();
    let cmd = RequestResupply {
        station_id: station,
        fuel_kg: 500.0,
    };
    let event = handler.handle(&state, cmd).await.expect("should succeed");
    assert_eq!(event.station_id, station);
    assert_eq!(event.fuel_kg, 500.0);
}

#[tokio::test]
async fn dispatch_resupply_succeeds_when_no_pending() {
    let handler = DispatchResupplyHandler;
    let state = Inventory {
        station_id: Some(Uuid::new_v4()),
        fuel_kg: 200,
        pending_resupply: None,
    };
    let ship = Uuid::new_v4();
    let cmd = DispatchResupply {
        inventory_id: Uuid::new_v4(),
        ship_id: ship,
    };
    let event = handler.handle(&state, cmd).await.expect("should succeed");
    assert_eq!(event.ship_id, ship);
    assert_eq!(event.fuel_kg, 200.0);
}

#[tokio::test]
async fn dispatch_resupply_fails_when_already_pending() {
    let handler = DispatchResupplyHandler;
    let pending_ship = Uuid::new_v4();
    let state = Inventory {
        station_id: Some(Uuid::new_v4()),
        fuel_kg: 200,
        pending_resupply: Some(pending_ship),
    };
    let cmd = DispatchResupply {
        inventory_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
    };
    let err = handler.handle(&state, cmd).await.expect_err("should fail");
    match err {
        SupplyError::ResupplyAlreadyPending { pending_ship_id } => {
            assert_eq!(pending_ship_id, pending_ship);
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn confirm_delivery_succeeds_with_pending() {
    let handler = ConfirmDeliveryHandler;
    let ship = Uuid::new_v4();
    let state = Inventory {
        station_id: Some(Uuid::new_v4()),
        fuel_kg: 200,
        pending_resupply: Some(ship),
    };
    let cmd = ConfirmDelivery {
        inventory_id: Uuid::new_v4(),
    };
    let event = handler.handle(&state, cmd).await.expect("should succeed");
    assert_eq!(event.ship_id, ship);
}

#[tokio::test]
async fn confirm_delivery_fails_without_pending() {
    let handler = ConfirmDeliveryHandler;
    let state = Inventory {
        station_id: Some(Uuid::new_v4()),
        fuel_kg: 200,
        pending_resupply: None,
    };
    let cmd = ConfirmDelivery {
        inventory_id: Uuid::new_v4(),
    };
    let err = handler.handle(&state, cmd).await.expect_err("should fail");
    assert!(matches!(err, SupplyError::NoPendingResupply));
}

// ---------------------------------------------------------------------------
// Event handler -- StockAlertHandler
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stock_alert_handler_produces_command_on_stock_low() {
    let handler = StockAlertHandler;
    let station = Uuid::new_v4();
    let events = vec![StationStockLow {
        station_id: station,
        current_fuel_kg: 50.0,
        threshold_kg: 200.0,
    }];
    let result = handler.handle(events).await.expect("should not error");
    let envelope = result.expect("should produce a command");
    assert_eq!(envelope.command_type, "RequestResupply");
    assert_eq!(*envelope.aggregate_id.as_uuid(), station);
    assert_eq!(envelope.command_version, 1);
}

#[tokio::test]
async fn stock_alert_handler_returns_none_for_empty_batch() {
    let handler = StockAlertHandler;
    let result = handler.handle(vec![]).await.expect("should not error");
    assert!(result.is_none());
}

#[tokio::test]
async fn stock_alert_handler_uses_last_event_in_batch() {
    let handler = StockAlertHandler;
    let station1 = Uuid::new_v4();
    let station2 = Uuid::new_v4();
    let events = vec![
        StationStockLow {
            station_id: station1,
            current_fuel_kg: 50.0,
            threshold_kg: 200.0,
        },
        StationStockLow {
            station_id: station2,
            current_fuel_kg: 30.0,
            threshold_kg: 100.0,
        },
    ];
    let result = handler.handle(events).await.expect("should not error");
    let envelope = result.expect("should produce a command");
    // Should use the last event (station2)
    assert_eq!(*envelope.aggregate_id.as_uuid(), station2);
}

// ---------------------------------------------------------------------------
// Projection handlers
// ---------------------------------------------------------------------------

#[test]
fn projection_stock_recorded() {
    let handler = StockRecordedProjectionHandler;
    let station = Uuid::new_v4();
    let inventory = Uuid::new_v4();
    let event = StockRecorded {
        inventory_id: inventory,
        station_id: station,
        fuel_kg: 750.0,
    };
    let mut model = InventoryReadModel {
        inventory_id: Uuid::nil(),
        station_id: Uuid::nil(),
        fuel_kg: 0,
        resupply_pending: false,
        updated_at: chrono::Utc::now(),
    };
    handler.apply(&event, &mut model);
    assert_eq!(model.inventory_id, inventory);
    assert_eq!(model.station_id, station);
    assert_eq!(model.fuel_kg, 750);
}

#[test]
fn projection_resupply_dispatched_sets_pending() {
    let handler = ResupplyDispatchedProjectionHandler;
    let event = ResupplyDispatched {
        inventory_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
        fuel_kg: 100.0,
    };
    let mut model = InventoryReadModel {
        inventory_id: Uuid::nil(),
        station_id: Uuid::nil(),
        fuel_kg: 0,
        resupply_pending: false,
        updated_at: chrono::Utc::now(),
    };
    handler.apply(&event, &mut model);
    assert!(model.resupply_pending);
}

#[test]
fn projection_delivery_confirmed_clears_pending() {
    let handler = DeliveryConfirmedProjectionHandler;
    let event = DeliveryConfirmed {
        inventory_id: Uuid::new_v4(),
        ship_id: Uuid::new_v4(),
    };
    let mut model = InventoryReadModel {
        inventory_id: Uuid::nil(),
        station_id: Uuid::nil(),
        fuel_kg: 0,
        resupply_pending: true,
        updated_at: chrono::Utc::now(),
    };
    handler.apply(&event, &mut model);
    assert!(!model.resupply_pending);
}

// ---------------------------------------------------------------------------
// Idempotency -- applying the same event twice produces identical state
// ---------------------------------------------------------------------------

#[test]
fn stock_recorded_is_idempotent() {
    let station = Uuid::new_v4();
    let event = StockRecorded {
        inventory_id: station,
        station_id: station,
        fuel_kg: 500.0,
    };
    let mut state = Inventory::default();
    event.combine(&mut state);
    let snapshot = state.clone();
    event.combine(&mut state);
    assert_eq!(state.station_id, snapshot.station_id);
    assert_eq!(state.fuel_kg, snapshot.fuel_kg);
}

#[test]
fn dispatch_and_confirm_cycle() {
    let mut state = Inventory::default();
    let ship = Uuid::new_v4();

    // Dispatch
    let dispatch = ResupplyDispatched {
        inventory_id: Uuid::new_v4(),
        ship_id: ship,
        fuel_kg: 100.0,
    };
    dispatch.combine(&mut state);
    assert_eq!(state.pending_resupply, Some(ship));

    // Confirm
    let confirm = DeliveryConfirmed {
        inventory_id: Uuid::new_v4(),
        ship_id: ship,
    };
    confirm.combine(&mut state);
    assert!(state.pending_resupply.is_none());

    // Second dispatch should work (pending cleared)
    let ship2 = Uuid::new_v4();
    let dispatch2 = ResupplyDispatched {
        inventory_id: Uuid::new_v4(),
        ship_id: ship2,
        fuel_kg: 200.0,
    };
    dispatch2.combine(&mut state);
    assert_eq!(state.pending_resupply, Some(ship2));
}
