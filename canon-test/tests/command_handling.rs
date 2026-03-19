use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_handle_valid_command() {
    let state = OrderAggregate::default();
    let order_id = Uuid::new_v4();

    let events = CommandHandler::<OrderAggregate>::handle(
        &PlaceOrderHandler,
        &state,
        PlaceOrder { order_id },
    )
    .await
    .unwrap();

    assert_eq!(events.len(), 1);
    match &events[0] {
        PlaceOrderEvent::OrderPlaced(e) => assert_eq!(e.order_id, order_id),
    }
}

#[tokio::test]
async fn test_handle_invalid_command() {
    let state = OrderAggregate {
        placed: false,
        cancelled: true,
        priority: None,
    };

    let result = CommandHandler::<OrderAggregate>::handle(
        &CancelOrderHandler,
        &state,
        CancelOrder {
            reason: "again".into(),
        },
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_version_increments_per_event() {
    let store = InMemoryEventStore::new();
    let id = AggregateId::new();
    let order_id = Uuid::new_v4();

    // First command: Place
    let state = OrderAggregate::default();
    let events = CommandHandler::<OrderAggregate>::handle(
        &PlaceOrderHandler,
        &state,
        PlaceOrder { order_id },
    )
    .await
    .unwrap();
    let envelopes: Vec<EventEnvelope> = events
        .iter()
        .map(|e| match e {
            PlaceOrderEvent::OrderPlaced(placed) => make_placed_envelope(&id, placed.order_id),
        })
        .collect();
    store
        .append(&id, Version::initial(), envelopes)
        .unwrap();

    // Load, hydrate, then run second command: Cancel
    let loaded = store.load(&id).unwrap();
    let current_version = loaded.last().unwrap().version;
    let mut state2 = OrderAggregate::default();
    OrderAggregate::hydrate(&mut state2, loaded.into_iter()).unwrap();

    let events2 = CommandHandler::<OrderAggregate>::handle(
        &CancelOrderHandler,
        &state2,
        CancelOrder {
            reason: "test".into(),
        },
    )
    .await
    .unwrap();
    let envelopes2: Vec<EventEnvelope> = events2
        .iter()
        .map(|e| match e {
            CancelOrderEvent::OrderCancelled(c) => make_cancelled_envelope(&id, &c.reason),
        })
        .collect();
    store.append(&id, current_version, envelopes2).unwrap();

    // Assert sequential version progression
    let all_events = store.load(&id).unwrap();
    assert_eq!(all_events.len(), 2);
    assert_eq!(all_events[0].version.as_u64(), 1);
    assert_eq!(all_events[1].version.as_u64(), 2);
}
