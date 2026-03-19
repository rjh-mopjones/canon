use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_version_matched_combiner_routing() {
    let agg_id = AggregateId::new();
    let order_id = Uuid::new_v4();

    // v1 envelope: dispatches to OrderPlaced combiner (sets placed=true)
    let v1_envelope = make_placed_envelope(&agg_id, order_id);

    // v2 envelope: dispatches to OrderPlacedV2 combiner (sets placed=true, priority=Some(5))
    let v2_envelope = make_placed_v2_envelope(&agg_id, order_id, 5);

    let mut state = OrderAggregate::default();
    OrderAggregate::hydrate(
        &mut state,
        vec![v1_envelope, v2_envelope].into_iter(),
    )
    .unwrap();

    assert!(state.placed);
    assert_eq!(state.priority, Some(5));
}

#[tokio::test]
async fn test_version_matched_command_handler_routing() {
    let order_id = Uuid::new_v4();

    // v1 handler produces PlaceOrderEvent::OrderPlaced
    let state = OrderAggregate::default();
    let v1_events = CommandHandler::<OrderAggregate>::handle(
        &PlaceOrderHandler,
        &state,
        PlaceOrder { order_id },
    )
    .await
    .unwrap();

    assert_eq!(v1_events.len(), 1);
    match &v1_events[0] {
        PlaceOrderEvent::OrderPlaced(e) => assert_eq!(e.order_id, order_id),
    }

    // v2 handler produces PlaceOrderV2Event::OrderPlacedV2 (on fresh state)
    let state2 = OrderAggregate::default();
    let v2_events = CommandHandler::<OrderAggregate>::handle(
        &PlaceOrderV2Handler,
        &state2,
        PlaceOrderV2 {
            order_id,
            priority: 7,
        },
    )
    .await
    .unwrap();

    assert_eq!(v2_events.len(), 1);
    match &v2_events[0] {
        PlaceOrderV2Event::OrderPlacedV2(e) => {
            assert_eq!(e.order_id, order_id);
            assert_eq!(e.priority, 7);
        }
    }
}

#[tokio::test]
async fn test_command_version_persisted_to_command_store() {
    let store = InMemoryCommandStore::new();
    let id = AggregateId::new();

    let cmd = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    };
    store.append(cmd).unwrap();

    let loaded = store.load_range(&id, None, None).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].command_version, 1);
}

#[tokio::test]
async fn test_no_upcasting_during_hydration() {
    let agg_id = AggregateId::new();
    let order_id = Uuid::new_v4();
    let event_store = InMemoryEventStore::new();

    // Create v1 and v2 envelopes with known payloads
    let v1_payload = serde_json::to_vec(&OrderPlaced { order_id }).expect("serialize v1");
    let v2_payload = serde_json::to_vec(&OrderPlacedV2 {
        order_id,
        priority: 3,
    })
    .expect("serialize v2");

    let v1_envelope = EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: agg_id.clone(),
        version: Version::initial(),
        event_type: "OrderPlaced".to_string(),
        event_version: 1,
        payload: Bytes::from(v1_payload.clone()),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    };
    let v2_envelope = EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: agg_id.clone(),
        version: Version::initial(),
        event_type: "OrderPlacedV2".to_string(),
        event_version: 2,
        payload: Bytes::from(v2_payload.clone()),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    };

    // Store and reload — raw bytes must survive the round-trip
    event_store
        .append(
            &agg_id,
            Version::initial(),
            vec![v1_envelope, v2_envelope],
        )
        .unwrap();
    let loaded = event_store.load(&agg_id).unwrap();

    // Assert payloads are unchanged (no transformation, no upcasting)
    assert_eq!(loaded[0].payload.as_ref(), v1_payload.as_slice());
    assert_eq!(loaded[1].payload.as_ref(), v2_payload.as_slice());

    // Hydrate — version-matched routing dispatches to correct combiners
    let mut state = OrderAggregate::default();
    OrderAggregate::hydrate(&mut state, loaded.into_iter()).unwrap();

    assert!(state.placed);
    assert_eq!(state.priority, Some(3));
}
