use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_handle_valid_command() {
    let state = OrderState::default();
    let order_id = Uuid::new_v4();

    let events = TestAggregate::handle(&state, OrderCommand::Place { order_id })
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0], OrderEvent::Placed { order_id });
}

#[tokio::test]
async fn test_handle_invalid_command() {
    let state = OrderState {
        placed: false,
        cancelled: true,
    };

    let result = TestAggregate::handle(
        &state,
        OrderCommand::Cancel {
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
    let state = OrderState::default();
    let events = TestAggregate::handle(&state, OrderCommand::Place { order_id })
        .await
        .unwrap();
    let envelopes: Vec<EventEnvelope> = events
        .iter()
        .map(|e| make_event_envelope(&id, e))
        .collect();
    store
        .append(&id, Version::initial(), envelopes)
        .unwrap();

    // Load, hydrate, then run second command: Cancel
    let loaded = store.load(&id).unwrap();
    let mut state2 = OrderState::default();
    for e in &loaded {
        let domain_event = TestAggregate::upcast(e.clone()).unwrap();
        TestAggregate::apply(&mut state2, &domain_event);
    }

    let events2 = TestAggregate::handle(
        &state2,
        OrderCommand::Cancel {
            reason: "test".into(),
        },
    )
    .await
    .unwrap();
    let current_version = loaded.last().unwrap().version;
    let envelopes2: Vec<EventEnvelope> = events2
        .iter()
        .map(|e| make_event_envelope(&id, e))
        .collect();
    store.append(&id, current_version, envelopes2).unwrap();

    // Assert sequential version progression
    let all_events = store.load(&id).unwrap();
    assert_eq!(all_events.len(), 2);
    assert_eq!(all_events[0].version.as_u64(), 1);
    assert_eq!(all_events[1].version.as_u64(), 2);
}
