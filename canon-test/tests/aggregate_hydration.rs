use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_hydrate_from_events() {
    let order_id = Uuid::new_v4();
    let events = vec![
        OrderEvent::Placed { order_id },
        OrderEvent::Cancelled {
            reason: "changed mind".into(),
        },
    ];

    let mut state = OrderState::default();
    TestAggregate::hydrate(&mut state, events.into_iter());

    assert!(state.placed);
    assert!(state.cancelled);
}

#[tokio::test]
async fn test_hydrate_from_snapshot_plus_events() {
    let id = AggregateId::new();
    let event_store = InMemoryEventStore::new();
    let snapshot_store = InMemorySnapshotStore::new();
    let order_id = Uuid::new_v4();

    // Append 2 events: Placed (v1), Cancelled (v2)
    let events = vec![
        make_event_envelope(&id, &OrderEvent::Placed { order_id }),
        make_event_envelope(&id, &OrderEvent::Cancelled { reason: "snap".into() }),
    ];
    event_store.append(&id, Version::initial(), events).unwrap();

    // Save snapshot at version 1 (after Placed: placed=true, cancelled=false)
    let snap_version = version(1);
    let snapshot = Snapshot {
        aggregate_id: id.clone(),
        version: snap_version,
        state: Bytes::from_static(b"placed"),
        taken_at: Utc::now(),
    };
    snapshot_store.save(snapshot).unwrap();

    // Load snapshot
    let loaded_snap = snapshot_store.load(&id).unwrap().unwrap();
    assert_eq!(loaded_snap.version.as_u64(), 1);

    // Reconstruct state from snapshot
    let mut state = OrderState {
        placed: true,
        cancelled: false,
    };

    // Load events after snapshot version
    let remaining = event_store
        .load_from_version(&id, loaded_snap.version.next())
        .unwrap();
    assert_eq!(remaining.len(), 1); // only version 2

    // Upcast and hydrate remaining events
    let domain_events: Vec<OrderEvent> = remaining
        .into_iter()
        .map(|e| TestAggregate::upcast(e).unwrap())
        .collect();
    TestAggregate::hydrate(&mut state, domain_events.into_iter());

    assert!(state.placed);
    assert!(state.cancelled);
}
