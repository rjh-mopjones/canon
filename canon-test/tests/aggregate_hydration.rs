use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_hydrate_from_events() {
    let agg_id = AggregateId::new();
    let order_id = Uuid::new_v4();
    let events = vec![
        make_placed_envelope(&agg_id, order_id),
        make_cancelled_envelope(&agg_id, "changed mind"),
    ];

    let mut state = OrderAggregate::default();
    OrderAggregate::hydrate(&mut state, events.into_iter()).unwrap();

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
        make_placed_envelope(&id, order_id),
        make_cancelled_envelope(&id, "snap"),
    ];
    event_store.append(&id, Version::initial(), events).unwrap();

    // Save snapshot at version 1 (after Placed: placed=true, cancelled=false)
    let snap_version = version(1);
    let snap_state = serde_json::to_vec(&OrderAggregate {
        placed: true,
        cancelled: false,
        priority: None,
    })
    .expect("serialize snapshot state");
    let snapshot = Snapshot {
        aggregate_id: id.clone(),
        version: snap_version,
        state: Bytes::from(snap_state),
        taken_at: Utc::now(),
    };
    snapshot_store.save(snapshot).unwrap();

    // Load snapshot
    let loaded_snap = snapshot_store.load(&id).unwrap().unwrap();
    assert_eq!(loaded_snap.version.as_u64(), 1);

    // Reconstruct state from snapshot
    let mut state: OrderAggregate =
        serde_json::from_slice(&loaded_snap.state).expect("deserialize snapshot");

    // Load events after snapshot version
    let remaining = event_store
        .load_from_version(&id, loaded_snap.version.next())
        .unwrap();
    assert_eq!(remaining.len(), 1); // only version 2

    // Hydrate remaining events — no upcasting, version-matched routing
    OrderAggregate::hydrate(&mut state, remaining.into_iter()).unwrap();

    assert!(state.placed);
    assert!(state.cancelled);
}
