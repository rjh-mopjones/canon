use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;
use canon_test::harness::TestHarness;

#[tokio::test]
async fn test_snapshot_written_every_n_events() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Append 50 events
    let events: Vec<EventEnvelope> = (0..50)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();
    harness.append_events(&id, Version::initial(), events);

    // Simulate event store consumer: check version % 50 == 0
    let loaded = harness.load_events(&id);
    for event in &loaded {
        if event.version.as_u64() % 50 == 0 {
            let snapshot = Snapshot {
                aggregate_id: id.clone(),
                version: event.version,
                state: Bytes::from_static(b"snapshot_state"),
                taken_at: Utc::now(),
            };
            harness.save_snapshot(snapshot);
        }
    }

    // Assert snapshot was written at version 50
    let snap = harness.load_snapshot(&id).unwrap();
    assert_eq!(snap.version.as_u64(), 50);
}

#[tokio::test]
async fn test_no_snapshot_before_threshold() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Append 49 events (below threshold of 50)
    let events: Vec<EventEnvelope> = (0..49)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();
    harness.append_events(&id, Version::initial(), events);

    // Simulate event store consumer: check version % 50 == 0
    let loaded = harness.load_events(&id);
    for event in &loaded {
        if event.version.as_u64() % 50 == 0 {
            harness.save_snapshot(Snapshot {
                aggregate_id: id.clone(),
                version: event.version,
                state: Bytes::from_static(b"state"),
                taken_at: Utc::now(),
            });
        }
    }

    // No snapshot should exist
    assert!(harness.load_snapshot(&id).is_none());
}

#[tokio::test]
async fn test_snapshot_at_multiple_thresholds() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let snapshot_every = 10u64;

    // Append 25 events
    let events: Vec<EventEnvelope> = (0..25)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();
    harness.append_events(&id, Version::initial(), events);

    let loaded = harness.load_events(&id);
    let mut snapshot_count = 0u32;
    for event in &loaded {
        if event.version.as_u64() % snapshot_every == 0 {
            harness.save_snapshot(Snapshot {
                aggregate_id: id.clone(),
                version: event.version,
                state: Bytes::from_static(b"state"),
                taken_at: Utc::now(),
            });
            snapshot_count += 1;
        }
    }

    // Snapshots at v10 and v20 — two threshold crossings
    assert_eq!(snapshot_count, 2);
    // Latest snapshot is v20 (upsert semantics)
    let snap = harness.load_snapshot(&id).unwrap();
    assert_eq!(snap.version.as_u64(), 20);
}

#[tokio::test]
async fn test_hydrate_from_snapshot_skips_earlier_events() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let order_id = Uuid::new_v4();

    // Append 3 events: Placed, Cancelled, PlacedV2
    let events = vec![
        make_placed_envelope(&id, order_id),
        make_cancelled_envelope(&id, "changed mind"),
        make_placed_v2_envelope(&id, order_id, 5),
    ];
    harness.append_events(&id, Version::initial(), events);

    // Snapshot at version 2 (after Placed + Cancelled)
    let snap_state = serde_json::to_vec(&OrderAggregate {
        placed: true,
        cancelled: true,
        priority: None,
    })
    .expect("serialize snapshot");
    harness.save_snapshot(Snapshot {
        aggregate_id: id.clone(),
        version: version(2),
        state: Bytes::from(snap_state),
        taken_at: Utc::now(),
    });

    // Load snapshot and hydrate only remaining events
    let snap = harness.load_snapshot(&id).unwrap();
    let mut state: OrderAggregate =
        serde_json::from_slice(&snap.state).expect("deserialize snapshot");

    let remaining = harness
        .event_store
        .load_from_version(&id, snap.version.next())
        .unwrap();
    assert_eq!(remaining.len(), 1); // only version 3

    OrderAggregate::hydrate(&mut state, remaining.into_iter()).unwrap();
    assert!(state.placed);
    assert!(state.cancelled);
    assert_eq!(state.priority, Some(5));
}
