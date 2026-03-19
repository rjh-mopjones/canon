use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_snapshot_written_every_n_events() {
    let event_store = InMemoryEventStore::new();
    let snapshot_store = InMemorySnapshotStore::new();
    let id = AggregateId::new();

    // Append 50 events
    let events: Vec<EventEnvelope> = (0..50)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();
    event_store
        .append(&id, Version::initial(), events)
        .unwrap();

    // Simulate event store consumer: check version % 50 == 0
    let loaded = event_store.load(&id).unwrap();
    for event in &loaded {
        if event.version.as_u64() % 50 == 0 {
            let snapshot = Snapshot {
                aggregate_id: id.clone(),
                version: event.version,
                state: Bytes::from_static(b"snapshot_state"),
                taken_at: Utc::now(),
            };
            snapshot_store.save(snapshot).unwrap();
        }
    }

    // Assert snapshot was written at version 50
    let snap = snapshot_store.load(&id).unwrap().unwrap();
    assert_eq!(snap.version.as_u64(), 50);
}
