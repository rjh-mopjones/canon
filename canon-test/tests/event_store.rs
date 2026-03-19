use uuid::Uuid;

use canon_core::*;
use canon_test::domain::*;

#[tokio::test]
async fn test_append_and_load() {
    let store = InMemoryEventStore::new();
    let id = AggregateId::new();

    let events = vec![
        make_placed_envelope(&id, Uuid::new_v4()),
        make_cancelled_envelope(&id, "r1"),
        make_placed_envelope(&id, Uuid::new_v4()),
    ];

    store.append(&id, Version::initial(), events).unwrap();
    let loaded = store.load(&id).unwrap();

    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[0].version.as_u64(), 1);
    assert_eq!(loaded[1].version.as_u64(), 2);
    assert_eq!(loaded[2].version.as_u64(), 3);
}

#[tokio::test]
async fn test_optimistic_concurrency_conflict() {
    let store = InMemoryEventStore::new();
    let id = AggregateId::new();

    let event = make_placed_envelope(&id, Uuid::new_v4());
    store.append(&id, Version::initial(), vec![event]).unwrap();

    // Current version is 1; appending with expected_version=0 must fail
    let event2 = make_cancelled_envelope(&id, "late");
    let result = store.append(&id, Version::initial(), vec![event2]);

    assert!(matches!(result, Err(EventStoreError::VersionConflict { .. })));
}

#[tokio::test]
async fn test_load_from_version() {
    let store = InMemoryEventStore::new();
    let id = AggregateId::new();

    let events: Vec<EventEnvelope> = (0..5)
        .map(|_| make_placed_envelope(&id, Uuid::new_v4()))
        .collect();

    store.append(&id, Version::initial(), events).unwrap();

    // Load from version 3 (versions 3, 4, 5)
    let loaded = store.load_from_version(&id, version(3)).unwrap();

    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[0].version.as_u64(), 3);
    assert_eq!(loaded[1].version.as_u64(), 4);
    assert_eq!(loaded[2].version.as_u64(), 5);
}
