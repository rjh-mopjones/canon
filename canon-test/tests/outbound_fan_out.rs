use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

#[tokio::test]
async fn test_all_three_consumers_receive_event() {
    let outbound_queue = InMemoryOutboundQueue::new();

    // Register three consumers (event store, projection, publisher)
    let event_store_consumer = outbound_queue.register_consumer().unwrap();
    let projection_consumer = outbound_queue.register_consumer().unwrap();
    let publisher_consumer = outbound_queue.register_consumer().unwrap();

    // Publish one event
    let event = EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: AggregateId::new(),
        version: Version::initial().next(),
        event_type: "OrderPlaced".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    };
    let event_id = event.event_id;
    outbound_queue.publish(event).unwrap();

    // Each consumer receives the event independently
    let es_event = outbound_queue
        .receive(&event_store_consumer)
        .unwrap()
        .unwrap();
    let proj_event = outbound_queue
        .receive(&projection_consumer)
        .unwrap()
        .unwrap();
    let pub_event = outbound_queue
        .receive(&publisher_consumer)
        .unwrap()
        .unwrap();

    assert_eq!(es_event.event_id, event_id);
    assert_eq!(proj_event.event_id, event_id);
    assert_eq!(pub_event.event_id, event_id);
}
