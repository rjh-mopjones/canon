use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::harness::TestHarness;

fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial().next(),
        event_type: "OrderPlaced".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

#[tokio::test]
async fn test_all_three_consumers_receive_event() {
    let harness = TestHarness::new();

    // Register three consumers (event store, projection, publisher)
    let event_store_consumer = harness.outbound_queue.register_consumer().unwrap();
    let projection_consumer = harness.outbound_queue.register_consumer().unwrap();
    let publisher_consumer = harness.outbound_queue.register_consumer().unwrap();

    // Publish one event
    let id = AggregateId::new();
    let event = make_event(&id);
    let event_id = event.event_id;
    harness.publish_to_outbound(event);

    // Each consumer receives the event independently
    let es_event = harness
        .outbound_queue
        .receive(&event_store_consumer)
        .unwrap()
        .unwrap();
    let proj_event = harness
        .outbound_queue
        .receive(&projection_consumer)
        .unwrap()
        .unwrap();
    let pub_event = harness
        .outbound_queue
        .receive(&publisher_consumer)
        .unwrap()
        .unwrap();

    assert_eq!(es_event.event_id, event_id);
    assert_eq!(proj_event.event_id, event_id);
    assert_eq!(pub_event.event_id, event_id);
}

#[tokio::test]
async fn test_consumers_receive_independently() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let consumer_a = harness.outbound_queue.register_consumer().unwrap();
    let consumer_b = harness.outbound_queue.register_consumer().unwrap();

    let event1 = make_event(&id);
    let event2 = make_event(&id);
    let id1 = event1.event_id;
    let id2 = event2.event_id;

    harness.publish_to_outbound(event1);
    harness.publish_to_outbound(event2);

    // Consumer A receives first event
    let a1 = harness
        .outbound_queue
        .receive(&consumer_a)
        .unwrap()
        .unwrap();
    assert_eq!(a1.event_id, id1);

    // Consumer B still has both events -- A's receive did not affect B
    let b1 = harness
        .outbound_queue
        .receive(&consumer_b)
        .unwrap()
        .unwrap();
    let b2 = harness
        .outbound_queue
        .receive(&consumer_b)
        .unwrap()
        .unwrap();
    assert_eq!(b1.event_id, id1);
    assert_eq!(b2.event_id, id2);

    // Consumer A can still receive second event
    let a2 = harness
        .outbound_queue
        .receive(&consumer_a)
        .unwrap()
        .unwrap();
    assert_eq!(a2.event_id, id2);
}

#[tokio::test]
async fn test_consumer_registered_after_publish_receives_nothing() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Publish before any consumer exists
    harness.publish_to_outbound(make_event(&id));

    // Register consumer after the event was published
    let late_consumer = harness.outbound_queue.register_consumer().unwrap();
    assert!(harness
        .outbound_queue
        .receive(&late_consumer)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_multiple_events_fan_out_to_all_consumers() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let consumer_es = harness.outbound_queue.register_consumer().unwrap();
    let consumer_proj = harness.outbound_queue.register_consumer().unwrap();
    let consumer_pub = harness.outbound_queue.register_consumer().unwrap();

    // Publish 5 events
    let mut event_ids = Vec::new();
    for _ in 0..5 {
        let event = make_event(&id);
        event_ids.push(event.event_id);
        harness.publish_to_outbound(event);
    }

    // Each consumer receives all 5
    for consumer in [&consumer_es, &consumer_proj, &consumer_pub] {
        let mut received_ids = Vec::new();
        while let Some(event) = harness.outbound_queue.receive(consumer).unwrap() {
            received_ids.push(event.event_id);
        }
        assert_eq!(received_ids.len(), 5);
        assert_eq!(received_ids, event_ids);
    }
}

#[tokio::test]
async fn test_outbox_to_outbound_queue_to_publisher() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Register publisher consumer
    let pub_consumer = harness.outbound_queue.register_consumer().unwrap();

    // Simulate outbox processor: drain outbox event to outbound queue
    let event = make_event(&id);
    let event_id = event.event_id;
    harness.publish_to_outbound(event);

    // Publisher consumer picks up the event and publishes externally
    let received = harness
        .outbound_queue
        .receive(&pub_consumer)
        .unwrap()
        .unwrap();
    harness.publish_external(received, "canon.test.events");

    let published = harness.published_events();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0.event_id, event_id);
    assert_eq!(published[0].1, "canon.test.events");
}
