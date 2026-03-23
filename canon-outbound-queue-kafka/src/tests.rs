use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::{AggregateId, EventEnvelope, Version};

fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial().next(),
        event_type: "TestEvent".into(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

#[test]
fn test_parse_brokers() {
    let addrs = super::parse_brokers("localhost:9092,kafka:9093");
    assert_eq!(addrs.len(), 2);
    assert_eq!(addrs[0].host, "localhost");
    assert_eq!(addrs[0].port, 9092);
}

#[test]
fn test_event_serialization_roundtrip() {
    let agg_id = AggregateId::new();
    let event = make_event(&agg_id);
    let json = serde_json::to_vec(&event).unwrap();
    let deserialized: EventEnvelope = serde_json::from_slice(&json).unwrap();
    assert_eq!(deserialized.event_id, event.event_id);
    assert_eq!(deserialized.aggregate_id, agg_id);
}
