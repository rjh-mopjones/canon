use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::harness::TestHarness;

fn make_command_msg(aggregate_id: &AggregateId) -> IncomingMessage {
    IncomingMessage::Command(CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    })
}

fn make_external_event_msg(aggregate_id: &AggregateId, event_type: &str) -> IncomingMessage {
    IncomingMessage::ExternalEvent(EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial().next(),
        event_type: event_type.to_owned(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    })
}

fn make_internal_event_msg(aggregate_id: &AggregateId, event_type: &str) -> IncomingMessage {
    IncomingMessage::InternalEvent(EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::initial().next(),
        event_type: event_type.to_owned(),
        event_version: 1,
        payload: Bytes::from_static(b"{}"),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    })
}

#[tokio::test]
async fn test_oversight_not_ready_accumulation() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    harness
        .inbox
        .register_handler("h1", |_| Oversight::NotReady)
        .unwrap();

    // Submit 3 messages -- all accumulate, nothing dispatched
    for _ in 0..3 {
        harness
            .inbox
            .submit("h1", make_command_msg(&id), &harness.inbound_queue)
            .unwrap();
    }

    assert!(harness.inbound_queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_discard() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    harness
        .inbox
        .register_handler("h1", |_| Oversight::Discard)
        .unwrap();

    harness
        .inbox
        .submit("h1", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();

    // Window cleared, nothing dispatched
    assert!(harness.inbound_queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_ready_dispatch() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    harness
        .inbox
        .register_handler("h1", |_| Oversight::Ready)
        .unwrap();

    harness
        .inbox
        .submit("h1", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();

    // Batch dispatched to inbound queue
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
}

#[tokio::test]
async fn test_oversight_transitions_not_ready_to_ready() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Becomes Ready after accumulating 3 messages
    harness
        .inbox
        .register_handler("h1", |accumulated| {
            if accumulated.len() >= 3 {
                Oversight::Ready
            } else {
                Oversight::NotReady
            }
        })
        .unwrap();

    harness
        .inbox
        .submit("h1", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    harness
        .inbox
        .submit("h1", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    harness
        .inbox
        .submit("h1", make_command_msg(&id), &harness.inbound_queue)
        .unwrap();
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 3);
}

#[tokio::test]
async fn test_oversight_discard_on_decommissioned_event() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Mimics the UnloadingHandler oversight from CLAUDE.md:
    // Discard if ShipDecommissioned seen, Ready if has arrival + manifest,
    // otherwise NotReady
    harness
        .inbox
        .register_handler("unloading", |accumulated| {
            if accumulated.iter().any(|m| {
                matches!(
                    m,
                    IncomingMessage::ExternalEvent(e) if e.event_type == "ShipDecommissioned"
                )
            }) {
                return Oversight::Discard;
            }
            let has_arrival = accumulated.iter().any(|m| {
                matches!(
                    m,
                    IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"
                )
            });
            let has_manifest = accumulated.iter().any(|m| {
                matches!(
                    m,
                    IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"
                )
            });
            if has_arrival && has_manifest {
                Oversight::Ready
            } else {
                Oversight::NotReady
            }
        })
        .unwrap();

    // Submit arrival -- NotReady
    harness
        .inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipArrivedAtStation"),
            &harness.inbound_queue,
        )
        .unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    // Submit decommission -- Discard
    harness
        .inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipDecommissioned"),
            &harness.inbound_queue,
        )
        .unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_ready_with_arrival_and_manifest() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    harness
        .inbox
        .register_handler("unloading", |accumulated| {
            let has_arrival = accumulated.iter().any(|m| {
                matches!(
                    m,
                    IncomingMessage::ExternalEvent(e) if e.event_type == "ShipArrivedAtStation"
                )
            });
            let has_manifest = accumulated.iter().any(|m| {
                matches!(
                    m,
                    IncomingMessage::InternalEvent(e) if e.event_type == "ManifestCreated"
                )
            });
            if has_arrival && has_manifest {
                Oversight::Ready
            } else {
                Oversight::NotReady
            }
        })
        .unwrap();

    // Submit arrival -- NotReady
    harness
        .inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipArrivedAtStation"),
            &harness.inbound_queue,
        )
        .unwrap();
    assert!(harness.inbound_queue.receive().unwrap().is_none());

    // Submit manifest -- now Ready
    harness
        .inbox
        .submit(
            "unloading",
            make_internal_event_msg(&id, "ManifestCreated"),
            &harness.inbound_queue,
        )
        .unwrap();
    let batch = harness.inbound_queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 2);
}

#[tokio::test]
async fn test_oversight_unregistered_handler_returns_err() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let result = harness.inbox.submit(
        "unknown_handler",
        make_command_msg(&id),
        &harness.inbound_queue,
    );
    assert!(matches!(
        result,
        Err(InboxError::HandlerNotRegistered { .. })
    ));
}
