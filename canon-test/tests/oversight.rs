use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;

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
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
        .register_handler("h1", |_| Oversight::NotReady)
        .unwrap();

    // Submit 3 messages -- all accumulate, nothing dispatched
    for _ in 0..3 {
        inbox.submit("h1", make_command_msg(&id), &queue).unwrap();
    }

    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_discard() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
        .register_handler("h1", |_| Oversight::Discard)
        .unwrap();

    inbox.submit("h1", make_command_msg(&id), &queue).unwrap();

    // Window cleared, nothing dispatched
    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_ready_dispatch() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox.register_handler("h1", |_| Oversight::Ready).unwrap();

    inbox.submit("h1", make_command_msg(&id), &queue).unwrap();

    // Batch dispatched to inbound queue
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 1);
}

#[tokio::test]
async fn test_oversight_transitions_not_ready_to_ready() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    // Becomes Ready after accumulating 3 messages
    inbox
        .register_handler("h1", |accumulated| {
            if accumulated.len() >= 3 {
                Oversight::Ready
            } else {
                Oversight::NotReady
            }
        })
        .unwrap();

    inbox.submit("h1", make_command_msg(&id), &queue).unwrap();
    assert!(queue.receive().unwrap().is_none());

    inbox.submit("h1", make_command_msg(&id), &queue).unwrap();
    assert!(queue.receive().unwrap().is_none());

    inbox.submit("h1", make_command_msg(&id), &queue).unwrap();
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 3);
}

#[tokio::test]
async fn test_oversight_discard_on_decommissioned_event() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    // Mimics the UnloadingHandler oversight from CLAUDE.md:
    // Discard if ShipDecommissioned seen, Ready if has arrival + manifest,
    // otherwise NotReady
    inbox
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
    inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipArrivedAtStation"),
            &queue,
        )
        .unwrap();
    assert!(queue.receive().unwrap().is_none());

    // Submit decommission -- Discard
    inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipDecommissioned"),
            &queue,
        )
        .unwrap();
    assert!(queue.receive().unwrap().is_none());
}

#[tokio::test]
async fn test_oversight_ready_with_arrival_and_manifest() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    inbox
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
    inbox
        .submit(
            "unloading",
            make_external_event_msg(&id, "ShipArrivedAtStation"),
            &queue,
        )
        .unwrap();
    assert!(queue.receive().unwrap().is_none());

    // Submit manifest -- now Ready
    inbox
        .submit(
            "unloading",
            make_internal_event_msg(&id, "ManifestCreated"),
            &queue,
        )
        .unwrap();
    let batch = queue.receive().unwrap().unwrap();
    assert_eq!(batch.len(), 2);
}

#[tokio::test]
async fn test_oversight_unregistered_handler_returns_err() {
    let inbox = InMemoryInbox::new();
    let queue = InMemoryInboundQueue::new();
    let id = AggregateId::new();

    let result = inbox.submit("unknown_handler", make_command_msg(&id), &queue);
    assert!(matches!(
        result,
        Err(InboxError::HandlerNotRegistered { .. })
    ));
}
