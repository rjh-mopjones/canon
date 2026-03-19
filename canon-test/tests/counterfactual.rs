use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::make_command_envelope;
use canon_test::harness::TestCounterfactualReplay;

#[tokio::test]
async fn test_counterfactual_same_commands() {
    let command_store = InMemoryCommandStore::new();
    let event_store = InMemoryEventStore::new();
    let id = AggregateId::new();
    let payload = Bytes::from_static(b"place_order");

    // Store original command
    let cmd = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: payload.clone(),
        command_version: 1,
    };
    command_store.append(cmd).unwrap();

    let replay = TestCounterfactualReplay {
        event_store,
        command_store,
    };

    // Substitute with the same payload
    let substitute = make_command_envelope(&id, b"place_order");

    let request = CounterfactualRequest {
        aggregate_id: id,
        branch_version: Version::initial(), // substitute at index 0
        substituted_command: substitute,
    };

    let result = replay.replay(request).await.unwrap();

    assert_eq!(result.diff.unchanged.len(), 1);
    assert!(result.diff.added.is_empty());
    assert!(result.diff.removed.is_empty());
}

#[tokio::test]
async fn test_counterfactual_different_commands() {
    let command_store = InMemoryCommandStore::new();
    let event_store = InMemoryEventStore::new();
    let id = AggregateId::new();

    // Store 2 original commands
    let cmd1 = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"place"),
        command_version: 1,
    };
    let cmd2 = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"cancel"),
        command_version: 1,
    };
    command_store.append(cmd1).unwrap();
    command_store.append(cmd2).unwrap();

    let replay = TestCounterfactualReplay {
        event_store,
        command_store,
    };

    // Substitute first command with different payload
    let substitute = make_command_envelope(&id, b"different");

    let request = CounterfactualRequest {
        aggregate_id: id,
        branch_version: Version::initial(), // substitute at index 0
        substituted_command: substitute,
    };

    let result = replay.replay(request).await.unwrap();

    // First command differs, second is unchanged
    assert_eq!(result.diff.added.len(), 1);
    assert_eq!(result.diff.removed.len(), 1);
    assert_eq!(result.diff.unchanged.len(), 1);
}
