use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::*;
use canon_test::domain::make_command_envelope;
use canon_test::harness::TestHarness;

#[tokio::test]
async fn test_counterfactual_same_commands() {
    let harness = TestHarness::new();
    let id = AggregateId::new();
    let payload = Bytes::from_static(b"place_order");

    // Store original command
    let cmd = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        command_type: "TestCommand".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: payload.clone(),
        command_version: 1,
    };
    harness.command_store.append(cmd).unwrap();

    let replay = harness.counterfactual_replay();

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
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Store 2 original commands
    let cmd1 = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        command_type: "PlaceOrder".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"place"),
        command_version: 1,
    };
    let cmd2 = CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: id.clone(),
        command_type: "CancelOrder".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"cancel"),
        command_version: 1,
    };
    harness.command_store.append(cmd1).unwrap();
    harness.command_store.append(cmd2).unwrap();

    let replay = harness.counterfactual_replay();

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

#[tokio::test]
async fn test_counterfactual_branch_beyond_end_appends() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // No commands stored
    let replay = harness.counterfactual_replay();
    let substitute = make_command_envelope(&id, b"new_cmd");

    let result = replay
        .replay(CounterfactualRequest {
            aggregate_id: id,
            branch_version: Version::initial(),
            substituted_command: substitute,
        })
        .await
        .unwrap();

    assert_eq!(result.counterfactual_commands.len(), 1);
    assert_eq!(result.diff.added.len(), 1);
    assert!(result.diff.removed.is_empty());
    assert!(result.diff.unchanged.is_empty());
}

#[tokio::test]
async fn test_counterfactual_substitution_mid_history() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    // Store 3 commands: A, B, C
    for payload in [b"cmd_a" as &[u8], b"cmd_b", b"cmd_c"] {
        let cmd = CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::copy_from_slice(payload),
            command_version: 1,
        };
        harness.command_store.append(cmd).unwrap();
    }

    let replay = harness.counterfactual_replay();

    // Substitute command at index 1 (B) with different payload
    let substitute = make_command_envelope(&id, b"cmd_b_alt");

    let result = replay
        .replay(CounterfactualRequest {
            aggregate_id: id,
            branch_version: Version::from_u64(1), // index 1
            substituted_command: substitute,
        })
        .await
        .unwrap();

    // A and C are unchanged, B is replaced
    assert_eq!(result.diff.unchanged.len(), 2);
    assert_eq!(result.diff.added.len(), 1);
    assert_eq!(result.diff.removed.len(), 1);
    assert_eq!(result.original_commands.len(), 3);
    assert_eq!(result.counterfactual_commands.len(), 3);
}

#[tokio::test]
async fn test_counterfactual_diff_preserves_original_commands() {
    let harness = TestHarness::new();
    let id = AggregateId::new();

    let original_cmd_id = Uuid::new_v4();
    let cmd = CommandEnvelope {
        command_id: original_cmd_id,
        aggregate_id: id.clone(),
        command_type: "PlaceOrder".into(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"original"),
        command_version: 1,
    };
    harness.command_store.append(cmd).unwrap();

    let replay = harness.counterfactual_replay();
    let substitute = make_command_envelope(&id, b"modified");

    let result = replay
        .replay(CounterfactualRequest {
            aggregate_id: id,
            branch_version: Version::initial(),
            substituted_command: substitute,
        })
        .await
        .unwrap();

    // Original commands in result preserve the original command_id
    assert_eq!(result.original_commands.len(), 1);
    assert_eq!(result.original_commands[0].command_id, original_cmd_id);
}
