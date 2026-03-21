//! Debug inspection utilities for Canon aggregates.
//!
//! Provides [`DebugInspector`] for hydrating any aggregate from its event and
//! snapshot stores, plus JSON-serializable response types that downstream
//! crates (e.g. gateway) can serve on `/debug/*` endpoints.
//!
//! [`DebugEndpointHandler`] combines the inspector with a command store to
//! provide all three debug operations (aggregate state, events, commands)
//! through a single type that can be wired into any web framework.

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::traits::{CommandStore, EventStore, SnapshotStore};
use crate::{Aggregate, AggregateId, EventEnvelope, Version};

// ── Error ────────────────────────────────────────────────────────────────────

/// Errors that can occur during debug inspection.
#[derive(Debug, thiserror::Error)]
pub enum DebugInspectorError {
    /// Failed to load a snapshot from the snapshot store.
    #[error("snapshot store error: {0}")]
    SnapshotStore(String),

    /// Failed to load events from the event store.
    #[error("event store error: {0}")]
    EventStore(String),

    /// Failed to load commands from the command store.
    #[error("command store error: {0}")]
    CommandStore(String),

    /// Failed to deserialize snapshot state.
    #[error("snapshot deserialization error: {0}")]
    SnapshotDeserialization(String),

    /// Aggregate hydration failed.
    #[error("hydration error: {0}")]
    Hydration(String),

    /// Failed to serialize aggregate state to JSON.
    #[error("state serialization error: {0}")]
    StateSerialization(String),
}

// ── Response types ───────────────────────────────────────────────────────────

/// JSON-serializable response for a debug aggregate inspection.
#[derive(Debug, Clone, Serialize)]
pub struct DebugAggregateResponse {
    /// The aggregate instance identifier.
    pub aggregate_id: AggregateId,
    /// Current version after hydration.
    pub version: Version,
    /// Serialized aggregate state as JSON value.
    pub state: serde_json::Value,
    /// Version of the snapshot used for hydration, if any.
    pub snapshot_version: Option<u64>,
    /// Number of events replayed after the snapshot (or from zero).
    pub events_replayed: u64,
}

/// JSON-serializable response for a debug event inspection.
#[derive(Debug, Clone, Serialize)]
pub struct DebugEventResponse {
    pub event_id: Uuid,
    pub aggregate_id: AggregateId,
    pub version: Version,
    pub event_type: String,
    pub event_version: u32,
    /// Decoded event payload as JSON. Falls back to a string representation
    /// if the payload is not valid JSON.
    pub payload: serde_json::Value,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
}

/// JSON-serializable response for a debug command inspection.
#[derive(Debug, Clone, Serialize)]
pub struct DebugCommandResponse {
    pub command_id: Uuid,
    pub aggregate_id: AggregateId,
    pub command_type: String,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    /// Decoded command payload as JSON. Falls back to a string representation
    /// if the payload is not valid JSON.
    pub payload: serde_json::Value,
    pub command_version: u32,
}

// ── Inspector ────────────────────────────────────────────────────────────────

/// Generic debug inspector that can hydrate any aggregate and return
/// JSON-serializable state.
///
/// Works with any [`EventStore`] and [`SnapshotStore`] implementations,
/// including the in-memory variants in `canon-core`.
pub struct DebugInspector<ES, SS> {
    event_store: ES,
    snapshot_store: SS,
}

impl<ES, SS> DebugInspector<ES, SS>
where
    ES: EventStore,
    SS: SnapshotStore,
{
    /// Create a new `DebugInspector` backed by the given stores.
    pub fn new(event_store: ES, snapshot_store: SS) -> Self {
        Self {
            event_store,
            snapshot_store,
        }
    }

    /// Hydrate an aggregate and return its debug representation.
    ///
    /// 1. Loads the latest snapshot (if any).
    /// 2. Loads events from the snapshot version onward (or from zero).
    /// 3. Folds events via `A::hydrate()`.
    /// 4. Serializes the resulting state to JSON.
    pub async fn inspect<A: Aggregate>(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<DebugAggregateResponse, DebugInspectorError>
    where
        A::State: serde::de::DeserializeOwned,
    {
        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            "debug inspect: loading aggregate"
        );

        // 1. Try to load snapshot
        let snapshot = self
            .snapshot_store
            .load(aggregate_id)
            .await
            .map_err(|e| DebugInspectorError::SnapshotStore(e.to_string()))?;

        let (mut state, snapshot_version, from_version) = match &snapshot {
            Some(snap) => {
                let deserialized: A::State = serde_json::from_slice(&snap.state)
                    .map_err(|e| DebugInspectorError::SnapshotDeserialization(e.to_string()))?;
                let snap_ver = snap.version.as_u64();
                // Load events *after* the snapshot version
                (deserialized, Some(snap_ver), snap.version.next())
            }
            None => (A::State::default(), None, Version::initial()),
        };

        // 2. Load events from the appropriate version
        let events = self
            .event_store
            .load_from_version(aggregate_id, from_version)
            .await
            .map_err(|e| DebugInspectorError::EventStore(e.to_string()))?;

        let events_replayed = events.len() as u64;

        // 3. Determine the final version
        let version = events
            .last()
            .map(|e| e.version)
            .or_else(|| snapshot.as_ref().map(|s| s.version))
            .unwrap_or_else(Version::initial);

        // 4. Hydrate
        A::hydrate(&mut state, events.into_iter())
            .map_err(|e| DebugInspectorError::Hydration(e.to_string()))?;

        // 5. Serialize state to JSON
        let state_json = serde_json::to_value(&state)
            .map_err(|e| DebugInspectorError::StateSerialization(e.to_string()))?;

        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            version = version.as_u64(),
            snapshot_version = ?snapshot_version,
            events_replayed = events_replayed,
            "debug inspect: hydration complete"
        );

        Ok(DebugAggregateResponse {
            aggregate_id: aggregate_id.clone(),
            version,
            state: state_json,
            snapshot_version,
            events_replayed,
        })
    }
}

// ── Endpoint handler ─────────────────────────────────────────────────────────

/// Unified debug endpoint handler that provides all three debug operations:
/// aggregate state inspection, event history, and command history.
///
/// Created by [`ServiceBuilder`](crate::ServiceBuilder) when debug endpoints
/// are enabled. Downstream crates (e.g. gateway) can wire this into their
/// web framework of choice.
///
/// # Example
///
/// ```ignore
/// let handler = service.debug_handler().unwrap();
/// // In an axum handler:
/// let response = handler.get_events(&aggregate_id, None).await?;
/// ```
pub struct DebugEndpointHandler<ES, SS, CmdS> {
    inspector: DebugInspector<ES, SS>,
    command_store: CmdS,
}

impl<ES, SS, CmdS> DebugEndpointHandler<ES, SS, CmdS>
where
    ES: EventStore,
    SS: SnapshotStore,
    CmdS: CommandStore,
{
    /// Create a new `DebugEndpointHandler` from the given stores.
    pub fn new(event_store: ES, snapshot_store: SS, command_store: CmdS) -> Self {
        Self {
            inspector: DebugInspector::new(event_store, snapshot_store),
            command_store,
        }
    }

    /// Hydrate an aggregate and return its debug representation.
    ///
    /// Delegates to [`DebugInspector::inspect`].
    pub async fn get_aggregate<A: Aggregate>(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<DebugAggregateResponse, DebugInspectorError>
    where
        A::State: serde::de::DeserializeOwned,
    {
        self.inspector.inspect::<A>(aggregate_id).await
    }

    /// Load event history for an aggregate, optionally from a specific version.
    ///
    /// Returns each event envelope as a [`DebugEventResponse`] with the payload
    /// decoded to JSON where possible.
    pub async fn get_events(
        &self,
        aggregate_id: &AggregateId,
        from_version: Option<Version>,
    ) -> Result<Vec<DebugEventResponse>, DebugInspectorError> {
        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            from_version = ?from_version.map(|v| v.as_u64()),
            "debug endpoint: loading events"
        );

        let events = match from_version {
            Some(version) => self
                .inspector
                .event_store
                .load_from_version(aggregate_id, version)
                .await
                .map_err(|e| DebugInspectorError::EventStore(e.to_string()))?,
            None => self
                .inspector
                .event_store
                .load(aggregate_id)
                .await
                .map_err(|e| DebugInspectorError::EventStore(e.to_string()))?,
        };

        let responses: Vec<DebugEventResponse> =
            events.into_iter().map(envelope_to_event_response).collect();

        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            event_count = responses.len(),
            "debug endpoint: events loaded"
        );

        Ok(responses)
    }

    /// Load command history for an aggregate.
    ///
    /// Returns each command envelope as a [`DebugCommandResponse`] with the
    /// payload decoded to JSON where possible.
    pub async fn get_commands(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<DebugCommandResponse>, DebugInspectorError> {
        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            "debug endpoint: loading commands"
        );

        let commands = self
            .command_store
            .load_for_aggregate(aggregate_id)
            .await
            .map_err(|e| DebugInspectorError::CommandStore(e.to_string()))?;

        let responses: Vec<DebugCommandResponse> = commands
            .into_iter()
            .map(|cmd| DebugCommandResponse {
                command_id: cmd.command_id,
                aggregate_id: cmd.aggregate_id,
                command_type: cmd.command_type,
                correlation_id: cmd.correlation_id,
                causation_id: cmd.causation_id,
                timestamp: cmd.timestamp,
                payload: decode_payload(&cmd.payload),
                command_version: cmd.command_version,
            })
            .collect();

        tracing::debug!(
            aggregate_id = %aggregate_id.as_uuid(),
            command_count = responses.len(),
            "debug endpoint: commands loaded"
        );

        Ok(responses)
    }
}

/// Convert an [`EventEnvelope`] to a [`DebugEventResponse`].
fn envelope_to_event_response(envelope: EventEnvelope) -> DebugEventResponse {
    DebugEventResponse {
        event_id: envelope.event_id,
        aggregate_id: envelope.aggregate_id,
        version: envelope.version,
        event_type: envelope.event_type,
        event_version: envelope.event_version,
        payload: decode_payload(&envelope.payload),
        correlation_id: envelope.correlation_id,
        causation_id: envelope.causation_id,
        timestamp: envelope.timestamp,
    }
}

/// Attempt to decode a payload as JSON. Falls back to a string representation
/// if the payload is not valid JSON.
fn decode_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}

// ── Debug endpoint configuration ─────────────────────────────────────────────

/// Resolves whether debug endpoints should be enabled.
///
/// Priority:
/// 1. Explicit `debug_endpoints(bool)` call on ServiceBuilder (highest)
/// 2. `CANON_DEBUG_ENDPOINTS` environment variable (`true`/`1` = enabled)
/// 3. Defaults to `false` (disabled)
pub fn resolve_debug_enabled(explicit: Option<bool>) -> bool {
    if let Some(enabled) = explicit {
        return enabled;
    }

    std::env::var("CANON_DEBUG_ENDPOINTS")
        .ok()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{InMemoryCommandStore, InMemoryEventStore, InMemorySnapshotStore};
    use crate::{CommandEnvelope, EventEnvelope, MacroError, Snapshot, Version};
    use bytes::Bytes;
    use chrono::Utc;
    use uuid::Uuid;

    // -- Test aggregate --------------------------------------------------------

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestAggregate {
        counter: u64,
        name: String,
    }

    impl Aggregate for TestAggregate {
        type State = TestAggregate;
        type Error = MacroError;

        fn hydrate(
            state: &mut Self::State,
            events: impl Iterator<Item = EventEnvelope>,
        ) -> Result<(), Self::Error> {
            for envelope in events {
                match envelope.event_type.as_str() {
                    "Incremented" => {
                        let event: IncrementedEvent = serde_json::from_slice(&envelope.payload)
                            .map_err(|e| MacroError(e.to_string()))?;
                        state.counter += event.amount;
                    }
                    "Named" => {
                        let event: NamedEvent = serde_json::from_slice(&envelope.payload)
                            .map_err(|e| MacroError(e.to_string()))?;
                        state.name = event.name;
                    }
                    other => {
                        return Err(MacroError(format!("unknown event type: {}", other)));
                    }
                }
            }
            Ok(())
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct IncrementedEvent {
        amount: u64,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct NamedEvent {
        name: String,
    }

    // -- Helpers ---------------------------------------------------------------

    fn make_event(
        aggregate_id: &AggregateId,
        version: Version,
        event_type: &str,
        payload: Vec<u8>,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version,
            event_type: event_type.to_string(),
            event_version: 1,
            payload: Bytes::from(payload),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    fn make_snapshot(
        aggregate_id: &AggregateId,
        version: Version,
        state: &TestAggregate,
    ) -> Snapshot {
        Snapshot {
            aggregate_id: aggregate_id.clone(),
            version,
            state: Bytes::from(serde_json::to_vec(state).expect("serialize snapshot")),
            taken_at: Utc::now(),
        }
    }

    // -- Tests -----------------------------------------------------------------

    #[tokio::test]
    async fn inspect_empty_aggregate() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let inspector = DebugInspector::new(event_store, snapshot_store);

        let agg_id = AggregateId::new();
        let result = inspector.inspect::<TestAggregate>(&agg_id).await;
        assert!(result.is_ok());

        let response = result.expect("should succeed");
        assert_eq!(response.aggregate_id, agg_id);
        assert_eq!(response.version, Version::initial());
        assert!(response.snapshot_version.is_none());
        assert_eq!(response.events_replayed, 0);

        // State should be the default
        let state: TestAggregate =
            serde_json::from_value(response.state).expect("deserialize state");
        assert_eq!(state, TestAggregate::default());
    }

    #[tokio::test]
    async fn inspect_hydration_from_events_only() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let agg_id = AggregateId::new();

        // Append 3 events
        let inc1 = IncrementedEvent { amount: 10 };
        let named = NamedEvent {
            name: "test-agg".to_string(),
        };
        let inc2 = IncrementedEvent { amount: 5 };

        let events = vec![
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&inc1).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Named",
                serde_json::to_vec(&named).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&inc2).expect("serialize"),
            ),
        ];

        event_store
            .append(&agg_id, Version::initial(), events)
            .expect("append events");

        let inspector = DebugInspector::new(event_store, snapshot_store);
        let response = inspector
            .inspect::<TestAggregate>(&agg_id)
            .await
            .expect("inspect");

        assert_eq!(response.aggregate_id, agg_id);
        assert_eq!(response.version.as_u64(), 3);
        assert!(response.snapshot_version.is_none());
        assert_eq!(response.events_replayed, 3);

        let state: TestAggregate =
            serde_json::from_value(response.state).expect("deserialize state");
        assert_eq!(state.counter, 15);
        assert_eq!(state.name, "test-agg");
    }

    #[tokio::test]
    async fn inspect_hydration_from_snapshot_plus_events() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let agg_id = AggregateId::new();

        // Save a snapshot at version 2 with counter=10, name="snapped"
        let snap_state = TestAggregate {
            counter: 10,
            name: "snapped".to_string(),
        };
        snapshot_store
            .save(make_snapshot(&agg_id, Version::from_u64(2), &snap_state))
            .expect("save snapshot");

        // Append 4 events (versions 1-4). The inspector should only replay 3 and 4.
        let events = vec![
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 5 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 5 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 7 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Named",
                serde_json::to_vec(&NamedEvent {
                    name: "updated".to_string(),
                })
                .expect("serialize"),
            ),
        ];

        event_store
            .append(&agg_id, Version::initial(), events)
            .expect("append events");

        let inspector = DebugInspector::new(event_store, snapshot_store);
        let response = inspector
            .inspect::<TestAggregate>(&agg_id)
            .await
            .expect("inspect");

        assert_eq!(response.aggregate_id, agg_id);
        assert_eq!(response.version.as_u64(), 4);
        assert_eq!(response.snapshot_version, Some(2));
        // Events at version 3 and 4 are replayed (after snapshot at version 2)
        assert_eq!(response.events_replayed, 2);

        let state: TestAggregate =
            serde_json::from_value(response.state).expect("deserialize state");
        // Snapshot had counter=10, then events 3 (Incremented +7) and 4 (Named)
        assert_eq!(state.counter, 17);
        assert_eq!(state.name, "updated");
    }

    #[tokio::test]
    async fn inspect_hydration_from_snapshot_only() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let agg_id = AggregateId::new();

        let snap_state = TestAggregate {
            counter: 42,
            name: "snapshot-only".to_string(),
        };
        snapshot_store
            .save(make_snapshot(&agg_id, Version::from_u64(5), &snap_state))
            .expect("save snapshot");

        // No events in the event store

        let inspector = DebugInspector::new(event_store, snapshot_store);
        let response = inspector
            .inspect::<TestAggregate>(&agg_id)
            .await
            .expect("inspect");

        assert_eq!(response.aggregate_id, agg_id);
        assert_eq!(response.version.as_u64(), 5);
        assert_eq!(response.snapshot_version, Some(5));
        assert_eq!(response.events_replayed, 0);

        let state: TestAggregate =
            serde_json::from_value(response.state).expect("deserialize state");
        assert_eq!(state.counter, 42);
        assert_eq!(state.name, "snapshot-only");
    }

    #[test]
    fn debug_aggregate_response_serializes_to_json() {
        let response = DebugAggregateResponse {
            aggregate_id: AggregateId::new(),
            version: Version::from_u64(10),
            state: serde_json::json!({"counter": 42, "name": "test"}),
            snapshot_version: Some(5),
            events_replayed: 5,
        };

        let json = serde_json::to_value(&response).expect("serialize response");
        assert_eq!(json["version"], 10);
        assert_eq!(json["snapshot_version"], 5);
        assert_eq!(json["events_replayed"], 5);
        assert_eq!(json["state"]["counter"], 42);
    }

    #[test]
    fn debug_event_response_serializes_to_json() {
        let response = DebugEventResponse {
            event_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            version: Version::from_u64(3),
            event_type: "ShipDeparted".to_string(),
            event_version: 1,
            payload: serde_json::json!({"destination": "Alpha Depot"}),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        };

        let json = serde_json::to_value(&response).expect("serialize response");
        assert_eq!(json["event_type"], "ShipDeparted");
        assert_eq!(json["event_version"], 1);
        assert_eq!(json["version"], 3);
    }

    #[test]
    fn debug_command_response_serializes_to_json() {
        let response = DebugCommandResponse {
            command_id: Uuid::new_v4(),
            aggregate_id: AggregateId::new(),
            command_type: "DepartForStation".to_string(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: serde_json::json!({"destination": "Beta Relay"}),
            command_version: 1,
        };

        let json = serde_json::to_value(&response).expect("serialize response");
        assert_eq!(json["command_type"], "DepartForStation");
        assert_eq!(json["command_version"], 1);
        assert!(json["command_id"].is_string());
    }

    // -- DebugEndpointHandler tests -------------------------------------------

    fn make_command(aggregate_id: &AggregateId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            command_type: "TestCommand".into(),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            payload: Bytes::from(
                serde_json::to_vec(&serde_json::json!({"action": "test"})).expect("serialize"),
            ),
            command_version: 1,
        }
    }

    #[tokio::test]
    async fn endpoint_handler_get_events_returns_all_events() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        let events = vec![
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 1 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 2 }).expect("serialize"),
            ),
        ];
        event_store
            .append(&agg_id, Version::initial(), events)
            .expect("append events");

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler.get_events(&agg_id, None).await.expect("get events");

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].event_type, "Incremented");
        assert_eq!(responses[0].version.as_u64(), 1);
        assert_eq!(responses[1].version.as_u64(), 2);
    }

    #[tokio::test]
    async fn endpoint_handler_get_events_with_from_version() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        let events = vec![
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 1 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 2 }).expect("serialize"),
            ),
            make_event(
                &agg_id,
                Version::initial(),
                "Incremented",
                serde_json::to_vec(&IncrementedEvent { amount: 3 }).expect("serialize"),
            ),
        ];
        event_store
            .append(&agg_id, Version::initial(), events)
            .expect("append events");

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler
            .get_events(&agg_id, Some(Version::from_u64(2)))
            .await
            .expect("get events");

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].version.as_u64(), 2);
        assert_eq!(responses[1].version.as_u64(), 3);
    }

    #[tokio::test]
    async fn endpoint_handler_get_commands_returns_all_commands() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        command_store
            .append(make_command(&agg_id))
            .expect("append command");
        command_store
            .append(make_command(&agg_id))
            .expect("append command");

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler.get_commands(&agg_id).await.expect("get commands");

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].command_type, "TestCommand");
        assert_eq!(responses[0].command_version, 1);
        assert_eq!(responses[0].payload["action"], "test");
    }

    #[tokio::test]
    async fn endpoint_handler_get_commands_filters_by_aggregate() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_a = AggregateId::new();
        let agg_b = AggregateId::new();

        command_store
            .append(make_command(&agg_a))
            .expect("append command");
        command_store
            .append(make_command(&agg_b))
            .expect("append command");

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler.get_commands(&agg_a).await.expect("get commands");

        assert_eq!(responses.len(), 1);
    }

    #[tokio::test]
    async fn endpoint_handler_get_aggregate_delegates_to_inspector() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        let events = vec![make_event(
            &agg_id,
            Version::initial(),
            "Incremented",
            serde_json::to_vec(&IncrementedEvent { amount: 42 }).expect("serialize"),
        )];
        event_store
            .append(&agg_id, Version::initial(), events)
            .expect("append events");

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let response = handler
            .get_aggregate::<TestAggregate>(&agg_id)
            .await
            .expect("get aggregate");

        assert_eq!(response.aggregate_id, agg_id);
        assert_eq!(response.version.as_u64(), 1);
        assert_eq!(response.events_replayed, 1);

        let state: TestAggregate =
            serde_json::from_value(response.state).expect("deserialize state");
        assert_eq!(state.counter, 42);
    }

    #[tokio::test]
    async fn endpoint_handler_get_events_empty_aggregate() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler.get_events(&agg_id, None).await.expect("get events");

        assert!(responses.is_empty());
    }

    #[tokio::test]
    async fn endpoint_handler_get_commands_empty_aggregate() {
        let event_store = InMemoryEventStore::new();
        let snapshot_store = InMemorySnapshotStore::new();
        let command_store = InMemoryCommandStore::new();
        let agg_id = AggregateId::new();

        let handler = DebugEndpointHandler::new(event_store, snapshot_store, command_store);
        let responses = handler.get_commands(&agg_id).await.expect("get commands");

        assert!(responses.is_empty());
    }

    #[test]
    fn decode_payload_valid_json() {
        let payload = serde_json::to_vec(&serde_json::json!({"key": "value"})).expect("serialize");
        let decoded = decode_payload(&payload);
        assert_eq!(decoded["key"], "value");
    }

    #[test]
    fn decode_payload_invalid_json_falls_back_to_string() {
        let payload = b"not valid json";
        let decoded = decode_payload(payload);
        assert_eq!(decoded, serde_json::Value::String("not valid json".into()));
    }

    #[test]
    fn resolve_debug_enabled_explicit_true() {
        assert!(resolve_debug_enabled(Some(true)));
    }

    #[test]
    fn resolve_debug_enabled_explicit_false() {
        assert!(!resolve_debug_enabled(Some(false)));
    }

    #[test]
    fn resolve_debug_enabled_none_defaults_to_false() {
        // Remove the env var if it happens to be set.
        // Note: env var mutation is not thread-safe. This test could flake if
        // another parallel test sets CANON_DEBUG_ENDPOINTS. In practice the
        // risk is very low since no other test sets this variable.
        std::env::remove_var("CANON_DEBUG_ENDPOINTS");
        assert!(!resolve_debug_enabled(None));
    }
}
