//! Debug inspection utilities for Canon aggregates.
//!
//! Provides [`DebugInspector`] for hydrating any aggregate from its event and
//! snapshot stores, plus JSON-serializable response types that downstream
//! crates (e.g. gateway) can serve on `/debug/*` endpoints.

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::traits::{EventStore, SnapshotStore};
use crate::{Aggregate, AggregateId, Version};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{InMemoryEventStore, InMemorySnapshotStore};
    use crate::{EventEnvelope, MacroError, Snapshot, Version};
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
}
