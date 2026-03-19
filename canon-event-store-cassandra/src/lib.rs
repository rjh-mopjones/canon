use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use scylla::frame::value::CqlTimestamp;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::session::Session;
use scylla::transport::session_builder::SessionBuilder;
use uuid::Uuid;

pub use canon_core::{AggregateId, EventEnvelope, Version};
pub use canon_event_store::{EventStore, EventStoreError};
use canon_snapshot_store::{Snapshot, SnapshotStore};

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum CassandraEventStoreError {
    #[error("failed to connect to Cassandra: {0}")]
    Connection(String),

    #[error("query failed: {0}")]
    Query(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("CASSANDRA_NODES environment variable not set")]
    MissingNodes,
}

impl From<CassandraEventStoreError> for EventStoreError {
    fn from(err: CassandraEventStoreError) -> Self {
        EventStoreError::Store(Box::new(err))
    }
}

// ── Store ───────────────────────────────────────────────────────────────────

/// Cassandra-backed implementation of [`EventStore`].
///
/// Uses lightweight transactions (`IF NOT EXISTS` on `version`) for optimistic
/// concurrency. After a confirmed write, if `version % snapshot_every == 0`,
/// delegates a snapshot write to the injected [`SnapshotStore`].
pub struct CassandraEventStore {
    session: Arc<Session>,
    snapshot_every: u64,
    snapshot_store: Arc<dyn SnapshotStore>,
    stmt_append: PreparedStatement,
    stmt_load: PreparedStatement,
    stmt_load_from: PreparedStatement,
}

impl CassandraEventStore {
    /// Connect to Cassandra, prepare statements, and return a ready store.
    ///
    /// `nodes` is a comma-separated list of contact points (e.g. from `CASSANDRA_NODES`).
    pub async fn new(
        nodes: &str,
        snapshot_store: Arc<dyn SnapshotStore>,
        snapshot_every: u64,
    ) -> Result<Self, CassandraEventStoreError> {
        let mut builder = SessionBuilder::new();
        for node in nodes.split(',').map(|s| s.trim()) {
            builder = builder.known_node(node);
        }
        builder = builder.use_keyspace("canon", false);

        let session = builder
            .build()
            .await
            .map_err(|e| CassandraEventStoreError::Connection(e.to_string()))?;
        let session = Arc::new(session);

        let stmt_append = session
            .prepare(
                "INSERT INTO events \
                 (aggregate_id, version, event_id, event_type, event_version, \
                  payload, correlation_id, causation_id, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 IF NOT EXISTS",
            )
            .await
            .map_err(|e| CassandraEventStoreError::Query(e.to_string()))?;

        let stmt_load = session
            .prepare(
                "SELECT aggregate_id, version, event_id, event_type, event_version, \
                 payload, correlation_id, causation_id, created_at \
                 FROM events WHERE aggregate_id = ? ORDER BY version ASC",
            )
            .await
            .map_err(|e| CassandraEventStoreError::Query(e.to_string()))?;

        let stmt_load_from = session
            .prepare(
                "SELECT aggregate_id, version, event_id, event_type, event_version, \
                 payload, correlation_id, causation_id, created_at \
                 FROM events WHERE aggregate_id = ? AND version >= ? ORDER BY version ASC",
            )
            .await
            .map_err(|e| CassandraEventStoreError::Query(e.to_string()))?;

        Ok(Self {
            session,
            snapshot_every,
            snapshot_store,
            stmt_append,
            stmt_load,
            stmt_load_from,
        })
    }

    /// Connect using the `CASSANDRA_NODES` environment variable.
    pub async fn from_env(
        snapshot_store: Arc<dyn SnapshotStore>,
        snapshot_every: u64,
    ) -> Result<Self, CassandraEventStoreError> {
        let nodes =
            std::env::var("CASSANDRA_NODES").map_err(|_| CassandraEventStoreError::MissingNodes)?;
        Self::new(&nodes, snapshot_store, snapshot_every).await
    }

    /// Returns a reference to the underlying Scylla session.
    pub fn session(&self) -> &Session {
        &self.session
    }
}

// ── EventStore impl ─────────────────────────────────────────────────────────

type EventRow = (Uuid, i64, Uuid, String, i32, Vec<u8>, Uuid, Uuid, CqlTimestamp);

#[async_trait]
impl EventStore for CassandraEventStore {
    async fn append(
        &self,
        aggregate_id: &AggregateId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<(), EventStoreError> {
        let mut version = expected_version;

        for event in &events {
            version = version.next();
            let ts = CqlTimestamp(event.timestamp.timestamp_millis());

            let result = self
                .session
                .execute_unpaged(
                    &self.stmt_append,
                    (
                        *aggregate_id.as_uuid(),
                        version.as_u64() as i64,
                        event.event_id,
                        event.event_type.as_str(),
                        event.event_version as i32,
                        event.payload.to_vec(),
                        event.correlation_id,
                        event.causation_id,
                        ts,
                    ),
                )
                .await
                .map_err(|e| -> EventStoreError {
                    CassandraEventStoreError::Query(e.to_string()).into()
                })?;

            // LWT returns a result set with an [applied] column.
            let rows_result = result.into_rows_result().map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;

            let (applied,) = rows_result
                .first_row::<(bool,)>()
                .map_err(|e| -> EventStoreError {
                    CassandraEventStoreError::Deserialization(e.to_string()).into()
                })?;

            if !applied {
                return Err(EventStoreError::VersionConflict {
                    expected: expected_version,
                    found: version,
                });
            }
        }

        // Snapshot trigger — best-effort, does not affect the write success path.
        if self.snapshot_every > 0
            && version.as_u64() > 0
            && version.as_u64().is_multiple_of(self.snapshot_every)
        {
            let snapshot = Snapshot {
                aggregate_id: aggregate_id.clone(),
                version,
                state: Bytes::new(),
                taken_at: Utc::now(),
            };
            if let Err(e) = self.snapshot_store.save(snapshot).await {
                tracing::warn!(
                    aggregate_id = %aggregate_id.as_uuid(),
                    version = version.as_u64(),
                    error = %e,
                    "best-effort snapshot write failed"
                );
            }
        }

        Ok(())
    }

    async fn load(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Vec<EventEnvelope>, EventStoreError> {
        let result = self
            .session
            .execute_unpaged(&self.stmt_load, (*aggregate_id.as_uuid(),))
            .await
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Query(e.to_string()).into()
            })?;

        let rows_result = result.into_rows_result().map_err(|e| -> EventStoreError {
            CassandraEventStoreError::Deserialization(e.to_string()).into()
        })?;

        let rows = rows_result
            .rows::<EventRow>()
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;

        let mut envelopes = Vec::new();
        for row in rows {
            let row = row.map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;
            envelopes.push(row_to_envelope(row));
        }

        Ok(envelopes)
    }

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, EventStoreError> {
        let result = self
            .session
            .execute_unpaged(
                &self.stmt_load_from,
                (*aggregate_id.as_uuid(), from_version.as_u64() as i64),
            )
            .await
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Query(e.to_string()).into()
            })?;

        let rows_result = result.into_rows_result().map_err(|e| -> EventStoreError {
            CassandraEventStoreError::Deserialization(e.to_string()).into()
        })?;

        let rows = rows_result
            .rows::<EventRow>()
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;

        let mut envelopes = Vec::new();
        for row in rows {
            let row = row.map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;
            envelopes.push(row_to_envelope(row));
        }

        Ok(envelopes)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    let secs = millis / 1000;
    let nsecs = ((millis % 1000).unsigned_abs() * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs)
        .single()
        .unwrap_or_else(Utc::now)
}

fn row_to_envelope(row: EventRow) -> EventEnvelope {
    let (aggregate_id, version, event_id, event_type, event_version, payload, correlation_id, causation_id, created_at) = row;
    EventEnvelope {
        event_id,
        aggregate_id: AggregateId::from_uuid(aggregate_id),
        version: Version::from_u64(version as u64),
        event_type,
        event_version: event_version as u32,
        payload: Bytes::from(payload),
        correlation_id,
        causation_id,
        timestamp: millis_to_datetime(created_at.0),
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use canon_snapshot_store::SnapshotStoreError;
    use std::sync::Mutex;

    // ── Spy snapshot store ──────────────────────────────────────────────

    struct SpySnapshotStore {
        saved: Mutex<Vec<Snapshot>>,
    }

    impl SpySnapshotStore {
        fn new() -> Self {
            Self {
                saved: Mutex::new(Vec::new()),
            }
        }

        fn saved_count(&self) -> usize {
            self.saved.lock().map(|v| v.len()).unwrap_or(0)
        }

        fn last_saved(&self) -> Option<Snapshot> {
            self.saved.lock().ok().and_then(|v| v.last().cloned())
        }
    }

    #[async_trait]
    impl SnapshotStore for SpySnapshotStore {
        async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotStoreError> {
            if let Ok(mut saved) = self.saved.lock() {
                saved.push(snapshot);
            }
            Ok(())
        }

        async fn load(
            &self,
            _aggregate_id: &AggregateId,
        ) -> Result<Option<Snapshot>, SnapshotStoreError> {
            Ok(None)
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn cassandra_nodes() -> Option<String> {
        std::env::var("CASSANDRA_NODES").ok()
    }

    async fn ensure_schema(nodes: &str) {
        let mut builder = SessionBuilder::new();
        for node in nodes.split(',').map(|s| s.trim()) {
            builder = builder.known_node(node);
        }
        let session = builder
            .build()
            .await
            .expect("failed to connect to Cassandra");

        session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS canon \
                 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                &[],
            )
            .await
            .expect("create keyspace");

        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS canon.events ( \
                 aggregate_id UUID, version BIGINT, event_id UUID, event_type TEXT, \
                 event_version INT, payload BLOB, correlation_id UUID, causation_id UUID, \
                 created_at TIMESTAMP, \
                 PRIMARY KEY (aggregate_id, version) \
                 ) WITH CLUSTERING ORDER BY (version ASC)",
                &[],
            )
            .await
            .expect("create events table");
    }

    async fn setup_store(
        snapshot_store: Arc<dyn SnapshotStore>,
        snapshot_every: u64,
    ) -> CassandraEventStore {
        let nodes = cassandra_nodes().expect("CASSANDRA_NODES must be set");
        ensure_schema(&nodes).await;
        CassandraEventStore::new(&nodes, snapshot_store, snapshot_every)
            .await
            .expect("failed to create CassandraEventStore")
    }

    fn make_event(aggregate_id: &AggregateId) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            aggregate_id: aggregate_id.clone(),
            version: Version::initial(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: Bytes::from_static(b"{}"),
            correlation_id: Uuid::new_v4(),
            causation_id: Uuid::new_v4(),
            timestamp: Utc::now(),
        }
    }

    fn noop_snapshot_store() -> Arc<dyn SnapshotStore> {
        Arc::new(SpySnapshotStore::new())
    }

    // ── Integration tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_append_and_load() {
        if cassandra_nodes().is_none() {
            eprintln!("skipping: CASSANDRA_NODES not set");
            return;
        }

        let store = setup_store(noop_snapshot_store(), 0).await;
        let agg_id = AggregateId::new();

        let events = vec![make_event(&agg_id), make_event(&agg_id)];
        store
            .append(&agg_id, events, Version::initial())
            .await
            .expect("append should succeed");

        let loaded = store.load(&agg_id).await.expect("load should succeed");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 1);
        assert_eq!(loaded[1].version.as_u64(), 2);
        assert_eq!(loaded[0].event_type, "TestEvent");
    }

    #[tokio::test]
    async fn test_optimistic_concurrency_conflict() {
        if cassandra_nodes().is_none() {
            eprintln!("skipping: CASSANDRA_NODES not set");
            return;
        }

        let store = setup_store(noop_snapshot_store(), 0).await;
        let agg_id = AggregateId::new();

        store
            .append(&agg_id, vec![make_event(&agg_id)], Version::initial())
            .await
            .expect("first append should succeed");

        // Second append at same expected_version must fail
        let result = store
            .append(&agg_id, vec![make_event(&agg_id)], Version::initial())
            .await;

        assert!(
            matches!(result, Err(EventStoreError::VersionConflict { .. })),
            "expected VersionConflict, got: {result:?}",
        );
    }

    #[tokio::test]
    async fn test_load_from_version() {
        if cassandra_nodes().is_none() {
            eprintln!("skipping: CASSANDRA_NODES not set");
            return;
        }

        let store = setup_store(noop_snapshot_store(), 0).await;
        let agg_id = AggregateId::new();

        let events = vec![
            make_event(&agg_id),
            make_event(&agg_id),
            make_event(&agg_id),
        ];
        store
            .append(&agg_id, events, Version::initial())
            .await
            .expect("append should succeed");

        // Load from version 2 — should return events at version 2 and 3
        let loaded = store
            .load_from_version(&agg_id, Version::from_u64(2))
            .await
            .expect("load_from_version should succeed");

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].version.as_u64(), 2);
        assert_eq!(loaded[1].version.as_u64(), 3);
    }

    #[tokio::test]
    async fn test_snapshot_written_at_interval() {
        if cassandra_nodes().is_none() {
            eprintln!("skipping: CASSANDRA_NODES not set");
            return;
        }

        let spy = Arc::new(SpySnapshotStore::new());
        let store = setup_store(spy.clone() as Arc<dyn SnapshotStore>, 50).await;
        let agg_id = AggregateId::new();

        // Append 50 events in one batch
        let events: Vec<_> = (0..50).map(|_| make_event(&agg_id)).collect();
        store
            .append(&agg_id, events, Version::initial())
            .await
            .expect("append should succeed");

        // snapshot_every = 50, so version 50 triggers exactly one snapshot write
        assert_eq!(spy.saved_count(), 1, "expected exactly one snapshot write");
        let snap = spy.last_saved().expect("snapshot should exist");
        assert_eq!(snap.aggregate_id, agg_id);
        assert_eq!(snap.version.as_u64(), 50);
    }
}
