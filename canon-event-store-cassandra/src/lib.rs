use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use scylla::frame::value::CqlTimestamp;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::session::Session;
use scylla::transport::session_builder::SessionBuilder;
use uuid::Uuid;

use canon_core::traits::EventStore;
pub use canon_core::{AggregateId, EventEnvelope, Version};
pub use canon_event_store::EventStoreError;

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
/// concurrency.
pub struct CassandraEventStore {
    session: Arc<Session>,
    stmt_append: PreparedStatement,
    stmt_load: PreparedStatement,
    stmt_load_from: PreparedStatement,
    stmt_current_version: PreparedStatement,
}

impl CassandraEventStore {
    /// Connect to Cassandra, prepare statements, and return a ready store.
    ///
    /// `nodes` is a comma-separated list of contact points (e.g. from `CASSANDRA_NODES`).
    /// Uses the default `canon` keyspace. For per-service isolation, use
    /// [`new_with_keyspace`](Self::new_with_keyspace) instead.
    pub async fn new(nodes: &str) -> Result<Self, CassandraEventStoreError> {
        Self::new_with_keyspace(nodes, "canon").await
    }

    /// Connect to Cassandra using a specific keyspace.
    ///
    /// Each service should use its own keyspace (e.g. `canon_fleet`, `canon_cargo`)
    /// to ensure domain isolation — events from different services never share a
    /// Cassandra table.
    pub async fn new_with_keyspace(
        nodes: &str,
        keyspace: &str,
    ) -> Result<Self, CassandraEventStoreError> {
        let mut builder = SessionBuilder::new();
        for node in nodes.split(',').map(|s| s.trim()) {
            builder = builder.known_node(node);
        }
        builder = builder.use_keyspace(keyspace, false);

        let session = builder
            .build()
            .await
            .map_err(|e| CassandraEventStoreError::Connection(e.to_string()))?;

        // Validate keyspace exists by querying system schema. This catches
        // configuration errors early (e.g. typo in keyspace name, missing
        // init-schema job) instead of failing on the first real query.
        let ks_check = session
            .query_unpaged(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                (keyspace,),
            )
            .await
            .map_err(|e| {
                CassandraEventStoreError::Connection(format!(
                    "keyspace '{}' validation query failed: {}",
                    keyspace, e
                ))
            })?;
        let ks_rows = ks_check.into_rows_result().map_err(|e| {
            CassandraEventStoreError::Connection(format!(
                "keyspace '{}' validation deserialization failed: {}",
                keyspace, e
            ))
        })?;
        if ks_rows
            .rows::<(String,)>()
            .map_err(|e| {
                CassandraEventStoreError::Connection(format!(
                    "keyspace '{}' validation row parse failed: {}",
                    keyspace, e
                ))
            })?
            .next()
            .is_none()
        {
            return Err(CassandraEventStoreError::Connection(format!(
                "keyspace '{}' does not exist — run the init-schema job first",
                keyspace,
            )));
        }

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

        let stmt_current_version = session
            .prepare(
                "SELECT version FROM events \
                 WHERE aggregate_id = ? ORDER BY version DESC LIMIT 1",
            )
            .await
            .map_err(|e| CassandraEventStoreError::Query(e.to_string()))?;

        Ok(Self {
            session,
            stmt_append,
            stmt_load,
            stmt_load_from,
            stmt_current_version,
        })
    }

    /// Connect using the `CASSANDRA_NODES` environment variable.
    ///
    /// Reads `CASSANDRA_KEYSPACE` to determine the keyspace (defaults to `"canon"`
    /// when not set). For per-service isolation, set the env var to a
    /// service-specific keyspace like `canon_fleet`.
    pub async fn from_env() -> Result<Self, CassandraEventStoreError> {
        let nodes =
            std::env::var("CASSANDRA_NODES").map_err(|_| CassandraEventStoreError::MissingNodes)?;
        let keyspace = std::env::var("CASSANDRA_KEYSPACE").unwrap_or_else(|_| "canon".to_owned());
        Self::new_with_keyspace(&nodes, &keyspace).await
    }

    /// Returns a reference to the underlying Scylla session.
    pub fn session(&self) -> &Session {
        &self.session
    }
}

// ── EventStore impl ─────────────────────────────────────────────────────────

type EventRow = (
    Uuid,
    i64,
    Uuid,
    String,
    i32,
    Vec<u8>,
    Uuid,
    Uuid,
    CqlTimestamp,
);

#[async_trait]
impl EventStore for CassandraEventStore {
    type Error = EventStoreError;

    async fn append(
        &self,
        aggregate_id: &AggregateId,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) -> Result<(), Self::Error> {
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

            // LWT result: ScyllaDB always returns all columns (10 cols) with
            // [applied] first. Cassandra may return 1 col on success or 10 on
            // conflict. Deserialize as a wide tuple that covers both drivers.
            // On success the non-key columns are NULL (wrapped in Option).
            // Column order from ScyllaDB LWT response:
            //   [applied], aggregate_id, version, event_id,
            //   correlation_id, created_at, causation_id,
            //   event_type, event_version, payload
            type LwtRow = (
                bool,                 // [applied]
                Option<Uuid>,         // aggregate_id (UUID)
                Option<i64>,          // version (BigInt)
                Option<Uuid>,         // event_id (UUID)
                Option<Uuid>,         // correlation_id (UUID)
                Option<CqlTimestamp>, // created_at (Timestamp)
                Option<Uuid>,         // causation_id (UUID)
                Option<String>,       // event_type (Text)
                Option<i32>,          // event_version (Int)
                Option<Vec<u8>>,      // payload (Blob)
            );

            let rows_result = result.into_rows_result().map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;

            let col_count = rows_result.column_specs().len();

            let applied = if col_count == 1 {
                // Pure Cassandra: success returns only [applied]=true
                let (val,) =
                    rows_result
                        .first_row::<(bool,)>()
                        .map_err(|e| -> EventStoreError {
                            CassandraEventStoreError::Deserialization(e.to_string()).into()
                        })?;
                val
            } else {
                // ScyllaDB (or Cassandra conflict): all columns present
                let (val, ..) =
                    rows_result
                        .first_row::<LwtRow>()
                        .map_err(|e| -> EventStoreError {
                            CassandraEventStoreError::Deserialization(e.to_string()).into()
                        })?;
                val
            };
            if !applied {
                return Err(EventStoreError::VersionConflict {
                    expected: expected_version,
                    found: version,
                });
            }
        }

        Ok(())
    }

    async fn load(&self, aggregate_id: &AggregateId) -> Result<Vec<EventEnvelope>, Self::Error> {
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
            envelopes.push(row_to_envelope(row)?);
        }

        Ok(envelopes)
    }

    async fn load_from_version(
        &self,
        aggregate_id: &AggregateId,
        from_version: Version,
    ) -> Result<Vec<EventEnvelope>, Self::Error> {
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
            envelopes.push(row_to_envelope(row)?);
        }

        Ok(envelopes)
    }

    async fn current_version(&self, aggregate_id: &AggregateId) -> Result<Version, Self::Error> {
        let result = self
            .session
            .execute_unpaged(&self.stmt_current_version, (*aggregate_id.as_uuid(),))
            .await
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Query(e.to_string()).into()
            })?;

        let rows_result = result.into_rows_result().map_err(|e| -> EventStoreError {
            CassandraEventStoreError::Deserialization(e.to_string()).into()
        })?;

        let rows = rows_result
            .rows::<(i64,)>()
            .map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;

        let mut version = Version::initial();
        for row in rows {
            let (v,) = row.map_err(|e| -> EventStoreError {
                CassandraEventStoreError::Deserialization(e.to_string()).into()
            })?;
            version = Version::from_u64(v as u64);
        }

        Ok(version)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn millis_to_datetime(millis: i64) -> Result<DateTime<Utc>, CassandraEventStoreError> {
    let secs = millis / 1000;
    let nsecs = ((millis % 1000).unsigned_abs() * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single().ok_or_else(|| {
        CassandraEventStoreError::Deserialization(format!("invalid timestamp millis: {millis}"))
    })
}

fn row_to_envelope(row: EventRow) -> Result<EventEnvelope, CassandraEventStoreError> {
    let (
        aggregate_id,
        version,
        event_id,
        event_type,
        event_version,
        payload,
        correlation_id,
        causation_id,
        created_at,
    ) = row;
    Ok(EventEnvelope {
        event_id,
        aggregate_id: AggregateId::from_uuid(aggregate_id),
        version: Version::from_u64(version as u64),
        event_type,
        event_version: event_version as u32,
        payload: Bytes::from(payload),
        correlation_id,
        causation_id,
        timestamp: millis_to_datetime(created_at.0)?,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────
//
// Integration tests for CassandraEventStore live in canon-test/tests/tier2_e2e.rs
// alongside other testcontainers-based tests. This avoids starting a second
// ScyllaDB container (statics don't run destructors, so a container held in a
// static OnceCell leaks and competes for resources with the tier2 container on
// CI). All four tests (append_and_load, optimistic_concurrency_conflict,
// load_from_version, lwt_column_order_on_conflict) have been migrated there.
