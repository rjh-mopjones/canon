use std::sync::Arc;

use canon_event_store_cassandra::CassandraEventStore;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use sqlx::PgPool;
use tokio::sync::broadcast;

/// Shared application state, injected into all route handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// Broadcast channel for WebSocket event delivery.
    pub event_tx: broadcast::Sender<String>,

    /// YugabyteDB connection pool for command store, inbox, projections, and admin queries.
    pub yugabyte_pool: PgPool,

    /// Cassandra event store for read-through event history queries.
    pub event_store: Arc<CassandraEventStore>,

    /// YugabyteDB snapshot store for loading snapshot versions.
    pub snapshot_store: Arc<YugabyteSnapshotStore>,
}
