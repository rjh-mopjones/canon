use std::collections::HashMap;
use std::sync::Arc;

use canon_event_store_cassandra::CassandraEventStore;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use sqlx::PgPool;
use tokio::sync::broadcast;

/// Per-service YugabyteDB pools and Cassandra event stores.
///
/// Each demo service has its own YugabyteDB schema (e.g. `canon_fleet`,
/// `canon_station`) and Cassandra keyspace (e.g. `canon_fleet`, `canon_cargo`).
/// The gateway maintains a pool and event store per service so that commands
/// are written to the correct service's inbox and events are read from the
/// correct keyspace.
#[derive(Clone)]
pub struct ServiceStores {
    /// YugabyteDB pool with search_path set to the service's schema.
    pub pool: PgPool,

    /// Cassandra event store connected to the service's keyspace.
    pub event_store: Arc<CassandraEventStore>,

    /// YugabyteDB snapshot store (uses the same service pool).
    pub snapshot_store: Arc<YugabyteSnapshotStore>,
}

/// Shared application state, injected into all route handlers via axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// Broadcast channel for WebSocket event delivery.
    pub event_tx: broadcast::Sender<String>,

    /// Per-service stores keyed by service name (e.g. "fleet", "cargo", "station").
    pub service_stores: HashMap<String, ServiceStores>,

    /// Fleet-specific pool used as the default for fleet commands/queries.
    /// This is a convenience alias for `service_stores["fleet"].pool`.
    pub yugabyte_pool: PgPool,

    /// Fleet-specific Cassandra event store for backwards-compatible read paths.
    /// Routes that know their target service should use `service_stores` instead.
    pub event_store: Arc<CassandraEventStore>,

    /// Fleet-specific snapshot store (backwards compat convenience).
    pub snapshot_store: Arc<YugabyteSnapshotStore>,
}

impl AppState {
    /// Get the YugabyteDB pool for a specific service.
    ///
    /// Falls back to the fleet pool if the service is not found (should not
    /// happen in practice since all 5 services are registered at startup).
    pub fn pool_for_service(&self, service: &str) -> &PgPool {
        self.service_stores
            .get(service)
            .map(|s| &s.pool)
            .unwrap_or(&self.yugabyte_pool)
    }

    /// Get the Cassandra event store for a specific service.
    pub fn event_store_for_service(&self, service: &str) -> &Arc<CassandraEventStore> {
        self.service_stores
            .get(service)
            .map(|s| &s.event_store)
            .unwrap_or(&self.event_store)
    }

    /// Get the snapshot store for a specific service.
    pub fn snapshot_store_for_service(&self, service: &str) -> &Arc<YugabyteSnapshotStore> {
        self.service_stores
            .get(service)
            .map(|s| &s.snapshot_store)
            .unwrap_or(&self.snapshot_store)
    }
}
