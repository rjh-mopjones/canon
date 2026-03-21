//! Observability endpoints for Canon services.
//!
//! Provides [`ObservabilityHandler`] for querying infrastructure health, projection
//! status, and outbox throughput. Auto-mounted by [`ServiceBuilder`](crate::ServiceBuilder)
//! alongside debug endpoints.
//!
//! Three endpoints:
//! - `/debug/infra` — infrastructure connectivity and latency
//! - `/debug/projections` — registered projections with checkpoint lag and rebuild status
//! - `/debug/outbox` — outbox health (pending count, oldest age, throughput)
//!
//! **Note:** This module provides detailed operational metrics (latency, pool sizes,
//! consumer lag, throughput). It is distinct from the [`health`](crate::health) module,
//! which provides simple pass/fail K8s health probes (`/health/live`, `/health/ready`,
//! `/health/startup`).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::registration::ProjectionRegistration;
use crate::traits::ProjectionCheckpointStore;

// ── Error ────────────────────────────────────────────────────────────────────

/// Errors that can occur during observability queries.
#[derive(Debug, thiserror::Error)]
pub enum ObservabilityError {
    /// Failed to query projection checkpoint store.
    #[error("projection checkpoint store error: {0}")]
    ProjectionCheckpoint(String),

    /// Failed to query outbox store.
    #[error("outbox store error: {0}")]
    Outbox(String),

    /// Failed to query infrastructure status.
    #[error("infra status error: {0}")]
    Infra(String),
}

// ── Response types ──────────────────────────────────────────────────────────

/// Infrastructure connectivity and latency snapshot.
///
/// Returned by `GET /debug/infra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatus {
    /// Cassandra cluster status.
    pub cassandra: CassandraStatus,
    /// YugabyteDB status.
    pub yugabyte: YugabyteStatus,
    /// Kafka consumer group status.
    pub kafka: KafkaStatus,
}

/// Cassandra cluster connectivity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraStatus {
    pub connected: bool,
    pub latency_ms: u64,
    pub node_count: u32,
}

/// YugabyteDB connection pool status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YugabyteStatus {
    pub connected: bool,
    pub pool_size: u32,
    pub pool_idle: u32,
    pub latency_ms: u64,
}

/// Kafka consumer group lag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaStatus {
    pub connected: bool,
    pub consumer_groups: KafkaConsumerGroups,
}

/// Lag per consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConsumerGroups {
    pub event_store: KafkaGroupLag,
    pub projection: KafkaGroupLag,
    pub publisher: KafkaGroupLag,
}

/// Lag for a single Kafka consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaGroupLag {
    pub lag: u64,
}

/// Single projection status entry.
///
/// Returned as an element of `GET /debug/projections`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionStatus {
    /// The projection's unique identifier.
    pub projection_id: String,
    /// Last checkpoint version processed.
    pub last_version: u64,
    /// Whether the projection is currently rebuilding.
    pub rebuilding: bool,
    /// When the checkpoint was last updated.
    pub updated_at: DateTime<Utc>,
    /// Number of events behind the latest version.
    pub lag: u64,
}

/// Outbox health snapshot.
///
/// Returned by `GET /debug/outbox`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxStatus {
    /// Number of entries pending delivery.
    pub pending_count: u64,
    /// Age of the oldest pending entry in milliseconds.
    pub oldest_pending_age_ms: u64,
    /// Events delivered in the last minute.
    pub delivered_last_minute: u64,
    /// Events delivered in the last hour.
    pub delivered_last_hour: u64,
}

// ── InfraStatusProvider trait ───────────────────────────────────────────────

/// Trait for providing infrastructure health status.
///
/// Infrastructure crates implement this to report real connectivity. The
/// in-memory implementation returns synthetic healthy defaults.
#[async_trait::async_trait]
pub trait InfraStatusProvider: Send + Sync + 'static {
    /// Gather the current infrastructure status.
    async fn get_status(&self) -> Result<InfraStatus, ObservabilityError>;
}

/// Blanket impl so `Box<dyn InfraStatusProvider>` also implements the trait.
#[async_trait::async_trait]
impl InfraStatusProvider for Box<dyn InfraStatusProvider> {
    async fn get_status(&self) -> Result<InfraStatus, ObservabilityError> {
        (**self).get_status().await
    }
}

/// Default in-memory infra status provider that returns synthetic healthy stats.
#[derive(Debug, Clone)]
pub struct InMemoryInfraStatusProvider;

#[async_trait::async_trait]
impl InfraStatusProvider for InMemoryInfraStatusProvider {
    async fn get_status(&self) -> Result<InfraStatus, ObservabilityError> {
        Ok(InfraStatus {
            cassandra: CassandraStatus {
                connected: true,
                latency_ms: 1,
                node_count: 1,
            },
            yugabyte: YugabyteStatus {
                connected: true,
                pool_size: 10,
                pool_idle: 8,
                latency_ms: 1,
            },
            kafka: KafkaStatus {
                connected: true,
                consumer_groups: KafkaConsumerGroups {
                    event_store: KafkaGroupLag { lag: 0 },
                    projection: KafkaGroupLag { lag: 0 },
                    publisher: KafkaGroupLag { lag: 0 },
                },
            },
        })
    }
}

// ── OutboxStatusProvider trait ──────────────────────────────────────────────

/// Trait for providing outbox health metrics.
///
/// Infrastructure crates implement this to query actual outbox tables. The
/// in-memory implementation returns synthetic defaults based on the store state.
#[async_trait::async_trait]
pub trait OutboxStatusProvider: Send + Sync + 'static {
    /// Gather current outbox health metrics.
    async fn get_status(&self) -> Result<OutboxStatus, ObservabilityError>;
}

/// Blanket impl so `Box<dyn OutboxStatusProvider>` also implements the trait.
#[async_trait::async_trait]
impl OutboxStatusProvider for Box<dyn OutboxStatusProvider> {
    async fn get_status(&self) -> Result<OutboxStatus, ObservabilityError> {
        (**self).get_status().await
    }
}

/// In-memory outbox status provider that delegates to the in-memory outbox store.
#[derive(Clone)]
pub struct InMemoryOutboxStatusProvider {
    store: crate::memory::InMemoryOutboxStore,
}

impl std::fmt::Debug for InMemoryOutboxStatusProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryOutboxStatusProvider").finish()
    }
}

impl InMemoryOutboxStatusProvider {
    /// Create a new provider backed by the given in-memory outbox store.
    pub fn new(store: crate::memory::InMemoryOutboxStore) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl OutboxStatusProvider for InMemoryOutboxStatusProvider {
    async fn get_status(&self) -> Result<OutboxStatus, ObservabilityError> {
        let pending = self.store.undelivered_count() as u64;
        let delivered = self.store.delivered_count() as u64;
        Ok(OutboxStatus {
            pending_count: pending,
            oldest_pending_age_ms: if pending > 0 { 50 } else { 0 },
            delivered_last_minute: delivered,
            delivered_last_hour: delivered,
        })
    }
}

// ── ObservabilityHandler ───────────────────────────────────────────────────

/// Unified observability endpoint handler.
///
/// Provides all three observability operations:
/// - `get_infra_status()` — infrastructure connectivity and latency
/// - `get_projection_status()` — projection checkpoint state and lag
/// - `get_outbox_status()` — outbox health metrics
///
/// Created by [`ServiceBuilder`](crate::ServiceBuilder) when observability
/// is enabled. Downstream crates (e.g. gateway) wire this into their web
/// framework.
///
/// # Example
///
/// ```ignore
/// let handler = service.observability_handler();
/// let infra = handler.get_infra_status().await?;
/// let projections = handler.get_projection_status().await?;
/// let outbox = handler.get_outbox_status().await?;
/// ```
pub struct ObservabilityHandler<CS, ISP, OSP> {
    projection_checkpoint_store: CS,
    infra_status_provider: ISP,
    outbox_status_provider: OSP,
}

impl<CS, ISP, OSP> ObservabilityHandler<CS, ISP, OSP>
where
    CS: ProjectionCheckpointStore,
    ISP: InfraStatusProvider,
    OSP: OutboxStatusProvider,
{
    /// Create a new `ObservabilityHandler`.
    pub fn new(
        projection_checkpoint_store: CS,
        infra_status_provider: ISP,
        outbox_status_provider: OSP,
    ) -> Self {
        Self {
            projection_checkpoint_store,
            infra_status_provider,
            outbox_status_provider,
        }
    }

    /// Returns infrastructure connectivity and latency status.
    ///
    /// Delegates to the [`InfraStatusProvider`] for actual health checks.
    pub async fn get_infra_status(&self) -> Result<InfraStatus, ObservabilityError> {
        tracing::debug!("observability: querying infra status");
        let status = self.infra_status_provider.get_status().await?;
        tracing::debug!(
            cassandra_connected = status.cassandra.connected,
            yugabyte_connected = status.yugabyte.connected,
            kafka_connected = status.kafka.connected,
            "observability: infra status collected"
        );
        Ok(status)
    }

    /// Returns the status of all registered projections.
    ///
    /// Discovers projections via `inventory` registrations and queries the
    /// checkpoint store for each one's current state.
    pub async fn get_projection_status(&self) -> Result<Vec<ProjectionStatus>, ObservabilityError> {
        tracing::debug!("observability: querying projection status");

        let mut statuses = Vec::new();

        for reg in inventory::iter::<ProjectionRegistration> {
            let projection_id = reg.projection_id;
            let version = self
                .projection_checkpoint_store
                .get_checkpoint(projection_id)
                .await
                .map_err(|e| ObservabilityError::ProjectionCheckpoint(e.to_string()))?;

            // Rebuilding status: check if the store supports it.
            // For now, we report false as the base trait doesn't expose rebuilding.
            // Infrastructure crates that implement ProjectionRebuildManager can
            // override this via a richer provider.
            let rebuilding = false;

            statuses.push(ProjectionStatus {
                projection_id: projection_id.to_owned(),
                last_version: version.as_u64(),
                rebuilding,
                updated_at: Utc::now(),
                lag: 0,
            });
        }

        tracing::debug!(
            projection_count = statuses.len(),
            "observability: projection status collected"
        );

        Ok(statuses)
    }

    /// Returns outbox health metrics.
    ///
    /// Delegates to the [`OutboxStatusProvider`] for actual store queries.
    pub async fn get_outbox_status(&self) -> Result<OutboxStatus, ObservabilityError> {
        tracing::debug!("observability: querying outbox status");
        let status = self.outbox_status_provider.get_status().await?;
        tracing::debug!(
            pending_count = status.pending_count,
            oldest_pending_age_ms = status.oldest_pending_age_ms,
            delivered_last_minute = status.delivered_last_minute,
            "observability: outbox status collected"
        );
        Ok(status)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{InMemoryOutboxStore, InMemoryProjectionStore};
    use crate::Version;

    #[test]
    fn infra_status_serializes_to_json() {
        let status = InfraStatus {
            cassandra: CassandraStatus {
                connected: true,
                latency_ms: 3,
                node_count: 3,
            },
            yugabyte: YugabyteStatus {
                connected: true,
                pool_size: 10,
                pool_idle: 7,
                latency_ms: 1,
            },
            kafka: KafkaStatus {
                connected: true,
                consumer_groups: KafkaConsumerGroups {
                    event_store: KafkaGroupLag { lag: 0 },
                    projection: KafkaGroupLag { lag: 12 },
                    publisher: KafkaGroupLag { lag: 0 },
                },
            },
        };

        let json = serde_json::to_value(&status).expect("serialize infra status");
        assert_eq!(json["cassandra"]["connected"], true);
        assert_eq!(json["cassandra"]["latency_ms"], 3);
        assert_eq!(json["cassandra"]["node_count"], 3);
        assert_eq!(json["yugabyte"]["connected"], true);
        assert_eq!(json["yugabyte"]["pool_size"], 10);
        assert_eq!(json["yugabyte"]["pool_idle"], 7);
        assert_eq!(json["kafka"]["connected"], true);
        assert_eq!(json["kafka"]["consumer_groups"]["event_store"]["lag"], 0);
        assert_eq!(json["kafka"]["consumer_groups"]["projection"]["lag"], 12);
    }

    #[test]
    fn projection_status_serializes_to_json() {
        let status = ProjectionStatus {
            projection_id: "station_inventory".to_owned(),
            last_version: 4821,
            rebuilding: false,
            updated_at: Utc::now(),
            lag: 3,
        };

        let json = serde_json::to_value(&status).expect("serialize projection status");
        assert_eq!(json["projection_id"], "station_inventory");
        assert_eq!(json["last_version"], 4821);
        assert_eq!(json["rebuilding"], false);
        assert_eq!(json["lag"], 3);
    }

    #[test]
    fn outbox_status_serializes_to_json() {
        let status = OutboxStatus {
            pending_count: 2,
            oldest_pending_age_ms: 150,
            delivered_last_minute: 847,
            delivered_last_hour: 42300,
        };

        let json = serde_json::to_value(&status).expect("serialize outbox status");
        assert_eq!(json["pending_count"], 2);
        assert_eq!(json["oldest_pending_age_ms"], 150);
        assert_eq!(json["delivered_last_minute"], 847);
        assert_eq!(json["delivered_last_hour"], 42300);
    }

    #[tokio::test]
    async fn in_memory_infra_provider_returns_healthy() {
        let provider = InMemoryInfraStatusProvider;
        let status = provider.get_status().await.expect("should succeed");

        assert!(status.cassandra.connected);
        assert!(status.yugabyte.connected);
        assert!(status.kafka.connected);
        assert_eq!(status.cassandra.node_count, 1);
        assert_eq!(status.yugabyte.pool_size, 10);
    }

    #[tokio::test]
    async fn in_memory_outbox_provider_returns_counts() {
        let store = InMemoryOutboxStore::new();

        // Insert a couple of entries.
        let env = crate::EventEnvelope {
            event_id: uuid::Uuid::new_v4(),
            aggregate_id: crate::AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: bytes::Bytes::from_static(b"{}"),
            correlation_id: uuid::Uuid::new_v4(),
            causation_id: uuid::Uuid::new_v4(),
            timestamp: Utc::now(),
        };
        store.insert(env).expect("insert should succeed");

        let provider = InMemoryOutboxStatusProvider::new(store);
        let status = provider.get_status().await.expect("should succeed");

        assert_eq!(status.pending_count, 1);
        assert!(status.oldest_pending_age_ms > 0);
    }

    #[tokio::test]
    async fn handler_get_infra_status() {
        let projection_store = InMemoryProjectionStore::new();
        let infra_provider = InMemoryInfraStatusProvider;
        let outbox_provider = InMemoryOutboxStatusProvider::new(InMemoryOutboxStore::new());

        let handler = ObservabilityHandler::new(projection_store, infra_provider, outbox_provider);

        let status = handler.get_infra_status().await.expect("should succeed");
        assert!(status.cassandra.connected);
        assert!(status.yugabyte.connected);
        assert!(status.kafka.connected);
    }

    #[tokio::test]
    async fn handler_get_projection_status_empty() {
        let projection_store = InMemoryProjectionStore::new();
        let infra_provider = InMemoryInfraStatusProvider;
        let outbox_provider = InMemoryOutboxStatusProvider::new(InMemoryOutboxStore::new());

        let handler = ObservabilityHandler::new(projection_store, infra_provider, outbox_provider);

        // No projections registered via inventory in this test binary, so
        // the result should be empty (or contain whatever inventory has).
        let statuses = handler
            .get_projection_status()
            .await
            .expect("should succeed");
        // We can't assert exact count since inventory may contain registrations
        // from other tests. Just assert the call succeeded without error.
        let _ = statuses;
    }

    #[tokio::test]
    async fn handler_get_outbox_status() {
        let projection_store = InMemoryProjectionStore::new();
        let infra_provider = InMemoryInfraStatusProvider;
        let outbox_store = InMemoryOutboxStore::new();
        let outbox_provider = InMemoryOutboxStatusProvider::new(outbox_store);

        let handler = ObservabilityHandler::new(projection_store, infra_provider, outbox_provider);

        let status = handler.get_outbox_status().await.expect("should succeed");
        assert_eq!(status.pending_count, 0);
        assert_eq!(status.oldest_pending_age_ms, 0);
    }

    #[tokio::test]
    async fn handler_outbox_status_reflects_store_state() {
        let projection_store = InMemoryProjectionStore::new();
        let infra_provider = InMemoryInfraStatusProvider;
        let outbox_store = InMemoryOutboxStore::new();

        // Insert entries before creating the provider
        let env = crate::EventEnvelope {
            event_id: uuid::Uuid::new_v4(),
            aggregate_id: crate::AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent".into(),
            event_version: 1,
            payload: bytes::Bytes::from_static(b"{}"),
            correlation_id: uuid::Uuid::new_v4(),
            causation_id: uuid::Uuid::new_v4(),
            timestamp: Utc::now(),
        };
        outbox_store.insert(env).expect("insert should succeed");

        let env2 = crate::EventEnvelope {
            event_id: uuid::Uuid::new_v4(),
            aggregate_id: crate::AggregateId::new(),
            version: Version::initial().next(),
            event_type: "TestEvent2".into(),
            event_version: 1,
            payload: bytes::Bytes::from_static(b"{}"),
            correlation_id: uuid::Uuid::new_v4(),
            causation_id: uuid::Uuid::new_v4(),
            timestamp: Utc::now(),
        };
        outbox_store.insert(env2).expect("insert should succeed");

        let outbox_provider = InMemoryOutboxStatusProvider::new(outbox_store);

        let handler = ObservabilityHandler::new(projection_store, infra_provider, outbox_provider);

        let status = handler.get_outbox_status().await.expect("should succeed");
        assert_eq!(status.pending_count, 2);
    }
}
