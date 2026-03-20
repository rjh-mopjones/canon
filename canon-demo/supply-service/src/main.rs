use tracing::{error, info};

use canon_core::{
    EventPayloadSnapshotProvider, InMemoryDeadLetterStore, InMemoryEventStore,
    InMemoryOutboundQueue, InMemoryOutboxPublisher, InMemoryOutboxStore, InMemoryProjectionStore,
    InMemoryPublisher, InMemoryRetryTracker, InMemorySnapshotStore, ServiceBuilder,
};
use supply_service::aggregate::Inventory;

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("failed to connect to YugabyteDB: {0}")]
    YugabyteConnection(#[from] sqlx::Error),

    #[error("failed to connect to Cassandra: {0}")]
    CassandraConnection(String),

    #[error("service builder error: {0}")]
    ServiceBuilder(#[from] canon_core::ServiceBuilderError),
}

fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // ── Environment variables ─────────────────────────────────────────────
    let yugabyte_url = env_or_default(
        "YUGABYTE_URL",
        "postgres://canon:canon@localhost:5433/canon",
    );
    let cassandra_nodes = env_or_default("CASSANDRA_NODES", "localhost:9042");
    let kafka_brokers = env_or_default("KAFKA_BROKERS", "localhost:9092");

    // ── Infrastructure connections ────────────────────────────────────────
    info!("connecting to YugabyteDB at {yugabyte_url}");
    let _yugabyte_pool = sqlx::PgPool::connect(&yugabyte_url).await?;
    info!("YugabyteDB connected");

    info!("connecting to Cassandra at {cassandra_nodes}");
    let _event_store = canon_event_store_cassandra::CassandraEventStore::new(&cassandra_nodes)
        .await
        .map_err(|e| StartupError::CassandraConnection(e.to_string()))?;
    info!("Cassandra connected");

    info!(brokers = %kafka_brokers, "Kafka brokers configured");

    // ── ServiceBuilder ────────────────────────────────────────────────────
    let service = ServiceBuilder::new("supply")
        .for_aggregate::<Inventory>()
        .event_store(InMemoryEventStore::new())
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(InMemoryOutboxStore::new())
        .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .topic("canon.supply.events")
        .build()?;

    info!(service = service.service_name(), "supply-service ready");

    // Wait for shutdown signal.
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "failed to listen for ctrl-c");
    }

    info!("supply-service shutting down");
    Ok(())
}
