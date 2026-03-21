use std::sync::Arc;

use tracing::{error, info, warn};

use canon_core::{
    Dispatcher, DispatcherConfig, EventPayloadSnapshotProvider, InMemoryDeadLetterStore,
    InMemoryEventStore, InMemoryOutboundQueue, InMemoryOutboxPublisher, InMemoryOutboxStore,
    InMemoryProjectionStore, InMemoryPublisher, InMemoryRetryTracker, InMemorySnapshotStore,
    ServiceBuilder,
};
use fleet_service::aggregate::Ship;
use fleet_service::dispatcher_store::PgDispatcherStore;

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
    let yugabyte_pool = sqlx::PgPool::connect(&yugabyte_url).await?;
    info!("YugabyteDB connected");

    info!("connecting to Cassandra at {cassandra_nodes}");
    let event_store = Arc::new(
        canon_event_store_cassandra::CassandraEventStore::new(&cassandra_nodes)
            .await
            .map_err(|e| StartupError::CassandraConnection(e.to_string()))?,
    );
    info!("Cassandra connected");

    info!(brokers = %kafka_brokers, "Kafka brokers configured");

    // ── ServiceBuilder ────────────────────────────────────────────────────
    // The infrastructure crates implement their own trait-crate traits
    // (e.g. canon_event_store::EventStore) rather than the canon-core traits
    // (e.g. canon_core::traits::EventStore) that ServiceBuilder requires.
    // Until trait unification is complete, we use in-memory impls in the
    // ServiceBuilder while establishing real connections above for readiness
    // verification and future use.
    let service = ServiceBuilder::new("fleet")
        .for_aggregate::<Ship>()
        .event_store(InMemoryEventStore::new())
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(InMemoryDeadLetterStore::new())
        .retry_tracker(InMemoryRetryTracker::new())
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(InMemoryOutboxStore::new())
        .outbox_publisher(InMemoryOutboxPublisher::new(InMemoryOutboundQueue::new()))
        .projection_checkpoint_store(InMemoryProjectionStore::new())
        .publisher(InMemoryPublisher::new())
        .topic("canon.fleet.events")
        .build()?;

    info!(service = service.service_name(), "fleet-service ready");

    // ── Command Dispatcher ────────────────────────────────────────────────
    // The dispatcher polls inbox_messages for commands addressed to "Ship",
    // runs the registered command handlers, and writes resulting events to
    // the outbox table. The outbox processor then publishes them to Kafka.
    let dispatcher_store =
        PgDispatcherStore::new(yugabyte_pool.clone(), event_store.clone(), "Ship");
    let dispatcher_config = DispatcherConfig {
        batch_size: 100,
        poll_interval_ms: 100,
        aggregate_type_id: std::any::TypeId::of::<Ship>(),
        max_retries: 3,
    };
    let dispatcher = Dispatcher::new(dispatcher_store, dispatcher_config);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let dispatcher_handle = tokio::spawn(async move {
        info!("command dispatcher started");
        if let Err(e) = dispatcher
            .run(shutdown_rx, |err| {
                warn!(error = %err, "dispatcher error");
            })
            .await
        {
            error!(error = %e, "dispatcher exited with error");
        }
    });

    // Wait for shutdown signal.
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "failed to listen for ctrl-c");
    }

    info!("fleet-service shutting down");
    let _ = shutdown_tx.send(true);
    let _ = dispatcher_handle.await;

    Ok(())
}
