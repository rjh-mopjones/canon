use std::sync::Arc;

use tracing::{error, info, warn};

use canon_command_store_yugabyte::outbox_store::YugabyteOutboxStore;
use canon_core::{
    Dispatcher, DispatcherConfig, EventPayloadSnapshotProvider, InMemoryConsumerReceiver,
    InMemoryEventStore, InMemoryOutboundQueue, InMemoryProjectionStore, InMemoryPublisher,
    InMemorySnapshotStore, ServiceBuilder,
};
use canon_deadletter_yugabyte::{YugabyteDeadLetterStore, YugabyteRetryTracker};
use canon_outbound_queue_kafka::{KafkaOutboundProducer, KafkaOutboundProducerConfig};
// YugabyteSnapshotStore available but not yet used in ServiceBuilder
// (CassandraEventStore is not Clone, so the consumer-side stores are in-memory)
use fleet_service::aggregate::Ship;
use fleet_service::dispatcher_store::PgDispatcherStore;

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("failed to connect to YugabyteDB: {0}")]
    YugabyteConnection(#[from] sqlx::Error),

    #[error("failed to connect to Cassandra: {0}")]
    CassandraConnection(String),

    #[error("failed to create Kafka producer: {0}")]
    KafkaProducer(String),

    #[error("failed to create outbound queue: {0}")]
    OutboundQueue(String),

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

    // ── Real infrastructure stores ────────────────────────────────────────

    // YugabyteDB-backed stores
    let outbox_store = YugabyteOutboxStore::new(yugabyte_pool.clone());
    let dead_letter_store = YugabyteDeadLetterStore::new(yugabyte_pool.clone());
    let retry_tracker = YugabyteRetryTracker::new(yugabyte_pool.clone());

    // Kafka outbox publisher: outbox processor → outbound queue
    let outbox_publisher = KafkaOutboundProducer::new(&KafkaOutboundProducerConfig {
        brokers: kafka_brokers.clone(),
        topic: "canon.fleet.outbound".to_owned(),
    })
    .map_err(|e| StartupError::KafkaProducer(e.to_string()))?;

    // Consumer receivers: the Kafka consumer does not yet implement
    // ConsumerReceiver, so we use in-memory receivers connected to a
    // shared InMemoryOutboundQueue. In production, these would be
    // KafkaOutboundConsumer instances with distinct consumer group IDs.
    let consumer_queue = InMemoryOutboundQueue::new();
    let es_receiver = InMemoryConsumerReceiver::new(consumer_queue.clone())
        .map_err(|e| StartupError::OutboundQueue(e.to_string()))?;
    let proj_receiver = InMemoryConsumerReceiver::new(consumer_queue.clone())
        .map_err(|e| StartupError::OutboundQueue(e.to_string()))?;
    let pub_receiver = InMemoryConsumerReceiver::new(consumer_queue)
        .map_err(|e| StartupError::OutboundQueue(e.to_string()))?;

    // ── ServiceBuilder ────────────────────────────────────────────────────
    // Uses real YugabyteDB/Kafka stores for the outbox path (command →
    // outbox → Kafka). Consumer-side receivers and the event/snapshot
    // stores within the ServiceBuilder are in-memory because:
    //   1. KafkaOutboundConsumer doesn't implement ConsumerReceiver yet
    //   2. CassandraEventStore is not Clone (required for build w/ command store)
    // The real CassandraEventStore is used by the Dispatcher for hydration.
    // The real outbox path is: outbox_store (YugabyteDB) → outbox_publisher (Kafka).
    let service = ServiceBuilder::new("fleet")
        .for_aggregate::<Ship>()
        .event_store(InMemoryEventStore::new())
        .snapshot_store(InMemorySnapshotStore::new())
        .dead_letter_store(dead_letter_store)
        .retry_tracker(retry_tracker)
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(outbox_store)
        .outbox_publisher(outbox_publisher)
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

    // ── Background pipeline processors ────────────────────────────────────
    // service.start() spawns the outbox processor, event store consumer,
    // projection consumer, and publisher consumer as background tasks.
    let service_shutdown_rx = shutdown_tx.subscribe();
    let service_handle = tokio::spawn(async move {
        info!("starting pipeline background processors");
        service
            .start(
                service_shutdown_rx,
                None,
                es_receiver,
                proj_receiver,
                pub_receiver,
            )
            .await;
        info!("pipeline background processors stopped");
    });

    // Wait for shutdown signal.
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "failed to listen for ctrl-c");
    }

    info!("fleet-service shutting down");
    let _ = shutdown_tx.send(true);
    let _ = dispatcher_handle.await;
    let _ = service_handle.await;

    Ok(())
}
