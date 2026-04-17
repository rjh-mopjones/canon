use std::sync::Arc;

use tracing::{error, info, warn};

use canon_adaptor_kafka::KafkaEventAdaptor;
use canon_command_store_yugabyte::dispatcher_store::PgDispatcherStore;
use canon_command_store_yugabyte::outbox_store::YugabyteOutboxStore;
use canon_core::{
    new_dispatcher_notify_channel, new_outbox_notify_channel, Dispatcher, DispatcherConfig,
    EventPayloadSnapshotProvider, ServiceBuilder,
};
use canon_deadletter_yugabyte::{YugabyteDeadLetterStore, YugabyteRetryTracker};
use canon_inbound_queue_kafka::KafkaInboundQueue;
use canon_inbox_yugabyte::YugabyteInbox;
use canon_outbound_queue_kafka::{
    KafkaOutboundConsumer, KafkaOutboundConsumerConfig, KafkaOutboundProducer,
    KafkaOutboundProducerConfig,
};
use canon_projection_store_yugabyte::YugabyteProjectionStore;
use canon_publisher_kafka::KafkaPublisher;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use cargo_service::aggregate::ManifestState;

// Ensure inventory-registered event handlers are linked into the binary.
#[allow(unused_imports)]
use cargo_service::event_handlers as _;

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("failed to connect to YugabyteDB: {0}")]
    YugabyteConnection(#[from] sqlx::Error),

    #[error("failed to connect to Cassandra: {0}")]
    CassandraConnection(String),

    #[error("failed to create Kafka producer: {0}")]
    KafkaProducer(String),

    #[error("failed to create outbound consumer: {0}")]
    OutboundConsumer(String),

    #[error("failed to create publisher: {0}")]
    Publisher(String),

    #[error("failed to create inbound queue: {0}")]
    InboundQueue(String),

    #[error("failed to create adaptor: {0}")]
    Adaptor(String),

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
    // Per-service schema isolation: cargo-service uses {prefix}_cargo schema
    // in YugabyteDB and Cassandra (prefix set via SCHEMA_PREFIX env var).
    let schema_prefix = env_or_default("SCHEMA_PREFIX", "canon");
    let schema_name = format!("{schema_prefix}_cargo");
    info!("connecting to YugabyteDB at {yugabyte_url} (schema: {schema_name})");
    let yugabyte_pool =
        canon_demo_shared::db::create_service_pool(&yugabyte_url, &schema_name).await?;
    info!("YugabyteDB connected (schema: {schema_name})");

    info!("connecting to Cassandra at {cassandra_nodes} (keyspace: {schema_name})");
    let event_store = Arc::new(
        canon_event_store_cassandra::CassandraEventStore::new_with_keyspace(
            &cassandra_nodes,
            &schema_name,
        )
        .await
        .map_err(|e| StartupError::CassandraConnection(e.to_string()))?,
    );
    info!("Cassandra connected (keyspace: {schema_name})");

    // Topic prefix for Kafka topic isolation (set via TOPIC_PREFIX env var).
    let topic_prefix = env_or_default("TOPIC_PREFIX", "canon");
    let outbound_topic = format!("{topic_prefix}.cargo.outbound");
    let events_topic = format!("{topic_prefix}.cargo.events");

    info!(brokers = %kafka_brokers, topic_prefix = %topic_prefix, "Kafka brokers configured");

    // ── Real infrastructure stores ────────────────────────────────────────

    // YugabyteDB-backed stores
    let outbox_store = YugabyteOutboxStore::new(yugabyte_pool.clone());
    let dead_letter_store = YugabyteDeadLetterStore::new(yugabyte_pool.clone());
    let retry_tracker = YugabyteRetryTracker::new(yugabyte_pool.clone());

    // Kafka outbox publisher: outbox processor → outbound queue
    let outbox_publisher = KafkaOutboundProducer::new(&KafkaOutboundProducerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.clone(),
    })
    .await
    .map_err(|e| StartupError::KafkaProducer(e.to_string()))?;

    // Kafka outbound consumers: 3 independent consumer groups reading from
    // the outbound topic. Each uses a distinct group ID so they receive all
    // messages independently.
    //
    // Load persisted offsets so consumers resume where they left off.

    let es_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "cargo:es-consumer").await;
    info!(consumer = "cargo:es-consumer", offset = ?es_offset, "loaded persisted offset");
    let es_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.cargo.event-store-consumer".to_owned(),
        initial_offset: es_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let es_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        es_receiver,
        yugabyte_pool.clone(),
        "cargo:es-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    let proj_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "cargo:proj-consumer").await;
    info!(consumer = "cargo:proj-consumer", offset = ?proj_offset, "loaded persisted offset");
    let proj_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.cargo.projection-consumer".to_owned(),
        initial_offset: proj_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let proj_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        proj_receiver,
        yugabyte_pool.clone(),
        "cargo:proj-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    let pub_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "cargo:pub-consumer").await;
    info!(consumer = "cargo:pub-consumer", offset = ?pub_offset, "loaded persisted offset");
    let pub_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.cargo.publisher-consumer".to_owned(),
        initial_offset: pub_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let pub_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        pub_receiver,
        yugabyte_pool.clone(),
        "cargo:pub-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    // YugabyteDB-backed snapshot and projection stores
    let snapshot_store = YugabyteSnapshotStore::new(yugabyte_pool.clone());
    let projection_store = YugabyteProjectionStore::from_pool(yugabyte_pool.clone());

    // Kafka publisher for cross-service event distribution
    let publisher = KafkaPublisher::new(&kafka_brokers, "cargo")
        .await
        .map_err(|e| StartupError::Publisher(e.to_string()))?;

    // ── ServiceBuilder ────────────────────────────────────────────────────
    // All stores are real infrastructure — no InMemory* stores.
    // - Event store: CassandraEventStore (via Arc for shared ownership with Dispatcher)
    // - Snapshot store: YugabyteSnapshotStore
    // - Outbox: YugabyteOutboxStore → KafkaOutboundProducer
    // - Projection checkpoints: YugabyteProjectionStore
    // - Publisher: KafkaPublisher → canon.cargo.events
    // - Consumer receivers: KafkaOutboundConsumer x 3 (distinct consumer groups)
    let service = ServiceBuilder::new("cargo")
        .for_aggregate::<ManifestState>()
        .event_store(event_store.clone())
        .snapshot_store(snapshot_store)
        .dead_letter_store(dead_letter_store)
        .retry_tracker(retry_tracker)
        .snapshot_state_provider(EventPayloadSnapshotProvider)
        .outbox_store(outbox_store)
        .outbox_publisher(outbox_publisher)
        .projection_checkpoint_store(projection_store)
        .publisher(publisher)
        .topic(&events_topic)
        .build()?;

    info!(service = service.service_name(), "cargo-service ready");

    // ── Command Dispatcher ────────────────────────────────────────────────
    // The dispatcher polls inbox_messages for commands addressed to "ManifestState",
    // runs the registered command handlers, and writes resulting events to
    // the outbox table. The outbox processor then publishes them to Kafka.
    let dispatcher_store =
        PgDispatcherStore::new(yugabyte_pool.clone(), event_store.clone(), "ManifestState");
    let dispatcher_config = DispatcherConfig {
        batch_size: 100,
        poll_interval_ms: 100,
        aggregate_type_id: std::any::TypeId::of::<ManifestState>(),
        max_retries: 3,
    };
    // Create an outbox notify channel so the outbox processor wakes immediately
    // when the dispatcher writes new events, instead of waiting for its next poll cycle.
    let (notify_tx, notify_rx) = new_outbox_notify_channel(16);

    // Create a dispatcher notify channel so the dispatcher wakes immediately
    // when a cross-service consumer writes to the inbox.
    let (_dispatcher_notify_tx, dispatcher_notify_rx) = new_dispatcher_notify_channel(16);

    let mut dispatcher = Dispatcher::new(dispatcher_store, dispatcher_config)
        .with_outbox_notify(notify_tx)
        .with_dispatcher_notify(dispatcher_notify_rx);

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
                Some(notify_rx),
                es_receiver,
                proj_receiver,
                pub_receiver,
            )
            .await;
        info!("pipeline background processors stopped");
    });

    // ── Cross-service event consumers (framework-driven) ─────────────────
    // Uses KafkaEventAdaptor::consume_and_route() to subscribe to external
    // topics and auto-route events to registered #[event_handler] impls via
    // the inbox. No hand-wired Kafka consumers or manual CommandEnvelope
    // construction.
    let inbound_queue = KafkaInboundQueue::new(&kafka_brokers, "cargo", "cargo-inbound")
        .await
        .map_err(|e| StartupError::InboundQueue(e.to_string()))?;
    let inbox = Arc::new(YugabyteInbox::new(
        yugabyte_pool.clone(),
        Arc::new(inbound_queue),
    ));
    let adaptor = KafkaEventAdaptor::new(&kafka_brokers, "cargo", inbox);

    // Navigation:ShipArrivedAtStation → ArrivalHandler
    let nav_topic = format!("{topic_prefix}.navigation.events");
    let nav_handle = adaptor
        .consume_and_route(&nav_topic, shutdown_tx.subscribe())
        .await
        .map_err(|e| StartupError::Adaptor(e.to_string()))?;

    // Wait for shutdown signal.
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "failed to listen for ctrl-c");
    }

    info!("cargo-service shutting down");
    let _ = shutdown_tx.send(true);
    let _ = dispatcher_handle.await;
    let _ = service_handle.await;
    let _ = nav_handle.await;

    Ok(())
}
