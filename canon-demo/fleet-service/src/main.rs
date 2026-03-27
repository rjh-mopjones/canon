use std::sync::Arc;

use tracing::{error, info, warn};

use canon_command_store_yugabyte::dispatcher_store::PgDispatcherStore;
use canon_command_store_yugabyte::outbox_store::YugabyteOutboxStore;
use canon_core::{
    new_outbox_notify_channel, Dispatcher, DispatcherConfig, EventPayloadSnapshotProvider,
    ServiceBuilder,
};
use canon_deadletter_yugabyte::{YugabyteDeadLetterStore, YugabyteRetryTracker};
use canon_outbound_queue_kafka::{
    KafkaOutboundConsumer, KafkaOutboundConsumerConfig, KafkaOutboundProducer,
    KafkaOutboundProducerConfig,
};
use canon_projection_store_yugabyte::YugabyteProjectionStore;
use canon_publisher_kafka::KafkaPublisher;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use fleet_service::aggregate::Ship;

mod cross_service;

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
    // Per-service schema isolation: fleet-service uses {prefix}_fleet schema
    // in YugabyteDB and Cassandra. Default prefix is "canon", staging uses
    // "canon_staging" (set via SCHEMA_PREFIX env var).
    let schema_prefix = env_or_default("SCHEMA_PREFIX", "canon");
    let schema_name = format!("{schema_prefix}_fleet");
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

    // Topic prefix for Kafka topic isolation (staging vs prod).
    // Default prefix is "canon", staging uses "canon.staging" (set via TOPIC_PREFIX env var).
    let topic_prefix = env_or_default("TOPIC_PREFIX", "canon");
    let outbound_topic = format!("{topic_prefix}.fleet.outbound");
    let events_topic = format!("{topic_prefix}.fleet.events");

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
    // Load persisted offsets so consumers resume where they left off instead
    // of replaying from zero on every restart.

    let es_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "fleet:es-consumer").await;
    info!(consumer = "fleet:es-consumer", offset = ?es_offset, "loaded persisted offset");
    let es_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.fleet.event-store-consumer".to_owned(),
        initial_offset: es_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let es_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        es_receiver,
        yugabyte_pool.clone(),
        "fleet:es-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    let proj_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "fleet:proj-consumer").await;
    info!(consumer = "fleet:proj-consumer", offset = ?proj_offset, "loaded persisted offset");
    let proj_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.fleet.projection-consumer".to_owned(),
        initial_offset: proj_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let proj_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        proj_receiver,
        yugabyte_pool.clone(),
        "fleet:proj-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    let pub_offset =
        canon_demo_shared::offsets::load_offset(&yugabyte_pool, "fleet:pub-consumer").await;
    info!(consumer = "fleet:pub-consumer", offset = ?pub_offset, "loaded persisted offset");
    let pub_receiver = KafkaOutboundConsumer::new(&KafkaOutboundConsumerConfig {
        brokers: kafka_brokers.clone(),
        topic: outbound_topic.to_owned(),
        group_id: "canon.fleet.publisher-consumer".to_owned(),
        initial_offset: pub_offset,
        ..Default::default()
    })
    .await
    .map_err(|e| StartupError::OutboundConsumer(e.to_string()))?;
    let pub_receiver = canon_demo_shared::offsets::OffsetTrackingReceiver::new(
        pub_receiver,
        yugabyte_pool.clone(),
        "fleet:pub-consumer".to_owned(),
        outbound_topic.to_owned(),
    );

    // YugabyteDB-backed snapshot and projection stores
    let snapshot_store = YugabyteSnapshotStore::new(yugabyte_pool.clone());
    let projection_store = YugabyteProjectionStore::from_pool(yugabyte_pool.clone());

    // Kafka publisher for cross-service event distribution
    let publisher = KafkaPublisher::new(&kafka_brokers, "fleet")
        .await
        .map_err(|e| StartupError::Publisher(e.to_string()))?;

    // ── ServiceBuilder ────────────────────────────────────────────────────
    // All stores are real infrastructure — no InMemory* stores.
    // - Event store: CassandraEventStore (via Arc for shared ownership with Dispatcher)
    // - Snapshot store: YugabyteSnapshotStore
    // - Outbox: YugabyteOutboxStore → KafkaOutboundProducer
    // - Projection checkpoints: YugabyteProjectionStore
    // - Publisher: KafkaPublisher → canon.fleet.events
    // - Consumer receivers: KafkaOutboundConsumer × 3 (distinct consumer groups)
    let service = ServiceBuilder::new("fleet")
        .for_aggregate::<Ship>()
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
    // Create an outbox notify channel so the outbox processor wakes immediately
    // when the dispatcher writes new events, instead of waiting for its next poll cycle.
    let (notify_tx, notify_rx) = new_outbox_notify_channel(16);

    let dispatcher =
        Dispatcher::new(dispatcher_store, dispatcher_config).with_outbox_notify(notify_tx);

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

    // ── Cross-service event consumer ──────────────────────────────────────
    // Subscribes to {topic_prefix}.supply.events and submits ScheduleResupply
    // commands to the fleet inbox when ResupplyDispatched arrives.
    let cross_service_pool = yugabyte_pool.clone();
    let cross_service_brokers = kafka_brokers.clone();
    let cross_service_shutdown = shutdown_tx.subscribe();
    let cross_service_topic_prefix = topic_prefix.clone();
    let cross_service_handle = tokio::spawn(async move {
        let supply_events_topic = format!("{cross_service_topic_prefix}.supply.events");
        info!("cross-service consumer started ({supply_events_topic})");
        cross_service::consume_supply_events(
            &cross_service_brokers,
            cross_service_pool,
            cross_service_shutdown,
            &cross_service_topic_prefix,
        )
        .await;
    });

    // ── Navigation event consumer ─────────────────────────────────────
    // Subscribes to {topic_prefix}.navigation.events and submits DockShip commands
    // to the fleet inbox when ShipArrivedAtStation arrives, transitioning
    // the ship back to Docked status.
    let nav_pool = yugabyte_pool.clone();
    let nav_brokers = kafka_brokers.clone();
    let nav_shutdown = shutdown_tx.subscribe();
    let nav_topic_prefix = topic_prefix.clone();
    let nav_handle = tokio::spawn(async move {
        let nav_events_topic = format!("{nav_topic_prefix}.navigation.events");
        info!("navigation event consumer started ({nav_events_topic})");
        cross_service::consume_navigation_events(
            &nav_brokers,
            nav_pool,
            nav_shutdown,
            &nav_topic_prefix,
        )
        .await;
    });

    // Wait for shutdown signal.
    if let Err(e) = tokio::signal::ctrl_c().await {
        error!(error = %e, "failed to listen for ctrl-c");
    }

    info!("fleet-service shutting down");
    let _ = shutdown_tx.send(true);
    let _ = dispatcher_handle.await;
    let _ = service_handle.await;
    let _ = cross_service_handle.await;
    let _ = nav_handle.await;

    Ok(())
}
