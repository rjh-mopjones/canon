//! Tier 2: Testcontainers e2e test suite for Canon pipeline.
//!
//! Exercises the full Canon pipeline against real YugabyteDB (via Postgres container),
//! Cassandra (via ScyllaDB container), and Kafka instances managed by testcontainers.
//!
//! No `#[ignore]` — testcontainers handles container lifecycle automatically.
//! Each test uses unique aggregate IDs to avoid cross-test interference.
//! Containers are shared per test module via `std::sync::OnceLock`.

use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use uuid::Uuid;

use canon_core::consumers::{
    EventPayloadSnapshotProvider, EventStoreConsumer, EventStoreConsumerConfig, ProjectionConsumer,
    RegisteredProjection,
};
use canon_core::traits::{CommandStore, DeadLetterStore, EventStore, Publisher, SnapshotStore};
use canon_core::{AggregateId, CommandEnvelope, EventEnvelope, Version};
use canon_projection_store::ProjectionStore;

use canon_command_store_yugabyte::YugabyteCommandStore;
use canon_deadletter_yugabyte::{YugabyteDeadLetterStore, YugabyteRetryTracker};
use canon_event_store_cassandra::{CassandraEventStore, EventStoreError};
use canon_outbound_queue_kafka::{
    KafkaOutboundConsumer, KafkaOutboundConsumerConfig, KafkaOutboundProducer,
    KafkaOutboundProducerConfig,
};
use canon_projection_store_yugabyte::YugabyteProjectionStore;
use canon_publisher_kafka::KafkaPublisher;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;

use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::kafka::apache::Kafka;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::scylladb::ScyllaDB;

// ── Container infrastructure ────────────────────────────────────────────────

/// Shared Postgres container (simulating YugabyteDB).
/// YugabyteDB is wire-compatible with Postgres, so we use a Postgres container.
struct PgContainer {
    _container: ContainerAsync<Postgres>,
    url: String,
}

/// Shared ScyllaDB container (Cassandra-compatible).
struct ScyllaContainer {
    _container: ContainerAsync<ScyllaDB>,
    nodes: String,
}

/// Shared Kafka container.
struct KafkaContainer {
    _container: ContainerAsync<Kafka>,
    brokers: String,
}

static PG: OnceLock<tokio::sync::OnceCell<PgContainer>> = OnceLock::new();
static SCYLLA: OnceLock<tokio::sync::OnceCell<ScyllaContainer>> = OnceLock::new();
static KAFKA: OnceLock<tokio::sync::OnceCell<KafkaContainer>> = OnceLock::new();

fn pg_cell() -> &'static tokio::sync::OnceCell<PgContainer> {
    PG.get_or_init(tokio::sync::OnceCell::new)
}

fn scylla_cell() -> &'static tokio::sync::OnceCell<ScyllaContainer> {
    SCYLLA.get_or_init(tokio::sync::OnceCell::new)
}

fn kafka_cell() -> &'static tokio::sync::OnceCell<KafkaContainer> {
    KAFKA.get_or_init(tokio::sync::OnceCell::new)
}

async fn get_pg() -> &'static PgContainer {
    pg_cell()
        .get_or_init(|| async {
            let container = Postgres::default()
                .start()
                .await
                .expect("failed to start Postgres container");

            let host_port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("failed to get Postgres port");

            let url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres");

            // Retry connection — Postgres may take a moment to accept connections
            let mut pool = None;
            for attempt in 0..30 {
                match PgPool::connect(&url).await {
                    Ok(p) => {
                        pool = Some(p);
                        break;
                    }
                    Err(e) => {
                        if attempt >= 29 {
                            panic!("failed to connect to Postgres after 30 attempts: {e}");
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            let pool = pool.expect("Postgres pool should be established");

            // Run all YugabyteDB migrations/schema setup
            setup_yugabyte_schema(&pool).await;

            PgContainer {
                _container: container,
                url,
            }
        })
        .await
}

async fn get_scylla() -> &'static ScyllaContainer {
    scylla_cell()
        .get_or_init(|| async {
            let container = ScyllaDB::default()
                .with_startup_timeout(Duration::from_secs(120))
                .start()
                .await
                .expect("failed to start ScyllaDB container");

            let host_port = container
                .get_host_port_ipv4(9042)
                .await
                .expect("failed to get ScyllaDB port");

            let nodes = format!("127.0.0.1:{host_port}");

            // Wait for ScyllaDB to be ready and create schema
            setup_cassandra_schema(&nodes).await;

            ScyllaContainer {
                _container: container,
                nodes,
            }
        })
        .await
}

async fn get_kafka() -> &'static KafkaContainer {
    kafka_cell()
        .get_or_init(|| async {
            let container = Kafka::default()
                .start()
                .await
                .expect("failed to start Kafka container");

            let host_port = container
                .get_host_port_ipv4(9092)
                .await
                .expect("failed to get Kafka port");

            let brokers = format!("127.0.0.1:{host_port}");

            // Retry Kafka readiness — the broker may take a moment to accept connections.
            // We probe by creating a BaseConsumer and fetching cluster metadata.
            for attempt in 0..30 {
                let probe: Result<rdkafka::consumer::BaseConsumer, _> =
                    rdkafka::config::ClientConfig::new()
                        .set("bootstrap.servers", &brokers)
                        .create();
                match probe {
                    Ok(consumer) => {
                        use rdkafka::consumer::Consumer;
                        match consumer.fetch_metadata(None, Duration::from_secs(2)) {
                            Ok(_) => break,
                            Err(e) => {
                                if attempt >= 29 {
                                    panic!("Kafka broker not ready after 30 attempts: {e}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if attempt >= 29 {
                            panic!("failed to create Kafka probe consumer after 30 attempts: {e}");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            KafkaContainer {
                _container: container,
                brokers,
            }
        })
        .await
}

// ── Schema setup ────────────────────────────────────────────────────────────

async fn setup_yugabyte_schema(pool: &PgPool) {
    // Enable pgcrypto for gen_random_uuid() support in standard Postgres
    sqlx::query("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\"")
        .execute(pool)
        .await
        .expect("create pgcrypto extension");

    // Use include_str! to load DDL from the actual migration files rather than
    // duplicating the SQL inline. This ensures tests always match the real schema.

    // Commands (migration file uses IF NOT EXISTS)
    sqlx::raw_sql(include_str!(
        "../../canon-command-store-yugabyte/migrations/001_commands.sql"
    ))
    .execute(pool)
    .await
    .expect("run commands migration");

    // Inbox (migration file omits IF NOT EXISTS — add it for idempotent setup)
    sqlx::raw_sql(
        &include_str!("../../canon-inbox-yugabyte/migrations/001_inbox.sql")
            .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "),
    )
    .execute(pool)
    .await
    .expect("run inbox migration");

    // Snapshots (migration file omits IF NOT EXISTS — add it for idempotent setup)
    sqlx::raw_sql(
        &include_str!("../../canon-snapshot-store-yugabyte/migrations/001_snapshots.sql")
            .replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "),
    )
    .execute(pool)
    .await
    .expect("run snapshots migration");

    // Projections (migration file uses IF NOT EXISTS)
    sqlx::raw_sql(include_str!(
        "../../canon-projection-store-yugabyte/migrations/001_projections.sql"
    ))
    .execute(pool)
    .await
    .expect("run projections migration");

    // Retry attempts (migration file uses IF NOT EXISTS)
    sqlx::raw_sql(include_str!(
        "../../canon-deadletter-yugabyte/migrations/001_retry_attempts.sql"
    ))
    .execute(pool)
    .await
    .expect("run retry_attempts migration");

    // Outbox (no migration file — inline DDL)
    sqlx::query("CREATE SEQUENCE IF NOT EXISTS outbox_seq")
        .execute(pool)
        .await
        .expect("create outbox seq");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS outbox (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            sequence_number BIGINT      DEFAULT nextval('outbox_seq'),
            aggregate_id    UUID,
            payload         BYTEA,
            created_at      TIMESTAMPTZ DEFAULT now(),
            delivered_at    TIMESTAMPTZ
        )",
    )
    .execute(pool)
    .await
    .expect("create outbox table");
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS outbox_seq_idx
         ON outbox (sequence_number) WHERE delivered_at IS NULL",
    )
    .execute(pool)
    .await
    .expect("create outbox index");

    // Dead letters (no migration file — from canon-deadletter-yugabyte/src/lib.rs)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS dead_letters (
            id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id      UUID,
            handler_id      TEXT,
            aggregate_id    UUID,
            payload         BYTEA,
            error           TEXT,
            attempts        INT         DEFAULT 1,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_attempted  TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await
    .expect("create dead_letters table");
}

async fn setup_cassandra_schema(nodes: &str) {
    use scylla::SessionBuilder;

    // Retry connection — ScyllaDB may take a moment to start
    let mut session = None;
    for attempt in 0..30 {
        match SessionBuilder::new().known_node(nodes).build().await {
            Ok(s) => {
                session = Some(s);
                break;
            }
            Err(e) => {
                if attempt >= 29 {
                    panic!("failed to connect to ScyllaDB after 30 attempts: {e}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    let session = session.expect("ScyllaDB session should be established");

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

// ── Helper functions ────────────────────────────────────────────────────────

fn make_event_envelope(
    aggregate_id: &AggregateId,
    version: u64,
    event_type: &str,
) -> EventEnvelope {
    EventEnvelope {
        event_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        version: Version::from_u64(version),
        event_type: event_type.to_owned(),
        event_version: 1,
        payload: Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "test": true,
                "version": version
            }))
            .expect("serialize test payload"),
        ),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
    }
}

fn make_command_envelope(aggregate_id: &AggregateId, command_type: &str) -> CommandEnvelope {
    CommandEnvelope {
        command_id: Uuid::new_v4(),
        aggregate_id: aggregate_id.clone(),
        command_type: command_type.to_owned(),
        correlation_id: Uuid::new_v4(),
        causation_id: Uuid::new_v4(),
        timestamp: Utc::now(),
        payload: Bytes::from_static(b"{}"),
        command_version: 1,
    }
}

async fn get_pg_pool() -> PgPool {
    let pg = get_pg().await;
    PgPool::connect(&pg.url)
        .await
        .expect("failed to connect to Postgres pool")
}

/// Receive from a Kafka consumer with retries, waiting for consumer group rebalancing.
async fn receive_with_retry(
    consumer: &KafkaOutboundConsumer,
    retries: u32,
) -> Option<EventEnvelope> {
    for _ in 0..retries {
        match consumer.receive().await {
            Ok(Some(env)) => return Some(env),
            Ok(None) => {}
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

// ── Test 1: Command -> Cassandra event store ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_command_to_cassandra_event_store() {
    let scylla = get_scylla().await;
    let pg = get_pg().await;

    let cassandra_store = CassandraEventStore::new(&scylla.nodes)
        .await
        .expect("failed to create CassandraEventStore");
    let pool = PgPool::connect(&pg.url)
        .await
        .expect("failed to connect to pg");
    let snapshot_store = YugabyteSnapshotStore::new(pool.clone());
    let dead_letter_store = YugabyteDeadLetterStore::new(pool.clone());
    let retry_tracker = YugabyteRetryTracker::new(pool);

    let consumer = EventStoreConsumer::new(
        cassandra_store,
        snapshot_store,
        dead_letter_store,
        retry_tracker,
        EventPayloadSnapshotProvider,
        EventStoreConsumerConfig::default(),
    );

    let agg_id = AggregateId::new();
    let envelope = make_event_envelope(&agg_id, 1, "ShipRegistered");

    consumer
        .process(envelope.clone())
        .await
        .expect("event store consumer should process event");

    // Verify the event was written to real Cassandra
    let cassandra_verify = CassandraEventStore::new(&scylla.nodes)
        .await
        .expect("failed to create verification store");
    let events = cassandra_verify
        .load(&agg_id)
        .await
        .expect("failed to load events from Cassandra");

    assert_eq!(events.len(), 1, "Cassandra should have exactly 1 event");
    assert_eq!(events[0].event_type, "ShipRegistered");
    assert_eq!(events[0].version.as_u64(), 1);
    assert_eq!(events[0].aggregate_id, agg_id);
}

// ── Test 2: Command -> YugabyteDB projection ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_command_to_yugabyte_projection() {
    let pool = get_pg_pool().await;
    let projection_store = YugabyteProjectionStore::from_pool(pool.clone());

    let mut projection_consumer = ProjectionConsumer::new(projection_store.clone());

    let projection_id = format!("test-proj-{}", Uuid::new_v4());
    let proj_id_clone = projection_id.clone();

    // Register a projection that upserts into the real YugabyteDB projections table.
    // The apply_fn is synchronous, so we use tokio::task::block_in_place + block_on
    // to execute the async upsert within the sync closure.
    let write_pool = pool.clone();
    projection_consumer.register(RegisteredProjection {
        projection_id: projection_id.clone(),
        apply_fn: Box::new(move |proj_id, envelope| {
            let state = serde_json::json!({
                "event_type": &envelope.event_type,
                "version": envelope.version.as_u64()
            })
            .to_string();

            let pool = write_pool.clone();
            let proj_id = proj_id.to_owned();
            let agg_uuid = *envelope.aggregate_id.as_uuid();
            let json_value: serde_json::Value =
                serde_json::from_str(&state).map_err(|e| e.to_string())?;

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    sqlx::query(
                        "INSERT INTO projections (projection_id, aggregate_id, state, updated_at) \
                         VALUES ($1, $2, $3, now()) \
                         ON CONFLICT (projection_id, aggregate_id) \
                         DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
                    )
                    .bind(&proj_id)
                    .bind(agg_uuid)
                    .bind(&json_value)
                    .execute(&pool)
                    .await
                    .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                })
            })
        }),
    });

    let agg_id = AggregateId::new();
    let envelope = make_event_envelope(&agg_id, 1, "ShipRegistered");

    projection_consumer
        .process(&envelope, 1)
        .await
        .expect("projection consumer should process event");

    // Verify checkpoint was updated in real YugabyteDB
    let version = projection_store
        .get_last_version(&proj_id_clone)
        .await
        .expect("failed to get checkpoint");

    assert_eq!(
        version.as_u64(),
        1,
        "projection checkpoint should be at version 1 in real YugabyteDB"
    );

    // Verify actual projection data was written to the projections table
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT state FROM projections WHERE projection_id = $1 AND aggregate_id = $2",
    )
    .bind(&proj_id_clone)
    .bind(agg_id.as_uuid())
    .fetch_optional(&pool)
    .await
    .expect("failed to query projection data");

    let (state_value,) = row.expect("projection row should exist in real YugabyteDB");
    assert_eq!(state_value["event_type"], "ShipRegistered");
    assert_eq!(state_value["version"], 1);
}

// ── Test 3: Command -> Kafka external publish -> consume ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_command_to_kafka_publish_consume() {
    let kafka = get_kafka().await;

    let topic = format!("canon.test.events.{}", Uuid::new_v4());
    let publisher =
        KafkaPublisher::new(&kafka.brokers, "test").expect("failed to create KafkaPublisher");

    let agg_id = AggregateId::new();
    let envelope = make_event_envelope(&agg_id, 1, "ShipRegistered");
    let event_id = envelope.event_id;

    // Publish to Kafka
    publisher
        .publish(envelope, &topic)
        .await
        .expect("publish should succeed");

    // Consume from the same topic
    let consumer_config = KafkaOutboundConsumerConfig {
        brokers: kafka.brokers.clone(),
        topic: topic.clone(),
        group_id: format!("test-consume-{}", Uuid::new_v4()),
        session_timeout_ms: 6000,
        enable_auto_commit: false,
        receive_timeout_ms: 500,
    };
    let consumer =
        KafkaOutboundConsumer::new(&consumer_config).expect("failed to create Kafka consumer");

    let received = receive_with_retry(&consumer, 50)
        .await
        .expect("should receive the published event from Kafka");

    assert_eq!(received.event_id, event_id);
    assert_eq!(received.aggregate_id, agg_id);
    assert_eq!(received.event_type, "ShipRegistered");
}

// ── Test 4: Cross-service cascade via Kafka ─────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_cross_service_cascade() {
    let kafka = get_kafka().await;

    // Simulate: fleet service publishes ShipDeparted on canon.fleet.events,
    // navigation service consumes it.
    let topic = format!("canon.fleet.events.{}", Uuid::new_v4());

    let fleet_publisher =
        KafkaPublisher::new(&kafka.brokers, "fleet").expect("failed to create fleet publisher");

    let agg_id = AggregateId::new();
    let ship_departed = make_event_envelope(&agg_id, 1, "ShipDeparted");
    let event_id = ship_departed.event_id;

    // Fleet service publishes
    fleet_publisher
        .publish(ship_departed, &topic)
        .await
        .expect("fleet publish should succeed");

    // Navigation service consumes (different consumer group)
    let nav_consumer_config = KafkaOutboundConsumerConfig {
        brokers: kafka.brokers.clone(),
        topic: topic.clone(),
        group_id: format!("navigation-consumer-{}", Uuid::new_v4()),
        session_timeout_ms: 6000,
        enable_auto_commit: false,
        receive_timeout_ms: 500,
    };
    let nav_consumer =
        KafkaOutboundConsumer::new(&nav_consumer_config).expect("failed to create nav consumer");

    let received = receive_with_retry(&nav_consumer, 50)
        .await
        .expect("navigation should receive ShipDeparted from fleet topic");

    assert_eq!(received.event_id, event_id);
    assert_eq!(received.event_type, "ShipDeparted");
    assert_eq!(received.aggregate_id, agg_id);
}

// ── Test 5: Snapshotting — 50 events -> snapshot in real YugabyteDB ─────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_snapshotting() {
    let scylla = get_scylla().await;
    let pool = get_pg_pool().await;

    let cassandra_store = CassandraEventStore::new(&scylla.nodes)
        .await
        .expect("failed to create CassandraEventStore");
    let snapshot_store = YugabyteSnapshotStore::new(pool.clone());
    let dead_letter_store = YugabyteDeadLetterStore::new(pool.clone());
    let retry_tracker = YugabyteRetryTracker::new(pool);

    let consumer = EventStoreConsumer::new(
        cassandra_store,
        snapshot_store.clone(),
        dead_letter_store,
        retry_tracker,
        EventPayloadSnapshotProvider,
        EventStoreConsumerConfig {
            snapshot_every: 50,
            max_retries: 3,
        },
    );

    let agg_id = AggregateId::new();

    // Send 50 events to trigger a snapshot
    for version in 1..=50u64 {
        let envelope = make_event_envelope(&agg_id, version, "TestEvent");
        consumer
            .process(envelope)
            .await
            .unwrap_or_else(|e| panic!("failed to process event at version {version}: {e}"));
    }

    // Verify snapshot exists in real YugabyteDB
    let snapshot = snapshot_store
        .load(&agg_id)
        .await
        .expect("failed to load snapshot")
        .expect("snapshot should exist at version 50");

    assert_eq!(
        snapshot.version.as_u64(),
        50,
        "snapshot should be at version 50 in real YugabyteDB"
    );
}

// ── Test 6: Idempotent replay — duplicate event rejected ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_idempotent_replay() {
    let scylla = get_scylla().await;

    let cassandra_store = CassandraEventStore::new(&scylla.nodes)
        .await
        .expect("failed to create CassandraEventStore");

    let agg_id = AggregateId::new();
    let envelope = make_event_envelope(&agg_id, 1, "ShipRegistered");

    // First append succeeds
    cassandra_store
        .append(&agg_id, Version::initial(), vec![envelope.clone()])
        .await
        .expect("first append should succeed");

    // Second append at same version should fail with version conflict (IF NOT EXISTS)
    let result = cassandra_store
        .append(&agg_id, Version::initial(), vec![envelope])
        .await;

    assert!(
        result.is_err(),
        "duplicate event should be rejected by Cassandra IF NOT EXISTS"
    );

    // Verify only one event exists
    let events = cassandra_store
        .load(&agg_id)
        .await
        .expect("failed to load events");
    assert_eq!(
        events.len(),
        1,
        "Cassandra should have exactly 1 event, not a duplicate"
    );
}

// ── Test 7: Dead letter store round-trip on real YugabyteDB ──────────────────
//
// Infrastructure-layer test: exercises the YugabyteDeadLetterStore directly
// against a real Postgres instance (store, list, requeue). A full e2e version
// would have a failing command handler cause the EventStoreConsumer to exhaust
// retries and route the event to the dead letter store automatically.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_dead_letter_store_roundtrip() {
    let pool = get_pg_pool().await;

    let dead_letter_store = YugabyteDeadLetterStore::new(pool.clone());
    let agg_id = AggregateId::new();
    let msg_id = Uuid::new_v4();

    // Store a dead letter directly
    let dl_id = dead_letter_store
        .store(
            msg_id,
            "test_handler",
            &agg_id,
            Bytes::from_static(b"{\"error\":true}"),
            "permanent failure: handler not found",
        )
        .await
        .expect("failed to store dead letter");

    // Verify it's in the real dead_letters table
    let letters = dead_letter_store
        .list(Some("test_handler"))
        .await
        .expect("failed to list dead letters");

    let found = letters.iter().any(|dl| dl.id == dl_id);
    assert!(
        found,
        "dead letter should be persisted in real YugabyteDB dead_letters table"
    );

    let dl = letters
        .iter()
        .find(|dl| dl.id == dl_id)
        .expect("dead letter should be found");
    assert_eq!(dl.message_id, msg_id);
    assert_eq!(dl.handler_id, "test_handler");
    assert_eq!(dl.error, "permanent failure: handler not found");

    // Requeue removes it
    dead_letter_store
        .requeue(dl_id)
        .await
        .expect("requeue should succeed");

    let remaining = dead_letter_store
        .list(Some("test_handler"))
        .await
        .expect("failed to list after requeue");
    let still_found = remaining.iter().any(|dl| dl.id == dl_id);
    assert!(
        !still_found,
        "dead letter should be removed after requeue in real YugabyteDB"
    );
}

// ── Test 8: Outbox ordering preserved through real Kafka ────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_outbox_ordering() {
    let kafka = get_kafka().await;

    let topic = format!("canon.test.ordering.{}", Uuid::new_v4());
    let producer_config = KafkaOutboundProducerConfig {
        brokers: kafka.brokers.clone(),
        topic: topic.clone(),
    };
    let producer =
        KafkaOutboundProducer::new(&producer_config).expect("failed to create Kafka producer");

    let agg_id = AggregateId::new();
    let mut event_ids = Vec::new();

    // Publish 5 events in order, same partition key (aggregate_id)
    for version in 1..=5u64 {
        let envelope = make_event_envelope(&agg_id, version, "TestEvent");
        event_ids.push(envelope.event_id);
        producer
            .publish(envelope)
            .await
            .unwrap_or_else(|e| panic!("failed to publish event {version}: {e}"));
    }

    // Consume and verify ordering
    let consumer_config = KafkaOutboundConsumerConfig {
        brokers: kafka.brokers.clone(),
        topic,
        group_id: format!("test-ordering-{}", Uuid::new_v4()),
        session_timeout_ms: 6000,
        enable_auto_commit: false,
        receive_timeout_ms: 500,
    };
    let consumer =
        KafkaOutboundConsumer::new(&consumer_config).expect("failed to create Kafka consumer");

    let mut received_ids = Vec::new();
    for _ in 0..5 {
        let received = receive_with_retry(&consumer, 50)
            .await
            .expect("should receive event");
        received_ids.push(received.event_id);
    }

    assert_eq!(
        received_ids, event_ids,
        "Kafka should preserve ordering for same partition key (aggregate_id)"
    );
}

// ── Test 9: Concurrent outbox SKIP LOCKED on real Postgres ───────────────────
//
// Infrastructure-layer test: exercises FOR UPDATE SKIP LOCKED directly on the
// outbox table to verify concurrent dispatchers don't double-process rows.
// A full e2e version would run two OutboxProcessor instances concurrently and
// verify each event is published to Kafka exactly once.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_concurrent_outbox_skip_locked() {
    let pool = get_pg_pool().await;

    // Insert two outbox entries
    let agg_id = AggregateId::new();
    for i in 1..=2 {
        let envelope = make_event_envelope(&agg_id, i, "TestEvent");
        let payload =
            serde_json::to_vec(&envelope).expect("serialize event envelope for outbox insert");
        sqlx::query(
            "INSERT INTO outbox (aggregate_id, payload, created_at) VALUES ($1, $2, now())",
        )
        .bind(agg_id.as_uuid())
        .bind(&payload)
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("failed to insert outbox entry {i}: {e}"));
    }

    // Simulate two concurrent dispatchers using FOR UPDATE SKIP LOCKED
    // Transaction 1: locks first row
    let mut tx1 = pool.begin().await.expect("begin tx1");
    let rows1: Vec<(Uuid, Vec<u8>)> = sqlx::query_as(
        "SELECT id, payload FROM outbox
         WHERE delivered_at IS NULL
         ORDER BY sequence_number ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED",
    )
    .fetch_all(&mut *tx1)
    .await
    .expect("tx1 poll");

    assert_eq!(rows1.len(), 1, "tx1 should lock 1 row");
    let tx1_id = rows1[0].0;

    // Transaction 2: should skip the locked row and get the second one
    let mut tx2 = pool.begin().await.expect("begin tx2");
    let rows2: Vec<(Uuid, Vec<u8>)> = sqlx::query_as(
        "SELECT id, payload FROM outbox
         WHERE delivered_at IS NULL
         ORDER BY sequence_number ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED",
    )
    .fetch_all(&mut *tx2)
    .await
    .expect("tx2 poll");

    assert_eq!(
        rows2.len(),
        1,
        "tx2 should lock 1 row (skipping the locked one)"
    );
    let tx2_id = rows2[0].0;

    assert_ne!(
        tx1_id, tx2_id,
        "concurrent dispatchers should process different outbox entries via SKIP LOCKED"
    );

    // Clean up: mark both as delivered
    sqlx::query("UPDATE outbox SET delivered_at = now() WHERE id = $1")
        .bind(tx1_id)
        .execute(&mut *tx1)
        .await
        .expect("mark delivered tx1");
    tx1.commit().await.expect("commit tx1");

    sqlx::query("UPDATE outbox SET delivered_at = now() WHERE id = $1")
        .bind(tx2_id)
        .execute(&mut *tx2)
        .await
        .expect("mark delivered tx2");
    tx2.commit().await.expect("commit tx2");
}

// ── Test 10: Projection rebuild checkpoint round-trip on real YugabyteDB ─────
//
// Infrastructure-layer test: exercises the YugabyteProjectionStore checkpoint
// lifecycle (set, reset, rebuilding flag, replay simulation) directly.
// A full e2e version would trigger a rebuild via the ProjectionConsumer, reset
// the Kafka consumer offset, replay events, and verify the projection data
// matches the replayed event stream.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_projection_rebuild_checkpoint() {
    let pool = get_pg_pool().await;

    let projection_store = YugabyteProjectionStore::from_pool(pool.clone());
    let projection_id = format!("rebuild-test-{}", Uuid::new_v4());

    // Set initial checkpoint at version 10
    projection_store
        .update_last_version(&projection_id, Version::from_u64(10))
        .await
        .expect("set initial version");

    // Verify initial state
    let v = projection_store
        .get_last_version(&projection_id)
        .await
        .expect("get version");
    assert_eq!(v.as_u64(), 10);

    // Start rebuild: reset checkpoint to 0, set rebuilding = true
    projection_store
        .reset_checkpoint(&projection_id, Version::initial())
        .await
        .expect("reset checkpoint");

    // Verify rebuilding state
    let rebuilding = projection_store
        .is_rebuilding(&projection_id)
        .await
        .expect("check rebuilding");
    assert!(rebuilding, "projection should be in rebuilding state");

    let v_after_reset = projection_store
        .get_last_version(&projection_id)
        .await
        .expect("get version after reset");
    assert_eq!(
        v_after_reset.as_u64(),
        0,
        "checkpoint should be reset to 0 for rebuild"
    );

    // Simulate replaying events by updating the checkpoint
    for version in 1..=10u64 {
        projection_store
            .update_last_version(&projection_id, Version::from_u64(version))
            .await
            .unwrap_or_else(|e| panic!("update version to {version}: {e}"));
    }

    // End rebuild
    projection_store
        .set_rebuilding(&projection_id, false)
        .await
        .expect("end rebuild");

    // Verify final state
    let final_version = projection_store
        .get_last_version(&projection_id)
        .await
        .expect("get final version");
    assert_eq!(
        final_version.as_u64(),
        10,
        "projection should be back at version 10 after rebuild"
    );

    let still_rebuilding = projection_store
        .is_rebuilding(&projection_id)
        .await
        .expect("check rebuilding after end");
    assert!(
        !still_rebuilding,
        "projection should no longer be rebuilding"
    );
}

// ── Test: Command store round-trip on real YugabyteDB ───────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_command_store_roundtrip() {
    let pool = get_pg_pool().await;
    let command_store = YugabyteCommandStore::new(pool);

    let agg_id = AggregateId::new();
    let cmd = make_command_envelope(&agg_id, "RegisterShip");
    let cmd_id = cmd.command_id;

    command_store
        .append(cmd)
        .await
        .expect("command store append should succeed");

    let loaded = command_store
        .load(cmd_id)
        .await
        .expect("command store load should succeed")
        .expect("command should be found");

    assert_eq!(loaded.command_id, cmd_id);
    assert_eq!(loaded.aggregate_id, agg_id);
    assert_eq!(loaded.command_type, "RegisterShip");
}

// ── Test: Command store idempotency on real YugabyteDB ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_command_store_idempotent() {
    let pool = get_pg_pool().await;
    let command_store = YugabyteCommandStore::new(pool);

    let agg_id = AggregateId::new();
    let cmd = make_command_envelope(&agg_id, "RegisterShip");

    command_store
        .append(cmd.clone())
        .await
        .expect("first append should succeed");

    // Duplicate should succeed silently (ON CONFLICT DO NOTHING)
    command_store
        .append(cmd)
        .await
        .expect("duplicate append should succeed silently");

    let loaded = command_store
        .load_for_aggregate(&agg_id)
        .await
        .expect("load_for_aggregate should succeed");

    assert_eq!(
        loaded.len(),
        1,
        "duplicate command_id should be silently ignored"
    );
}

// ── Test: Retry tracker on real YugabyteDB ──────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_retry_tracker() {
    use canon_core::traits::retry_tracker::RetryTracker;

    let pool = get_pg_pool().await;
    let tracker = YugabyteRetryTracker::new(pool);

    let msg_id = Uuid::new_v4();

    // First increment creates the entry
    let count = tracker
        .increment(msg_id, "test_handler")
        .expect("first increment should succeed");
    assert_eq!(count, 1);

    // Second increment bumps the count
    let count = tracker
        .increment(msg_id, "test_handler")
        .expect("second increment should succeed");
    assert_eq!(count, 2);

    // Get returns the entry
    let attempt = tracker
        .get(msg_id)
        .expect("get should succeed")
        .expect("entry should exist");
    assert_eq!(attempt.attempts, 2);
    assert_eq!(attempt.handler_id, "test_handler");

    // Remove cleans up
    tracker.remove(msg_id).expect("remove should succeed");
    let gone = tracker
        .get(msg_id)
        .expect("get after remove should succeed");
    assert!(gone.is_none(), "entry should be removed");
}

// ── Test: Snapshot store round-trip on real YugabyteDB ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_snapshot_store_roundtrip() {
    use canon_core::Snapshot;

    let pool = get_pg_pool().await;
    let snapshot_store = YugabyteSnapshotStore::new(pool);

    let agg_id = AggregateId::new();
    let snapshot = Snapshot {
        aggregate_id: agg_id.clone(),
        version: Version::from_u64(42),
        state: Bytes::from_static(b"serialized-aggregate-state"),
        taken_at: Utc::now(),
    };

    snapshot_store
        .save(snapshot)
        .await
        .expect("save snapshot should succeed");

    let loaded = snapshot_store
        .load(&agg_id)
        .await
        .expect("load snapshot should succeed")
        .expect("snapshot should exist");

    assert_eq!(loaded.version.as_u64(), 42);
    assert_eq!(loaded.state.as_ref(), b"serialized-aggregate-state");
}

// ── Test: LWT column order validation on ScyllaDB ───────────────────────────
//
// Validates that the LWT (Lightweight Transaction) response from ScyllaDB
// has the expected column order. ScyllaDB returns all table columns in the
// LWT result with `[applied]` first. The column order is alphabetical by
// column name (Cassandra's internal ordering), not by CREATE TABLE order.
//
// This test inserts the same event twice (causing a LWT conflict) and
// verifies the conflict is correctly detected with accurate version numbers,
// which exercises the LWT column parsing code path.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_cassandra_lwt_column_order_on_conflict() {
    let scylla = get_scylla().await;
    let store = CassandraEventStore::new(&scylla.nodes)
        .await
        .expect("failed to create CassandraEventStore");

    let agg_id = AggregateId::new();

    // First insert succeeds
    store
        .append(
            &agg_id,
            Version::initial(),
            vec![make_event_envelope(&agg_id, 1, "TestEvent")],
        )
        .await
        .expect("first append should succeed");

    // Second insert at same version triggers LWT conflict — this exercises
    // the LWT response parsing with the full 10-column tuple.
    let result = store
        .append(
            &agg_id,
            Version::initial(),
            vec![make_event_envelope(&agg_id, 1, "TestEvent")],
        )
        .await;

    match result {
        Err(EventStoreError::VersionConflict { expected, found }) => {
            assert_eq!(
                expected.as_u64(),
                0,
                "expected version should be 0 (initial)"
            );
            assert_eq!(
                found.as_u64(),
                1,
                "found version should be 1 (next after initial)"
            );
        }
        other => panic!("expected VersionConflict, got: {other:?}"),
    }
}
