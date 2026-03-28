mod command;
mod correlation;
mod error;
mod kafka;
mod middleware;
pub mod projection;
mod routes;
pub mod session;
mod state;
mod types;

use std::collections::HashMap;
use std::sync::Arc;

use canon_event_store_cassandra::CassandraEventStore;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::state::{AppState, ServiceStores};

/// Build the service-to-schema mapping, respecting SCHEMA_PREFIX env var.
fn service_schemas() -> Vec<(&'static str, String)> {
    let prefix = std::env::var("SCHEMA_PREFIX").unwrap_or_else(|_| "canon".to_string());
    vec![
        ("fleet", format!("{prefix}_fleet")),
        ("cargo", format!("{prefix}_cargo")),
        ("navigation", format!("{prefix}_navigation")),
        ("supply", format!("{prefix}_supply")),
        ("station", format!("{prefix}_station")),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gateway=info,tower_http=info".into()),
        )
        .init();

    // ── Environment variables ───────────────────────────────────────────────
    let yugabyte_url = std::env::var("YUGABYTE_URL")
        .unwrap_or_else(|_| "postgres://canon:canon@localhost:5433/canon".to_owned());
    let cassandra_nodes =
        std::env::var("CASSANDRA_NODES").unwrap_or_else(|_| "localhost:9042".to_owned());
    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let cors_origin =
        std::env::var("CORS_ORIGIN").unwrap_or_else(|_| "http://localhost:3000".to_owned());
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_owned());

    // ── Per-service infrastructure connections ───────────────────────────────
    let mut service_stores = HashMap::new();

    let services = service_schemas();
    for &(service_name, ref schema_name) in &services {
        info!(
            service = service_name,
            schema = schema_name.as_str(),
            "connecting to YugabyteDB"
        );
        let pool = canon_demo_shared::db::create_service_pool(&yugabyte_url, schema_name).await?;

        info!(
            service = service_name,
            keyspace = schema_name,
            "connecting to Cassandra"
        );
        let event_store =
            Arc::new(CassandraEventStore::new_with_keyspace(&cassandra_nodes, schema_name).await?);

        let snapshot_store = Arc::new(YugabyteSnapshotStore::new(pool.clone()));

        service_stores.insert(
            service_name.to_owned(),
            ServiceStores {
                pool,
                event_store,
                snapshot_store,
            },
        );
    }

    info!("all per-service pools and event stores created");

    // ── Convenience references for backwards-compatible code paths ────────
    let fleet_stores = service_stores
        .get("fleet")
        .ok_or("fleet service stores not initialized")?;
    let yugabyte_pool = fleet_stores.pool.clone();
    let event_store = fleet_stores.event_store.clone();
    let snapshot_store = fleet_stores.snapshot_store.clone();

    // ── Session store ──────────────────────────────────────────────────────
    let sessions = session::new_session_store();

    // ── Kafka consumers → apply events to session projections ──────────────
    let topic_prefix = std::env::var("TOPIC_PREFIX").unwrap_or_else(|_| "canon".to_string());
    info!("starting Kafka consumers for {kafka_brokers} (topic_prefix: {topic_prefix})");
    kafka::spawn_kafka_consumers(
        &kafka_brokers,
        sessions.clone(),
        yugabyte_pool.clone(),
        &topic_prefix,
    );

    // ── Shared infra status ────────────────────────────────────────────────
    let infra_status = Arc::new(tokio::sync::RwLock::new(state::InfraStatus::default()));

    // ── InfraStatus broadcaster (every 10s) ─────────────────────────────────
    kafka::spawn_infra_status_broadcaster(
        yugabyte_pool.clone(),
        cassandra_nodes,
        kafka_brokers,
        infra_status.clone(),
    );

    // ── Session reaper ─────────────────────────────────────────────────────
    session::spawn_session_reaper(sessions.clone());

    // ── Application state ───────────────────────────────────────────────────
    let state = AppState {
        service_stores,
        yugabyte_pool,
        event_store,
        snapshot_store,
        sessions,
        infra_status,
    };

    // ── CORS ────────────────────────────────────────────────────────────────
    let cors = CorsLayer::new()
        .allow_origin(
            cors_origin
                .parse::<axum::http::HeaderValue>()
                .unwrap_or_else(|_| axum::http::HeaderValue::from_static("http://localhost:3000")),
        )
        .allow_methods(Any)
        .allow_headers(Any);

    // ── Router ──────────────────────────────────────────────────────────────
    let app = routes::build_router(state).layer(cors);

    // ── Start server ────────────────────────────────────────────────────────
    let listener = tokio::net::TcpListener::bind(&listen_addr).await?;
    info!("gateway listening on {listen_addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
