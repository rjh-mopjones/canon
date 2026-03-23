mod command;
mod correlation;
mod error;
mod kafka;
mod routes;
mod state;
mod types;

use std::collections::HashMap;
use std::sync::Arc;

use canon_event_store_cassandra::CassandraEventStore;
use canon_snapshot_store_yugabyte::YugabyteSnapshotStore;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use crate::state::{AppState, ServiceStores};

/// Service names and their corresponding YugabyteDB schema names.
const SERVICES: &[(&str, &str)] = &[
    ("fleet", "canon_fleet"),
    ("cargo", "canon_cargo"),
    ("navigation", "canon_navigation"),
    ("supply", "canon_supply"),
    ("station", "canon_station"),
];

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
    // Each service gets its own YugabyteDB schema and Cassandra keyspace to
    // ensure complete domain isolation. The gateway needs to write commands to
    // the correct service's inbox and read from the correct event store.
    let mut service_stores = HashMap::new();

    for &(service_name, schema_name) in SERVICES {
        info!(
            service = service_name,
            schema = schema_name,
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
    // ── Broadcast channel for WebSocket events ──────────────────────────────
    let (event_tx, _) = broadcast::channel::<String>(1024);

    // ── Kafka consumers → broadcast ─────────────────────────────────────────
    info!("starting Kafka consumers for {kafka_brokers}");
    kafka::spawn_kafka_consumers(&kafka_brokers, event_tx.clone());

    // ── InfraStatus broadcaster (every 10s) ─────────────────────────────────
    kafka::spawn_infra_status_broadcaster(
        event_tx.clone(),
        yugabyte_pool.clone(),
        cassandra_nodes,
        kafka_brokers,
    );

    // ── Stock drain background task (every 3s) ───────────────────────────
    // Sends DrainStock commands through the Canon pipeline for each registered
    // station, replacing the previous client-side gloo_timers::Interval.
    let station_pool_drain = service_stores
        .get("station")
        .map(|s| s.pool.clone())
        .ok_or("station service stores not initialized")?;
    spawn_stock_drain_task(station_pool_drain);

    // ── Application state ───────────────────────────────────────────────────
    let state = AppState {
        event_tx,
        service_stores,
        yugabyte_pool,
        event_store,
        snapshot_store,
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

// ── Stock drain background task ──────────────────────────────────────────

/// Station drain rate configuration. The drain amount is expressed as a fraction
/// of capacity applied per 3-second tick — matching the CLAUDE.md spec.
///
/// Drain rates per 3s tick: Alpha 0.15%, Beta 0.20%, Gamma 0.25%, Delta 0.18%
/// of capacity expressed as kg per tick (capacity * rate / 100).
struct StationDrainConfig {
    name: &'static str,
    /// Drain rate as percentage of capacity per 3s tick.
    drain_rate_pct: f64,
    /// Station capacity in kg.
    capacity_kg: f64,
}

const STATION_DRAIN_CONFIGS: &[StationDrainConfig] = &[
    StationDrainConfig {
        name: "Alpha Depot",
        drain_rate_pct: 0.15,
        capacity_kg: 5000.0,
    },
    StationDrainConfig {
        name: "Beta Relay",
        drain_rate_pct: 0.20,
        capacity_kg: 3000.0,
    },
    StationDrainConfig {
        name: "Gamma Outpost",
        drain_rate_pct: 0.25,
        capacity_kg: 2000.0,
    },
    StationDrainConfig {
        name: "Delta Prime",
        drain_rate_pct: 0.18,
        capacity_kg: 4000.0,
    },
];

/// Spawn a background task that sends `DrainStock` commands every 3 seconds
/// for each registered station. This replaces the frontend's client-side drain
/// timer, ensuring stock changes flow through the Canon pipeline.
fn spawn_stock_drain_task(station_pool: PgPool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));

        // Small startup delay so stations have time to register.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        info!("stock drain background task started");

        loop {
            interval.tick().await;

            // Query registered station aggregate IDs by looking up RegisterStation commands.
            let rows: Vec<(uuid::Uuid, String)> = match sqlx::query_as(
                "SELECT c.aggregate_id, \
                 COALESCE((c.payload::json->>'name')::text, '') as name \
                 FROM commands c \
                 WHERE c.command_type = 'RegisterStation' \
                 ORDER BY c.created_at",
            )
            .fetch_all(&station_pool)
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "failed to query stations for drain task");
                    continue;
                }
            };

            for (agg_id, station_name) in &rows {
                // Find drain config by station name.
                let drain_kg = STATION_DRAIN_CONFIGS
                    .iter()
                    .find(|c| c.name == station_name.as_str())
                    .map(|c| (c.capacity_kg * c.drain_rate_pct / 100.0) as f32)
                    .unwrap_or(0.0);

                if drain_kg <= 0.0 {
                    continue;
                }

                #[derive(serde::Serialize)]
                struct DrainPayload {
                    station_id: uuid::Uuid,
                    drain_kg: f32,
                }

                let payload = DrainPayload {
                    station_id: *agg_id,
                    drain_kg,
                };

                let correlation_id = uuid::Uuid::new_v4();
                let envelope = match command::build_envelope(
                    "DrainStock",
                    Some(*agg_id),
                    correlation_id,
                    &payload,
                ) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(error = %e, station = %station_name, "failed to build DrainStock envelope");
                        continue;
                    }
                };

                if let Err(e) = command::submit_command(&station_pool, "Station", &envelope).await {
                    // Expected errors include StockDepleted (station at 0) or
                    // station not registered yet — log at debug level.
                    tracing::debug!(
                        error = %e,
                        station = %station_name,
                        "DrainStock command rejected (expected for depleted stations)"
                    );
                }
            }
        }
    });
}
