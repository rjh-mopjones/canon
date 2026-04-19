//! Per-session game state management.
//!
//! Each browser tab creates a session via `POST /sessions`. The session
//! holds unique aggregate IDs (1 ship + 4 stations), an in-memory game
//! projection maintained by Kafka consumers, and a stock drain task handle.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::command;
use crate::projection::GameProjection;

/// Aggregate IDs belonging to a single game session.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionIds {
    pub session_id: Uuid,
    pub ship_id: Uuid,
    /// Station IDs in canonical order: [Alpha Depot, Beta Relay, Gamma Outpost, Delta Prime].
    pub station_ids: [Uuid; 4],
}

impl SessionIds {
    /// Returns all aggregate IDs belonging to this session.
    pub fn aggregate_id_set(&self) -> HashSet<Uuid> {
        let mut set = HashSet::with_capacity(5);
        set.insert(self.ship_id);
        for sid in &self.station_ids {
            set.insert(*sid);
        }
        set
    }
}

/// A live session with its in-memory projection and drain task.
pub struct LiveSession {
    pub ids: SessionIds,
    pub drain_handle: Option<JoinHandle<()>>,
    /// In-memory game projection, incrementally updated by Kafka consumers.
    pub projection: Arc<RwLock<GameProjection>>,
    /// Epoch millis of the last poll (for session reaping).
    pub last_polled_at: AtomicU64,
    /// Mirror of projection.game_over for lock-free reaping checks.
    pub game_over: AtomicBool,
}

/// Thread-safe store of active sessions.
pub type SessionStore = Arc<RwLock<HashMap<Uuid, LiveSession>>>;

/// Create a new SessionStore.
pub fn new_session_store() -> SessionStore {
    Arc::new(RwLock::new(HashMap::new()))
}

fn epoch_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Spawn a background task that reaps idle sessions.
/// - game_over sessions: reaped after 30s of no polls
/// - active sessions: reaped after 60s of no polls
pub fn spawn_session_reaper(sessions: SessionStore) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let now = epoch_millis_now();
            let mut store = sessions.write().await;
            store.retain(|id, session| {
                let last_poll = session
                    .last_polled_at
                    .load(std::sync::atomic::Ordering::Relaxed);
                let idle_ms = now.saturating_sub(last_poll);
                let game_over = session.game_over.load(std::sync::atomic::Ordering::Relaxed);

                let max_idle_ms = if game_over { 30_000 } else { 60_000 };
                if idle_ms > max_idle_ms {
                    if let Some(handle) = &session.drain_handle {
                        handle.abort();
                    }
                    info!(session_id = %id, idle_ms, game_over, "reaping idle session");
                    return false;
                }
                true
            });
        }
    });
}

// ── Bootstrap configuration (same as CLAUDE.md spec) ────────────────────

pub struct BootstrapStation {
    pub name: &'static str,
    pub capacity_kg: f64,
    pub initial_stock_pct: f64,
}

pub const BOOTSTRAP_STATIONS: &[BootstrapStation] = &[
    BootstrapStation {
        name: "Alpha Depot",
        capacity_kg: 5000.0,
        initial_stock_pct: 85.0,
    },
    BootstrapStation {
        name: "Beta Relay",
        capacity_kg: 3000.0,
        initial_stock_pct: 60.0,
    },
    BootstrapStation {
        name: "Gamma Outpost",
        capacity_kg: 2000.0,
        initial_stock_pct: 40.0,
    },
    BootstrapStation {
        name: "Delta Prime",
        capacity_kg: 4000.0,
        initial_stock_pct: 75.0,
    },
];

/// Drain rate configuration per station.
#[allow(dead_code)]
pub struct StationDrainConfig {
    pub name: &'static str,
    pub drain_rate_pct: f64,
    pub capacity_kg: f64,
}

pub const STATION_DRAIN_CONFIGS: &[StationDrainConfig] = &[
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

/// Bootstrap a new game session: register stations with stock + register ship.
/// Returns the SessionIds. Commands are submitted to the pipeline.
///
/// Commands are submitted concurrently within each phase (station registration,
/// stock seeding) and the ship registration runs in parallel with the station
/// phase. Kafka partitioning by aggregate_id preserves order per aggregate, so
/// a station's RegisterStation will always be applied before its
/// RecordCargoReceived even when both are submitted back-to-back.
pub async fn bootstrap_session(station_pool: &PgPool, fleet_pool: &PgPool) -> SessionIds {
    let session_id = Uuid::new_v4();
    let ship_id = Uuid::new_v4();
    let mut station_ids = [Uuid::nil(); 4];
    for sid in station_ids.iter_mut() {
        *sid = Uuid::new_v4();
    }

    // Phase 1: register all stations in parallel
    let register_tasks = BOOTSTRAP_STATIONS
        .iter()
        .enumerate()
        .map(|(i, bs)| {
            let agg_id = station_ids[i];
            let pool = station_pool.clone();
            let name = bs.name.to_owned();
            let capacity_kg = bs.capacity_kg as f32;
            async move {
                #[derive(serde::Serialize)]
                struct RegisterPayload {
                    name: String,
                    capacity_kg: f32,
                }
                let payload = RegisterPayload {
                    name: name.clone(),
                    capacity_kg,
                };
                let corr_id = Uuid::new_v4();
                match command::build_envelope("RegisterStation", Some(agg_id), corr_id, &payload) {
                    Ok(envelope) => {
                        if let Err(e) =
                            command::submit_command(&pool, "Station", &envelope).await
                        {
                            warn!(error = %e, station = %name, "session bootstrap: RegisterStation failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, station = %name, "session bootstrap: failed to build RegisterStation")
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    // Phase 2: seed initial stock in parallel — safe to fire immediately because
    // Kafka partitions by aggregate_id so RegisterStation will be applied before
    // RecordCargoReceived for the same station.
    let stock_tasks = BOOTSTRAP_STATIONS
        .iter()
        .enumerate()
        .map(|(i, bs)| {
            let agg_id = station_ids[i];
            let pool = station_pool.clone();
            let name = bs.name.to_owned();
            let initial_kg = (bs.capacity_kg * bs.initial_stock_pct / 100.0) as f32;
            async move {
                #[derive(serde::Serialize)]
                struct CargoPayload {
                    station_id: Uuid,
                    manifest_id: Uuid,
                    weight_kg: f32,
                }
                let payload = CargoPayload {
                    station_id: agg_id,
                    manifest_id: Uuid::new_v4(),
                    weight_kg: initial_kg,
                };
                let corr_id = Uuid::new_v4();
                match command::build_envelope(
                    "RecordCargoReceived",
                    Some(agg_id),
                    corr_id,
                    &payload,
                ) {
                    Ok(envelope) => {
                        if let Err(e) =
                            command::submit_command(&pool, "Station", &envelope).await
                        {
                            warn!(error = %e, station = %name, "session bootstrap: RecordCargoReceived failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, station = %name, "session bootstrap: failed to build RecordCargoReceived")
                    }
                }
            }
        })
        .collect::<Vec<_>>();

    // Phase 3: register ship (independent aggregate, separate pool).
    let ship_task = {
        let pool = fleet_pool.clone();
        async move {
            #[derive(serde::Serialize)]
            struct ShipPayload {
                name: String,
                capacity_kg: f32,
                home_station: Option<Uuid>,
            }
            let payload = ShipPayload {
                name: "VSS Meridian".to_owned(),
                capacity_kg: 5000.0,
                home_station: None,
            };
            let corr_id = Uuid::new_v4();
            match command::build_envelope("RegisterShip", Some(ship_id), corr_id, &payload) {
                Ok(envelope) => {
                    if let Err(e) = command::submit_command(&pool, "Ship", &envelope).await {
                        warn!(error = %e, "session bootstrap: RegisterShip failed");
                    }
                }
                Err(e) => warn!(error = %e, "session bootstrap: failed to build RegisterShip"),
            }
        }
    };

    // Phase 1: register stations (parallel) + ship (independent aggregate,
    // runs alongside). All four stations fire at once, not sequentially.
    tokio::join!(futures::future::join_all(register_tasks), ship_task);

    // Wait for the station service to apply RegisterStation before seeding
    // stock. Without this, RecordCargoReceived races ahead of RegisterStation
    // in the station dispatcher and dead letters as "station not registered",
    // which leaves `current_stock_kg` at 0 so the projection never reaches
    // `ready: true` and the frontend gets stuck on the loading screen.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Phase 2: seed initial stock. Every station aggregate is now registered
    // so these commands succeed.
    futures::future::join_all(stock_tasks).await;

    info!(session_id = %session_id, ship_id = %ship_id, "session bootstrapped");

    SessionIds {
        session_id,
        ship_id,
        station_ids,
    }
}

/// Spawn a per-session stock drain task. Returns the JoinHandle for cancellation.
///
/// Reads the live projection before each drain to skip stations whose stock
/// has already hit zero, and stops submitting once any station is game_over.
/// Previously the loop fired `DrainStock` blindly, producing thousands of
/// `StockDepleted` dead letters per session over its lifetime.
pub fn spawn_session_drain(
    station_pool: PgPool,
    station_ids: [Uuid; 4],
    projection: Arc<RwLock<GameProjection>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Startup delay: let bootstrap commands flow through the pipeline.
        // This gives the outbox → outbound → event store chain enough time to
        // process registration and stock seed commands before draining starts.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Each station drains once every 10s, staggered 2.5s apart.
        let stagger_ms = 10_000 / station_ids.len() as u64; // 2500ms per station
        loop {
            for (i, station_id) in station_ids.iter().enumerate() {
                tokio::time::sleep(std::time::Duration::from_millis(stagger_ms)).await;

                // Skip any drain work once the session has gone game_over or
                // this station is already depleted. The aggregate would reject
                // a depleted-stock drain and dead letter after 3 retries.
                let skip = {
                    let proj = projection.read().await;
                    proj.game_over || proj.stations[i].current_stock_kg <= 0.0
                };
                if skip {
                    continue;
                }

                let drain_cfg = &STATION_DRAIN_CONFIGS[i];
                // Random drain between 1% and 5% of capacity per tick
                use rand::Rng;
                let pct = rand::thread_rng().gen_range(1.0_f64..=5.0);
                let drain_kg = (drain_cfg.capacity_kg * pct / 100.0) as f32;

                #[derive(serde::Serialize)]
                struct DrainPayload {
                    station_id: Uuid,
                    drain_kg: f32,
                }
                let payload = DrainPayload {
                    station_id: *station_id,
                    drain_kg,
                };
                let corr_id = Uuid::new_v4();

                let envelope = match command::build_envelope(
                    "DrainStock",
                    Some(*station_id),
                    corr_id,
                    &payload,
                ) {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                if let Err(e) = command::submit_command(&station_pool, "Station", &envelope).await {
                    tracing::debug!(error = %e, "DrainStock rejected");
                }
            }
        }
    })
}
