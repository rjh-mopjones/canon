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
pub async fn bootstrap_session(station_pool: &PgPool, fleet_pool: &PgPool) -> SessionIds {
    let session_id = Uuid::new_v4();
    let ship_id = Uuid::new_v4();
    let mut station_ids = [Uuid::nil(); 4];

    // Register stations
    for (i, bs) in BOOTSTRAP_STATIONS.iter().enumerate() {
        let agg_id = Uuid::new_v4();
        station_ids[i] = agg_id;

        #[derive(serde::Serialize)]
        struct RegisterPayload {
            name: String,
            capacity_kg: f32,
        }
        let payload = RegisterPayload {
            name: bs.name.to_owned(),
            capacity_kg: bs.capacity_kg as f32,
        };
        let corr_id = Uuid::new_v4();

        match command::build_envelope("RegisterStation", Some(agg_id), corr_id, &payload) {
            Ok(envelope) => {
                if let Err(e) = command::submit_command(station_pool, "Station", &envelope).await {
                    warn!(error = %e, station = bs.name, "session bootstrap: RegisterStation failed");
                }
            }
            Err(e) => {
                warn!(error = %e, station = bs.name, "session bootstrap: failed to build RegisterStation")
            }
        }
    }

    // Wait briefly for registrations to process before seeding stock
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Seed initial stock
    for (i, bs) in BOOTSTRAP_STATIONS.iter().enumerate() {
        let agg_id = station_ids[i];
        let initial_kg = (bs.capacity_kg * bs.initial_stock_pct / 100.0) as f32;

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

        match command::build_envelope("RecordCargoReceived", Some(agg_id), corr_id, &payload) {
            Ok(envelope) => {
                if let Err(e) = command::submit_command(station_pool, "Station", &envelope).await {
                    warn!(error = %e, station = bs.name, "session bootstrap: RecordCargoReceived failed");
                }
            }
            Err(e) => {
                warn!(error = %e, station = bs.name, "session bootstrap: failed to build RecordCargoReceived")
            }
        }
    }

    // Register ship without a home station — ship starts undocked in the
    // center of the map. The user chooses where to fly it.
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
            if let Err(e) = command::submit_command(fleet_pool, "Ship", &envelope).await {
                warn!(error = %e, "session bootstrap: RegisterShip failed");
            }
        }
        Err(e) => warn!(error = %e, "session bootstrap: failed to build RegisterShip"),
    }

    info!(session_id = %session_id, ship_id = %ship_id, "session bootstrapped");

    SessionIds {
        session_id,
        ship_id,
        station_ids,
    }
}

/// Spawn a per-session stock drain task. Returns the JoinHandle for cancellation.
pub fn spawn_session_drain(station_pool: PgPool, station_ids: [Uuid; 4]) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Startup delay: let bootstrap commands flow through the pipeline.
        // The bootstrap_session() already waits 2s for registrations before
        // seeding stock, so 5s total gives the outbox → outbound → event store
        // chain enough time to process before we start draining.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Each station drains once every 10s, staggered 2.5s apart.
        let stagger_ms = 10_000 / station_ids.len() as u64; // 2500ms per station
        loop {
            for (i, station_id) in station_ids.iter().enumerate() {
                tokio::time::sleep(std::time::Duration::from_millis(stagger_ms)).await;

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
                    tracing::debug!(error = %e, "DrainStock rejected (expected for depleted stations)");
                }
            }
        }
    })
}
