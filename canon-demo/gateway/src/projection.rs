//! In-memory game projection maintained per session.
//!
//! The Kafka consumer applies events directly to each session's projection,
//! eliminating the need for DB queries on the hot path. The `GET /game/:session_id`
//! endpoint reads the cached projection (sub-ms) instead of rebuilding from DB.

use std::collections::{HashSet, VecDeque};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use canon_core::EventEnvelope;

use crate::session::{BootstrapStation, SessionIds};
use crate::state::InfraStatus;
use crate::types::{
    GameCargoResponse, GameEventResponse, GameStateResponse, GameStationResponse,
    InfraStatusResponse, OversightWindowResponse, ShipStateResponse,
};

/// Maximum number of events kept in the ring buffer.
const MAX_EVENTS: usize = 100;

// ── Projected sub-state structs ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ProjectedShip {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub station_id: Option<Uuid>,
    pub route_label: String,
    pub fuel_level: f32,
    pub capacity: f32,
    pub aggregate_version: u64,
    pub last_snapshot_version: u64,
    pub correlation_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct ProjectedStation {
    pub id: Uuid,
    pub name: String,
    pub capacity_kg: f32,
    pub current_stock_kg: f32,
}

impl ProjectedStation {
    pub fn stock_pct(&self) -> f64 {
        if self.capacity_kg > 0.0 {
            (self.current_stock_kg as f64 / self.capacity_kg as f64 * 100.0).clamp(0.0, 100.0)
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProjectedCargo {
    pub manifest_id: Uuid,
    pub voyage_id: Uuid,
    pub status: String,
    pub closed: bool,
}

// ── GameProjection ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct GameProjection {
    pub session_ids: SessionIds,
    pub ship: Option<ProjectedShip>,
    pub stations: [ProjectedStation; 4],
    pub cargo: Option<ProjectedCargo>,
    /// Oversight is queried from DB on each poll (inbox_windows not in Kafka).
    pub oversight: Option<OversightWindowResponse>,
    /// Ring buffer of recent events (newest first), capped at MAX_EVENTS.
    pub events: VecDeque<EventLogEntry>,
    /// Dedup set for event IDs in the ring buffer.
    event_id_set: HashSet<Uuid>,
    /// Total events seen for this session (for lazy pagination).
    pub event_count: u64,
    /// All aggregate IDs this session tracks (base + discovered).
    pub tracked_ids: HashSet<Uuid>,
    /// Game over when any station hits 0%.
    pub game_over: bool,
}

#[derive(Debug, Clone)]
pub struct EventLogEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
    pub service: String,
    pub event_name: String,
    pub aggregate_id: Uuid,
    pub correlation_id: Uuid,
}

impl GameProjection {
    /// Create a projection seeded with bootstrap values.
    ///
    /// Because the Kafka consumer persists offsets, it won't replay events
    /// that occurred before the session was created. We seed the projection
    /// with the known bootstrap values (station names, capacities, initial
    /// stock, ship name) so the projection starts correct.
    pub fn seeded(ids: SessionIds, bootstrap: &[BootstrapStation]) -> Self {
        let tracked_ids = ids.aggregate_id_set();

        let stations = std::array::from_fn(|i| {
            let bs = &bootstrap[i];
            let capacity_kg = bs.capacity_kg as f32;
            let current_stock_kg = capacity_kg * (bs.initial_stock_pct as f32 / 100.0);
            ProjectedStation {
                id: ids.station_ids[i],
                name: bs.name.to_owned(),
                capacity_kg,
                current_stock_kg,
            }
        });

        let ship = Some(ProjectedShip {
            id: ids.ship_id,
            name: "VSS Meridian".to_owned(),
            status: "docked".to_owned(),
            station_id: None,
            route_label: String::new(),
            fuel_level: 5000.0,
            capacity: 5000.0,
            aggregate_version: 0,
            last_snapshot_version: 0,
            correlation_id: Uuid::nil(),
        });

        Self {
            session_ids: ids,
            ship,
            stations,
            cargo: None,
            oversight: None,
            events: VecDeque::with_capacity(MAX_EVENTS),
            event_id_set: HashSet::with_capacity(MAX_EVENTS),
            event_count: 0,
            tracked_ids,
            game_over: false,
        }
    }

    /// Check whether this projection cares about the given aggregate.
    pub fn tracks_aggregate(&self, aggregate_id: Uuid, related_ids: &[Uuid]) -> bool {
        self.tracked_ids.contains(&aggregate_id)
            || related_ids.iter().any(|id| self.tracked_ids.contains(id))
    }

    /// Apply a single event to the projection, updating state incrementally.
    pub fn apply_event(&mut self, service: &str, envelope: &EventEnvelope) {
        let agg_id = *envelope.aggregate_id.as_uuid();

        match service {
            "fleet" => self.apply_fleet_event(envelope),
            "station" => self.apply_station_event(agg_id, envelope),
            "cargo" => self.apply_cargo_event(agg_id, envelope),
            "navigation" => self.apply_navigation_event(agg_id, envelope),
            "supply" => self.apply_supply_event(agg_id, envelope),
            _ => {}
        }

        // Push to event ring buffer with dedup
        self.push_event(service, envelope);
    }

    /// Convert projection state to the API response type.
    pub fn to_game_state_response(&self, infra: &InfraStatus) -> GameStateResponse {
        let ship = self.ship.as_ref().map(|s| {
            let fuel_pct = if s.capacity > 0.0 {
                ((s.fuel_level / s.capacity) * 100.0).clamp(0.0, 100.0) as u32
            } else {
                0
            };
            ShipStateResponse {
                id: s.id,
                name: s.name.clone(),
                status: s.status.clone(),
                station_id: s.station_id,
                route_label: s.route_label.clone(),
                fuel_pct,
                aggregate_version: s.aggregate_version,
                last_snapshot_version: s.last_snapshot_version,
                correlation_id: s.correlation_id,
            }
        });

        let stations = self
            .stations
            .iter()
            .map(|s| {
                let stock_pct = s.stock_pct();
                GameStationResponse {
                    id: s.id,
                    name: s.name.clone(),
                    stock_pct,
                    capacity_kg: s.capacity_kg,
                    stock_low: stock_pct < 20.0,
                }
            })
            .collect();

        let cargo = self.cargo.as_ref().and_then(|c| {
            if c.closed {
                return None;
            }
            let destination_station_id = self.cargo_destination();
            Some(GameCargoResponse {
                manifest_id: c.manifest_id,
                voyage_id: c.voyage_id,
                destination_station_id,
                amount_pct: 35,
                status: c.status.clone(),
            })
        });

        let events = self
            .events
            .iter()
            .map(|e| GameEventResponse {
                id: e.id,
                timestamp: e.timestamp.to_rfc3339(),
                version: e.version,
                service: e.service.clone(),
                event_name: e.event_name.clone(),
                aggregate_id: e.aggregate_id,
                correlation_id: e.correlation_id,
            })
            .collect();

        GameStateResponse {
            ship,
            stations,
            cargo,
            oversight: self.oversight.clone(),
            events,
            game_over: self.game_over,
            infra: InfraStatusResponse {
                kafka: infra.kafka,
                yugabyte: infra.yugabyte,
                cassandra: infra.cassandra,
            },
        }
    }

    // ── Fleet events ─────────────────────────────────────────────────────────

    fn apply_fleet_event(&mut self, envelope: &EventEnvelope) {
        let agg_id = *envelope.aggregate_id.as_uuid();
        if agg_id != self.session_ids.ship_id {
            return;
        }

        match envelope.event_type.as_str() {
            "ShipRegistered" => {
                #[derive(serde::Deserialize)]
                struct E {
                    name: String,
                    capacity_kg: f32,
                    #[serde(default)]
                    home_station: Option<Uuid>,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    self.ship = Some(ProjectedShip {
                        id: agg_id,
                        name: e.name,
                        status: "docked".to_owned(),
                        station_id: e.home_station,
                        route_label: String::new(),
                        fuel_level: e.capacity_kg,
                        capacity: e.capacity_kg,
                        aggregate_version: envelope.version.as_u64(),
                        last_snapshot_version: 0,
                        correlation_id: envelope.correlation_id,
                    });
                }
            }
            "RouteAssigned" => {
                #[derive(serde::Deserialize)]
                struct E {
                    route_id: Uuid,
                }
                if let Some(ship) = &mut self.ship {
                    if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                        ship.route_label = e.route_id.to_string();
                        ship.aggregate_version = envelope.version.as_u64();
                        ship.correlation_id = envelope.correlation_id;
                    }
                }
            }
            "ShipDeparted" => {
                #[derive(serde::Deserialize)]
                struct E {
                    destination: Uuid,
                    fuel_at_departure: f32,
                }
                if let Some(ship) = &mut self.ship {
                    if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                        ship.status = "transit".to_owned();
                        ship.station_id = Some(e.destination);
                        ship.fuel_level -= e.fuel_at_departure * 0.1;
                        ship.aggregate_version = envelope.version.as_u64();
                        ship.correlation_id = envelope.correlation_id;
                    }
                }
            }
            "ShipDockedAtStation" => {
                #[derive(serde::Deserialize)]
                struct E {
                    station_id: Uuid,
                }
                if let Some(ship) = &mut self.ship {
                    if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                        ship.status = "docked".to_owned();
                        ship.station_id = Some(e.station_id);
                        ship.aggregate_version = envelope.version.as_u64();
                        ship.correlation_id = envelope.correlation_id;
                    }
                }
            }
            "ResupplyScheduled" => {
                #[derive(serde::Deserialize)]
                struct E {
                    fuel_kg: f32,
                }
                if let Some(ship) = &mut self.ship {
                    if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                        ship.fuel_level = (ship.fuel_level + e.fuel_kg).min(ship.capacity);
                        ship.aggregate_version = envelope.version.as_u64();
                        ship.correlation_id = envelope.correlation_id;
                    }
                }
            }
            "ShipDecommissioned" => {
                if let Some(ship) = &mut self.ship {
                    ship.status = "dead".to_owned();
                    ship.aggregate_version = envelope.version.as_u64();
                    ship.correlation_id = envelope.correlation_id;
                }
            }
            _ => {}
        }
    }

    // ── Station events ───────────────────────────────────────────────────────

    fn apply_station_event(&mut self, agg_id: Uuid, envelope: &EventEnvelope) {
        let station_idx = match self
            .session_ids
            .station_ids
            .iter()
            .position(|id| *id == agg_id)
        {
            Some(idx) => idx,
            None => return,
        };

        match envelope.event_type.as_str() {
            "StationRegistered" => {
                #[derive(serde::Deserialize)]
                struct E {
                    name: String,
                    capacity_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    self.stations[station_idx].name = e.name;
                    self.stations[station_idx].capacity_kg = e.capacity_kg;
                }
            }
            "CargoReceived" => {
                #[derive(serde::Deserialize)]
                struct E {
                    weight_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    let station = &mut self.stations[station_idx];
                    station.current_stock_kg =
                        (station.current_stock_kg + e.weight_kg).min(station.capacity_kg);
                }
            }
            "CapacityUpdated" => {
                #[derive(serde::Deserialize)]
                struct E {
                    capacity_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    self.stations[station_idx].capacity_kg = e.capacity_kg;
                }
            }
            "StockDrained" => {
                #[derive(serde::Deserialize)]
                struct E {
                    remaining_kg: f32,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    self.stations[station_idx].current_stock_kg = e.remaining_kg;
                }
            }
            "StationOffline" => {
                self.stations[station_idx].current_stock_kg = 0.0;
            }
            _ => {}
        }

        // Check game_over after any station event
        self.game_over = self
            .stations
            .iter()
            .any(|s| s.stock_pct() <= 0.0 && s.capacity_kg > 0.0);
    }

    // ── Cargo events ─────────────────────────────────────────────────────────

    fn apply_cargo_event(&mut self, agg_id: Uuid, envelope: &EventEnvelope) {
        match envelope.event_type.as_str() {
            "ManifestCreated" => {
                #[derive(serde::Deserialize)]
                struct E {
                    ship_id: Uuid,
                    #[serde(default)]
                    voyage_id: Option<Uuid>,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    if e.ship_id == self.session_ids.ship_id {
                        self.tracked_ids.insert(agg_id);
                        self.cargo = Some(ProjectedCargo {
                            manifest_id: agg_id,
                            voyage_id: e.voyage_id.unwrap_or(agg_id),
                            status: "Created".to_owned(),
                            closed: false,
                        });
                    }
                }
            }
            "CargoLoaded" => {
                if let Some(cargo) = &mut self.cargo {
                    if cargo.manifest_id == agg_id {
                        cargo.status = "Loaded".to_owned();
                    }
                }
            }
            "UnloadingStarted" => {
                if let Some(cargo) = &mut self.cargo {
                    if cargo.manifest_id == agg_id {
                        cargo.status = "Unloading".to_owned();
                    }
                }
            }
            "CargoUnloaded" => {
                if let Some(cargo) = &mut self.cargo {
                    if cargo.manifest_id == agg_id {
                        cargo.status = "Unloaded".to_owned();
                    }
                }
            }
            "ManifestClosed" => {
                if let Some(cargo) = &mut self.cargo {
                    if cargo.manifest_id == agg_id {
                        cargo.status = "Closed".to_owned();
                        cargo.closed = true;
                    }
                }
            }
            _ => {}
        }
    }

    // ── Navigation events ────────────────────────────────────────────────────

    fn apply_navigation_event(&mut self, agg_id: Uuid, envelope: &EventEnvelope) {
        match envelope.event_type.as_str() {
            "RoutePlanned" => {
                #[derive(serde::Deserialize)]
                struct E {
                    ship_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    if e.ship_id == self.session_ids.ship_id {
                        self.tracked_ids.insert(agg_id);
                    }
                }
            }
            // ShipArrivedAtStation and PositionUpdated — tracked for event log,
            // but ship state is updated via fleet events (ShipDockedAtStation).
            _ => {}
        }
    }

    // ── Supply events ────────────────────────────────────────────────────────

    fn apply_supply_event(&mut self, agg_id: Uuid, envelope: &EventEnvelope) {
        match envelope.event_type.as_str() {
            "StockRecorded" => {
                #[derive(serde::Deserialize)]
                struct E {
                    station_id: Uuid,
                }
                if let Ok(e) = serde_json::from_slice::<E>(&envelope.payload) {
                    if self.session_ids.station_ids.contains(&e.station_id) {
                        self.tracked_ids.insert(agg_id);
                    }
                }
            }
            _ => {}
        }
    }

    // ── Event ring buffer ────────────────────────────────────────────────────

    fn push_event(&mut self, service: &str, envelope: &EventEnvelope) {
        if self.event_id_set.contains(&envelope.event_id) {
            return; // dedup
        }

        let entry = EventLogEntry {
            id: envelope.event_id,
            timestamp: envelope.timestamp,
            version: envelope.version.as_u64(),
            service: service.to_owned(),
            event_name: envelope.event_type.clone(),
            aggregate_id: *envelope.aggregate_id.as_uuid(),
            correlation_id: envelope.correlation_id,
        };

        // Insert sorted by timestamp (newest first) to handle cross-service ordering
        let insert_pos = self
            .events
            .iter()
            .position(|e| e.timestamp <= entry.timestamp)
            .unwrap_or(self.events.len());
        self.events.insert(insert_pos, entry);
        self.event_id_set.insert(envelope.event_id);
        self.event_count += 1;

        // Evict oldest if over capacity
        while self.events.len() > MAX_EVENTS {
            if let Some(evicted) = self.events.pop_back() {
                self.event_id_set.remove(&evicted.id);
            }
        }
    }

    // ── Cargo destination helper ─────────────────────────────────────────────

    /// Determine the cargo destination station ID, matching the logic in game.rs.
    fn cargo_destination(&self) -> Option<Uuid> {
        let ship = self.ship.as_ref()?;

        if ship.status.eq_ignore_ascii_case("transit") {
            return ship.station_id;
        }

        // Ship is docked — next station in the cycle
        ship.station_id.and_then(|sid| {
            let idx = self
                .session_ids
                .station_ids
                .iter()
                .position(|id| *id == sid)?;
            Some(self.session_ids.station_ids[(idx + 1) % self.session_ids.station_ids.len()])
        })
    }
}
