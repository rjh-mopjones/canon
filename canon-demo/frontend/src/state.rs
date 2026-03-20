use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Domain types (local to frontend — no canon-core dependency)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShipStatus {
    Docked,
    Transit,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShipState {
    pub id: Uuid,
    pub name: String,
    pub status: ShipStatus,
    pub fuel_pct: f32,
    pub version: u64,
    pub events_since_snapshot: u32,
    pub snapshot_every: u32,
    pub current_station_idx: Option<usize>,
    pub destination_station_idx: Option<usize>,
    /// Left % position on canvas
    pub left_pct: f64,
    /// Top % position on canvas
    pub top_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDef {
    pub id: Uuid,
    pub name: String,
    pub left_pct: f64,
    pub top_pct: f64,
    pub stock_low: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: Uuid,
    pub timestamp: String,
    pub version: u64,
    pub service: String,
    pub event_name: String,
    pub aggregate_id: Uuid,
    pub correlation_id: Uuid,
    pub is_new: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OversightReqStatus {
    Pending,
    Met,
}

#[derive(Debug, Clone)]
pub struct OversightState {
    pub visible: bool,
    pub handler_id: String,
    pub gate_title: String,
    pub arrival_status: OversightReqStatus,
    pub manifest_status: OversightReqStatus,
}

impl Default for OversightState {
    fn default() -> Self {
        Self {
            visible: false,
            handler_id: String::new(),
            gate_title: String::new(),
            arrival_status: OversightReqStatus::Pending,
            manifest_status: OversightReqStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatus {
    pub kafka: bool,
    pub yugabyte: bool,
    pub cassandra: bool,
}

impl Default for InfraStatus {
    fn default() -> Self {
        Self {
            kafka: true,
            yugabyte: true,
            cassandra: true,
        }
    }
}

// WebSocket message types (matching gateway spec)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsMessage {
    Event(LiveEvent),
    ShipUpdate(ShipUpdateMsg),
    StationUpdate(StationUpdateMsg),
    OversightUpdate(OversightUpdateMsg),
    DeadLetter(DeadLetterMsg),
    InfraStatus(InfraStatusMsg),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEvent {
    pub event_type: String,
    pub service: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub version: u64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShipUpdateMsg {
    pub id: String,
    pub status: String,
    pub fuel_pct: f32,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationUpdateMsg {
    pub id: String,
    pub stock_low: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OversightUpdateMsg {
    pub handler_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterMsg {
    pub id: String,
    pub event_name: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraStatusMsg {
    pub kafka: bool,
    pub yugabyte: bool,
    pub cassandra: bool,
}

// ---------------------------------------------------------------------------
// Global reactive state
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct AppState {
    pub ships: RwSignal<Vec<ShipState>>,
    pub stations: RwSignal<Vec<StationDef>>,
    pub log_entries: RwSignal<Vec<LogEntry>>,
    pub selected_ship: RwSignal<Option<usize>>,
    pub highlighted_corr: RwSignal<Option<Uuid>>,
    pub oversight: RwSignal<OversightState>,
    pub infra: RwSignal<InfraStatus>,
    pub active_tab: RwSignal<ActiveTab>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveTab {
    LiveFleet,
    Scenarios,
}

/// Station definitions
pub fn default_stations() -> Vec<StationDef> {
    vec![
        StationDef {
            id: Uuid::new_v4(),
            name: "Alpha Depot".into(),
            left_pct: 18.0,
            top_pct: 26.0,
            stock_low: false,
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Beta Relay".into(),
            left_pct: 68.0,
            top_pct: 14.0,
            stock_low: false,
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Gamma Outpost".into(),
            left_pct: 76.0,
            top_pct: 68.0,
            stock_low: true,
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Delta Prime".into(),
            left_pct: 24.0,
            top_pct: 74.0,
            stock_low: false,
        },
    ]
}

/// Ship initial definitions — all start docked at stations
pub fn default_ships(stations: &[StationDef]) -> Vec<ShipState> {
    vec![
        ShipState {
            id: Uuid::new_v4(),
            name: "Meridian".into(),
            status: ShipStatus::Docked,
            fuel_pct: 87.0,
            version: 12,
            events_since_snapshot: 12,
            snapshot_every: 50,
            current_station_idx: Some(0),
            destination_station_idx: None,
            left_pct: stations[0].left_pct,
            top_pct: stations[0].top_pct,
        },
        ShipState {
            id: Uuid::new_v4(),
            name: "Argo".into(),
            status: ShipStatus::Docked,
            fuel_pct: 64.0,
            version: 38,
            events_since_snapshot: 38,
            snapshot_every: 50,
            current_station_idx: Some(1),
            destination_station_idx: None,
            left_pct: stations[1].left_pct,
            top_pct: stations[1].top_pct,
        },
        ShipState {
            id: Uuid::new_v4(),
            name: "Eclipse".into(),
            status: ShipStatus::Docked,
            fuel_pct: 92.0,
            version: 5,
            events_since_snapshot: 5,
            snapshot_every: 50,
            current_station_idx: Some(2),
            destination_station_idx: None,
            left_pct: stations[2].left_pct,
            top_pct: stations[2].top_pct,
        },
        ShipState {
            id: Uuid::new_v4(),
            name: "Kronos".into(),
            status: ShipStatus::Docked,
            fuel_pct: 73.0,
            version: 22,
            events_since_snapshot: 22,
            snapshot_every: 50,
            current_station_idx: Some(3),
            destination_station_idx: None,
            left_pct: stations[3].left_pct,
            top_pct: stations[3].top_pct,
        },
        ShipState {
            id: Uuid::new_v4(),
            name: "Herald".into(),
            status: ShipStatus::Dead,
            fuel_pct: 0.0,
            version: 247,
            events_since_snapshot: 47,
            snapshot_every: 50,
            current_station_idx: None,
            destination_station_idx: None,
            left_pct: 48.0,
            top_pct: 44.0,
        },
    ]
}

pub fn create_app_state() -> AppState {
    let stations = default_stations();
    let ships = default_ships(&stations);

    AppState {
        ships: RwSignal::new(ships),
        stations: RwSignal::new(stations),
        log_entries: RwSignal::new(Vec::new()),
        selected_ship: RwSignal::new(None),
        highlighted_corr: RwSignal::new(None),
        oversight: RwSignal::new(OversightState::default()),
        infra: RwSignal::new(InfraStatus::default()),
        active_tab: RwSignal::new(ActiveTab::LiveFleet),
    }
}
