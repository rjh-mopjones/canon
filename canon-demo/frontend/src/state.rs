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
    /// Canvas pixel X (set by animation system). None means derive from left_pct.
    #[serde(default)]
    pub canvas_x: Option<f64>,
    /// Canvas pixel Y (set by animation system). None means derive from top_pct.
    #[serde(default)]
    pub canvas_y: Option<f64>,
    /// Origin percentage X for current flight (for route line drawing).
    #[serde(default)]
    pub from_pct_x: Option<f64>,
    /// Origin percentage Y for current flight.
    #[serde(default)]
    pub from_pct_y: Option<f64>,
    /// Flight start time (performance.now() in ms). None = not animating.
    #[serde(default)]
    pub flight_start_ms: Option<f64>,
    /// Flight duration in ms.
    #[serde(default)]
    pub flight_duration_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDef {
    pub id: Uuid,
    pub name: String,
    pub left_pct: f64,
    pub top_pct: f64,
    pub stock_low: bool,
    /// Station stock percentage (0..100), used for canvas rendering.
    pub stock_pct: f64,
    /// Planet fill colour for canvas rendering (e.g. "#3B6D11").
    #[serde(default)]
    pub planet_color: String,
    /// Planet radius in canvas pixels.
    #[serde(default)]
    pub planet_radius: f64,
    /// Whether this station has a ring (Beta Relay).
    #[serde(default)]
    pub has_ring: bool,
    /// Supply loop: which station index is supplied by this station.
    #[serde(default)]
    pub supplied_by_name: String,
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

/// WebSocket connection status shown in the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Not yet attempted.
    Disconnected,
    /// Actively connected and receiving messages.
    Connected,
    /// Connection lost, attempting to reconnect.
    Reconnecting,
}

/// Whether the frontend is running against a live gateway or in demo mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataMode {
    /// Using local simulation (no gateway available).
    Demo,
    /// Connected to a live Canon gateway.
    Live,
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
    /// Current WebSocket connection status.
    pub connection: RwSignal<ConnectionStatus>,
    /// Whether we are running against a live gateway or in demo mode.
    pub data_mode: RwSignal<DataMode>,
    /// Dead letter entries retrieved from the gateway.
    pub dead_letters: RwSignal<Vec<DeadLetterEntry>>,
}

/// Dead letter entry as received from `GET /admin/deadletters`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub event_type: String,
    pub service: String,
    pub aggregate_id: String,
    pub error: String,
    pub attempts: u32,
    pub requeued: bool,
    pub created_at: String,
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
            stock_pct: 85.0,
            planet_color: "#3B6D11".into(),
            planet_radius: 32.0,
            has_ring: false,
            supplied_by_name: "Delta Prime".into(),
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Beta Relay".into(),
            left_pct: 68.0,
            top_pct: 14.0,
            stock_low: false,
            stock_pct: 60.0,
            planet_color: "#534AB7".into(),
            planet_radius: 22.0,
            has_ring: true,
            supplied_by_name: "Alpha Depot".into(),
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Gamma Outpost".into(),
            left_pct: 76.0,
            top_pct: 68.0,
            stock_low: true,
            stock_pct: 40.0,
            planet_color: "#993C1D".into(),
            planet_radius: 28.0,
            has_ring: false,
            supplied_by_name: "Beta Relay".into(),
        },
        StationDef {
            id: Uuid::new_v4(),
            name: "Delta Prime".into(),
            left_pct: 24.0,
            top_pct: 74.0,
            stock_low: false,
            stock_pct: 75.0,
            planet_color: "#185FA5".into(),
            planet_radius: 20.0,
            has_ring: false,
            supplied_by_name: "Gamma Outpost".into(),
        },
    ]
}

/// Ship initial definition — single ship: VSS Meridian, starts undocked in the centre.
pub fn default_ships(_stations: &[StationDef]) -> Vec<ShipState> {
    vec![ShipState {
        id: Uuid::new_v4(),
        name: "Meridian".into(),
        status: ShipStatus::Docked,
        fuel_pct: 72.0,
        version: 147,
        events_since_snapshot: 47,
        snapshot_every: 100,
        current_station_idx: None,
        destination_station_idx: None,
        left_pct: 50.0,
        top_pct: 50.0,
        canvas_x: None,
        canvas_y: None,
        from_pct_x: None,
        from_pct_y: None,
        flight_start_ms: None,
        flight_duration_ms: None,
    }]
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
        connection: RwSignal::new(ConnectionStatus::Disconnected),
        data_mode: RwSignal::new(DataMode::Demo),
        dead_letters: RwSignal::new(Vec::new()),
    }
}
