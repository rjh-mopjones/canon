use leptos::prelude::RwSignal;
use serde::Deserialize;
use std::collections::VecDeque;

/// Ship status in the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShipStatus {
    Docked,
    Transit,
    Dead,
}

impl ShipStatus {
    /// CSS class name for this status.
    pub fn css_class(&self) -> &'static str {
        match self {
            ShipStatus::Docked => "docked",
            ShipStatus::Transit => "transit",
            ShipStatus::Dead => "dead",
        }
    }

    /// Emoji icon for this status.
    pub fn icon(&self) -> &'static str {
        match self {
            ShipStatus::Docked => "\u{1f6f8}",  // flying saucer
            ShipStatus::Transit => "\u{1f680}", // rocket
            ShipStatus::Dead => "\u{1f480}",    // skull
        }
    }
}

/// State of a single ship.
#[derive(Debug, Clone, Deserialize)]
pub struct ShipState {
    pub id: String,
    pub status: ShipStatus,
    pub x: f64,
    pub y: f64,
    pub station_id: Option<String>,
    pub dest: Option<String>,
    pub fuel: f64,
    pub version: u64,
    pub snapshot_version: u64,
    pub correlation_id: Option<String>,
}

/// Station stock status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StockStatus {
    Ok,
    Low,
}

/// State of a station.
#[derive(Debug, Clone, Deserialize)]
pub struct StationState {
    pub id: String,
    pub label: String,
    pub x: f64,
    pub y: f64,
    pub stock: StockStatus,
}

/// Service label for event log badges.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceLabel {
    Fleet,
    Cargo,
    Nav,
    Station,
    Supply,
}

impl ServiceLabel {
    /// Display name for the service badge.
    pub fn display_name(&self) -> &'static str {
        match self {
            ServiceLabel::Fleet => "fleet",
            ServiceLabel::Cargo => "cargo",
            ServiceLabel::Nav => "nav",
            ServiceLabel::Station => "station",
            ServiceLabel::Supply => "supply",
        }
    }

    /// CSS class for the service badge colouring.
    pub fn css_class(&self) -> &'static str {
        match self {
            ServiceLabel::Fleet => "sf",
            ServiceLabel::Cargo => "sc",
            ServiceLabel::Nav => "sn",
            ServiceLabel::Station => "ss",
            ServiceLabel::Supply => "su",
        }
    }
}

/// A live event shown in the activity log.
#[derive(Debug, Clone, Deserialize)]
pub struct LiveEvent {
    pub service: ServiceLabel,
    pub event_name: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub timestamp: String,
    pub version: u64,
    #[serde(default)]
    pub fresh: bool,
}

/// Oversight window status.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OversightStatus {
    NotReady,
    Ready,
    Discard,
}

/// A single requirement in an oversight window.
#[derive(Debug, Clone, Deserialize)]
pub struct Requirement {
    pub label: String,
    pub met: bool,
}

/// An oversight window state.
#[derive(Debug, Clone, Deserialize)]
pub struct OversightWindow {
    pub handler_id: String,
    pub title: String,
    pub requirements: Vec<Requirement>,
    pub status: OversightStatus,
}

/// A dead letter entry.
#[derive(Debug, Clone, Deserialize)]
pub struct DeadLetterEntry {
    pub id: String,
    pub event_name: String,
    pub aggregate_id: String,
    pub error: String,
    pub attempts: u32,
}

/// Infra service connection status.
#[derive(Debug, Clone, Deserialize)]
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

/// Active page in the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    LiveFleet,
    Scenarios,
}

/// Top-level application state backed by reactive signals.
#[derive(Debug, Clone)]
pub struct AppState {
    pub ships: RwSignal<Vec<ShipState>>,
    pub stations: RwSignal<Vec<StationState>>,
    pub events: RwSignal<VecDeque<LiveEvent>>,
    pub oversight_windows: RwSignal<Vec<OversightWindow>>,
    pub dead_letters: RwSignal<Vec<DeadLetterEntry>>,
    pub active_page: RwSignal<Page>,
    pub highlighted_correlation: RwSignal<Option<String>>,
    pub selected_ship: RwSignal<Option<String>>,
    pub light_mode: RwSignal<bool>,
    pub infra_status: RwSignal<InfraStatus>,
    pub ws_connected: RwSignal<bool>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    /// Create a new AppState with default values.
    pub fn new() -> Self {
        Self {
            ships: RwSignal::new(Vec::new()),
            stations: RwSignal::new(Vec::new()),
            events: RwSignal::new(VecDeque::new()),
            oversight_windows: RwSignal::new(Vec::new()),
            dead_letters: RwSignal::new(Vec::new()),
            active_page: RwSignal::new(Page::LiveFleet),
            highlighted_correlation: RwSignal::new(None),
            selected_ship: RwSignal::new(None),
            light_mode: RwSignal::new(false),
            infra_status: RwSignal::new(InfraStatus::default()),
            ws_connected: RwSignal::new(false),
        }
    }
}
