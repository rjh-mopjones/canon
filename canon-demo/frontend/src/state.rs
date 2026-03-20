use leptos::prelude::*;
use serde::{Deserialize, Serialize};

/// Which top-level page is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page {
    LiveFleet,
    Scenarios,
}

/// Unique scenario identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScenarioId {
    Oversight,
    Snapshot,
    Resupply,
    DeadLetter,
    Idempotency,
}

/// Infrastructure connection status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfraStatus {
    Connected,
    Disconnected,
}

/// Single entry in an event log (live sidebar or scenario log).
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub svc: &'static str,
    pub svc_class: &'static str,
    pub name: String,
    pub aggregate: String,
    pub correlation_id: String,
    pub timestamp: String,
    pub version: u32,
    pub fresh: bool,
}

/// Static data for a mission card.
#[derive(Clone, Debug)]
pub struct MissionCard {
    pub id: ScenarioId,
    pub number: &'static str,
    pub name: &'static str,
    pub context_line: &'static str,
    pub description: &'static str,
    pub tags: &'static [&'static str],
}

/// All five mission cards.
pub fn mission_cards() -> Vec<MissionCard> {
    vec![
        MissionCard {
            id: ScenarioId::Oversight,
            number: "MISSION 01",
            name: "The Stranded Cargo",
            context_line: "\u{1f6f8} VSS ARGO \u{00b7} BETA RELAY",
            description: "VSS Argo has arrived at Beta Relay but unloading is blocked \u{2014} the cargo manifest hasn't been filed yet. The oversight gate is holding the command back. You need to supply the missing piece.",
            tags: &["Oversight Gates", "Command Assembly", "NotReady \u{2192} Ready"],
        },
        MissionCard {
            id: ScenarioId::Snapshot,
            number: "MISSION 02",
            name: "The Ghost Ship",
            context_line: "\u{1f480} VSS HERALD \u{00b7} deep space",
            description: "VSS Herald has been drifting offline for years, accumulating hundreds of logged events. Reactivating it means replaying every event from scratch \u{2014} unless you snapshot first.",
            tags: &["Snapshotting", "Hydration Performance", "Event Replay"],
        },
        MissionCard {
            id: ScenarioId::Resupply,
            number: "MISSION 03",
            name: "The Resupply Crisis",
            context_line: "\u{26a0} GAMMA OUTPOST \u{00b7} critical stock",
            description: "Gamma Outpost has hit critical stock levels. A resupply chain needs to fire across all five services. Watch how a single event cascades into coordinated action across the fleet.",
            tags: &["Cross-service Events", "Event Fan-out", "5-service Chain"],
        },
        MissionCard {
            id: ScenarioId::DeadLetter,
            number: "MISSION 04",
            name: "The Cassandra Incident",
            context_line: "\u{1f534} FLEET-WIDE \u{00b7} storage failure",
            description: "A Cassandra node goes dark mid-flight. Events start failing, retry counts tick up, and the dead letter queue fills. Can you requeue the failures and bring the fleet back to order?",
            tags: &["Dead Letters", "Retry Handling", "Recovery"],
        },
        MissionCard {
            id: ScenarioId::Idempotency,
            number: "MISSION 05",
            name: "The Duplicate Signal",
            context_line: "\u{1f501} VSS KRONOS \u{00b7} inbox",
            description: "A network hiccup causes VSS Kronos's departure command to arrive twice. Watch Canon's inbox idempotency layer silently deduplicate the second command \u{2014} no duplicate state, no double-fire.",
            tags: &["Inbox Idempotency", "Deduplication", "Duplicate Commands"],
        },
    ]
}

/// Step definition for a scenario.
#[derive(Clone, Debug)]
pub struct ScenarioStep {
    pub label: &'static str,
    pub title: &'static str,
    pub body: &'static str,
}

/// Full scenario definition with all steps.
#[derive(Clone, Debug)]
pub struct ScenarioDefinition {
    pub id: ScenarioId,
    pub title: &'static str,
    pub steps: Vec<ScenarioStep>,
    pub success_title: &'static str,
    pub success_body: &'static str,
}

/// All scenario definitions.
pub fn scenario_definitions() -> Vec<ScenarioDefinition> {
    vec![
        ScenarioDefinition {
            id: ScenarioId::Oversight,
            title: "Mission 01 \u{2014} The Stranded Cargo",
            steps: vec![
                ScenarioStep {
                    label: "Arrive",
                    title: "VSS Argo has arrived at Beta Relay",
                    body: "The ship docked three minutes ago, but the cargo bay doors are still sealed. The oversight gate is holding the unload command back \u{2014} it needs two things to be true before it will fire: a confirmed arrival signal from navigation, and a cargo manifest from the cargo service. The arrival came through. The manifest has not.",
                },
                ScenarioStep {
                    label: "File Manifest",
                    title: "Manifest filed \u{2014} gate evaluating",
                    body: "The ManifestCreated event has been submitted. The oversight gate now has both conditions met. Watch it flip from NotReady to Ready and dispatch the BeginUnloading command.",
                },
                ScenarioStep {
                    label: "Gate Opens",
                    title: "Gate opened \u{2014} unloading dispatched",
                    body: "The oversight gate flipped to Ready and dispatched the unloading command. Cargo transfer is in progress.",
                },
                ScenarioStep {
                    label: "Unload",
                    title: "Cargo transfer in progress",
                    body: "Cargo is being unloaded from VSS Argo at Beta Relay. Events are flowing through the system.",
                },
                ScenarioStep {
                    label: "Complete",
                    title: "Unloading complete",
                    body: "All cargo has been transferred. The oversight gate consumed the window and closed.",
                },
            ],
            success_title: "Cargo unloaded successfully",
            success_body: "The oversight gate assembled both required events, dispatched the unloading command, and cargo has been transferred to Beta Relay. The gate consumed the window and closed. This is Canon's oversight system: conditional command dispatch without polling, without race conditions, without application code.",
        },
        ScenarioDefinition {
            id: ScenarioId::Snapshot,
            title: "Mission 02 \u{2014} The Ghost Ship",
            steps: vec![
                ScenarioStep {
                    label: "Assess",
                    title: "VSS Herald \u{2014} 247 logged events, no snapshot",
                    body: "This ship has been drifting offline for years. When we reactivate it, Canon must hydrate its aggregate state by replaying every single event from version 0 through to version 247. There is no snapshot. Watch what that costs.",
                },
                ScenarioStep {
                    label: "Replay Cold",
                    title: "Replaying 247 events from scratch",
                    body: "Canon is hydrating the aggregate by walking every event in order, applying each one to rebuild current state. Watch the counter. This is what happens without a snapshot.",
                },
                ScenarioStep {
                    label: "Snapshot",
                    title: "247 events replayed. Now take a snapshot.",
                    body: "That took 640ms to replay 247 events. Now we'll write a snapshot at the current version. Next time Herald needs to hydrate, it will load the snapshot directly and only replay events since the snapshot \u{2014} in this case, zero.",
                },
                ScenarioStep {
                    label: "Replay Hot",
                    title: "Snapshot written. Now reactivate from cold again.",
                    body: "We'll send Herald offline and reactivate it a second time. This time Canon loads the snapshot at v247, skips all 247 events, and hydrates instantly. Compare the two numbers.",
                },
                ScenarioStep {
                    label: "Compare",
                    title: "Performance comparison",
                    body: "Side-by-side comparison of hydration with and without snapshot.",
                },
            ],
            success_title: "Snapshot performance demonstrated",
            success_body: "Without snapshot: 640ms replaying 247 events. With snapshot: loading state directly. Canon writes snapshots automatically every 50 events via the event store consumer \u{2014} zero application code required.",
        },
        ScenarioDefinition {
            id: ScenarioId::Resupply,
            title: "Mission 03 \u{2014} The Resupply Crisis",
            steps: vec![
                ScenarioStep {
                    label: "Crisis",
                    title: "Gamma Outpost \u{2014} critical stock levels",
                    body: "Gamma Outpost has exhausted its fuel and parts reserves. The station service has just fired a StationStockLow event. Five services need to coordinate to respond: station detects the crisis, supply calculates requirements, fleet schedules a resupply mission, navigation plots the route, cargo prepares the manifest.",
                },
                ScenarioStep {
                    label: "Cascade",
                    title: "Cross-service cascade in progress",
                    body: "Watch the events propagate across all five services in sequence. Each service reacts to the previous one's output \u{2014} no orchestrator, no central coordinator. Pure event-driven choreography.",
                },
                ScenarioStep {
                    label: "Supply",
                    title: "Supply chain responding",
                    body: "Supply service has calculated requirements and dispatched a resupply mission.",
                },
                ScenarioStep {
                    label: "Dispatch",
                    title: "Fleet dispatched",
                    body: "Fleet service has scheduled the resupply and navigation has plotted the route.",
                },
                ScenarioStep {
                    label: "Resolved",
                    title: "Crisis resolved",
                    body: "All five services have coordinated to respond to the stock crisis.",
                },
            ],
            success_title: "Resupply mission launched",
            success_body: "A single StationStockLow event triggered coordinated action across 5 independent services, producing 10 events across fleet, supply, navigation and cargo \u{2014} with no central orchestrator. Each service reacted to what it knew, nothing more.",
        },
        ScenarioDefinition {
            id: ScenarioId::DeadLetter,
            title: "Mission 04 \u{2014} The Cassandra Incident",
            steps: vec![
                ScenarioStep {
                    label: "Node Down",
                    title: "Cassandra node failure detected",
                    body: "A Cassandra storage node has gone dark. Events that need to be written to the event store are failing. Canon's event store consumer will retry each event up to 3 times, tracking the attempt count in a crash-safe retry_attempts table. After 3 failures, the event is parked in the dead letter store rather than dropped.",
                },
                ScenarioStep {
                    label: "Failures",
                    title: "Failures accumulating \u{2014} retry counts ticking up",
                    body: "Three events have failed all 3 retry attempts and been dead-lettered. The Cassandra node is still down. You can requeue them once the node recovers \u{2014} Canon will re-enter them into the inbox with a fresh TTL.",
                },
                ScenarioStep {
                    label: "Dead Letters",
                    title: "Dead letter store \u{2014} 3 entries",
                    body: "The dead letter store holds the failed events. Each can be requeued or discarded.",
                },
                ScenarioStep {
                    label: "Requeue",
                    title: "Requeuing dead letters",
                    body: "Events are being requeued and reprocessed through the inbox.",
                },
                ScenarioStep {
                    label: "Recovered",
                    title: "All events recovered",
                    body: "All dead-lettered events have been successfully reprocessed.",
                },
            ],
            success_title: "All events recovered",
            success_body: "Three dead-lettered events were requeued, re-entered the inbox, passed through oversight, and were successfully written to Cassandra. No data was lost. The dead letter store is the safety net \u{2014} it catches what retry logic cannot.",
        },
        ScenarioDefinition {
            id: ScenarioId::Idempotency,
            title: "Mission 05 \u{2014} The Duplicate Signal",
            steps: vec![
                ScenarioStep {
                    label: "Depart",
                    title: "VSS Kronos \u{2014} departure command sent",
                    body: "Kronos has been issued a departure command to Delta Prime. The command enters the inbox tagged with a unique message_id. Now watch what happens when the same command arrives a second time due to a network hiccup.",
                },
                ScenarioStep {
                    label: "Duplicate",
                    title: "Command accepted \u{2014} now simulate a duplicate",
                    body: "The first command was accepted and stamped with a message_id. A network retry is about to send the exact same command again with the same message_id. Canon's inbox will see the duplicate and silently discard it.",
                },
                ScenarioStep {
                    label: "Deduplicate",
                    title: "Duplicate silently discarded",
                    body: "The second command arrived with the same message_id. Canon's inbox performed an INSERT \u{2026} ON CONFLICT DO NOTHING against the (handler_id, message_id) composite primary key. The row already existed \u{2014} the insert was a no-op. Zero downstream effects.",
                },
                ScenarioStep {
                    label: "Confirm",
                    title: "Ship departing \u{2014} exactly once",
                    body: "Despite two identical commands arriving, the ship departs exactly once. No duplicate state, no double-fire.",
                },
                ScenarioStep {
                    label: "Complete",
                    title: "Idempotency confirmed",
                    body: "Two identical commands arrived. One was processed. One was silently discarded.",
                },
            ],
            success_title: "Idempotency demonstrated",
            success_body: "Two identical commands arrived. One was processed. One was silently discarded. The ship departed exactly once. Canon's inbox uses a composite primary key (handler_id + message_id) with INSERT \u{2026} ON CONFLICT DO NOTHING \u{2014} the simplest possible idempotency mechanism, with zero application code required.",
        },
    ]
}

/// Global application state provided via Leptos context.
#[derive(Clone)]
pub struct AppState {
    pub active_page: RwSignal<Page>,
    pub active_scenario: RwSignal<Option<ScenarioId>>,
    pub scenario_step: RwSignal<usize>,
    pub scenario_completed: RwSignal<bool>,
    pub scenario_log: RwSignal<Vec<LogEntry>>,
    pub live_log: RwSignal<Vec<LogEntry>>,
    pub light_mode: RwSignal<bool>,
    pub kafka_status: RwSignal<InfraStatus>,
    pub yugabyte_status: RwSignal<InfraStatus>,
    pub cassandra_status: RwSignal<InfraStatus>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self {
            active_page: RwSignal::new(Page::LiveFleet),
            active_scenario: RwSignal::new(None),
            scenario_step: RwSignal::new(0),
            scenario_completed: RwSignal::new(false),
            scenario_log: RwSignal::new(Vec::new()),
            live_log: RwSignal::new(Vec::new()),
            light_mode: RwSignal::new(false),
            kafka_status: RwSignal::new(InfraStatus::Connected),
            yugabyte_status: RwSignal::new(InfraStatus::Connected),
            cassandra_status: RwSignal::new(InfraStatus::Connected),
        }
    }
}
