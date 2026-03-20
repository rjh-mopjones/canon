use leptos::prelude::*;

use crate::hydrate::hydrate_from_gateway;
use crate::pages::live_fleet::LiveFleetPage;
use crate::scenarios::dead_letter::DeadLetterScenario;
use crate::scenarios::idempotency::IdempotencyScenario;
use crate::scenarios::oversight::OversightScenario;
use crate::scenarios::resupply::ResupplyScenario;
use crate::scenarios::snapshot::SnapshotScenario;
use crate::state::{create_app_state, ActiveTab, AppState, ConnectionStatus};
use crate::ws::connect_ws;

#[component]
pub fn App() -> impl IntoView {
    let state = create_app_state();

    // Connect WebSocket and hydrate from gateway on mount
    Effect::new(move |_| {
        connect_ws(state);
        hydrate_from_gateway(state);
    });

    view! {
        <div class="app-shell">
            <Header state=state />
            <TopNav state=state />
            {move || {
                match state.active_tab.get() {
                    ActiveTab::LiveFleet => view! { <LiveFleetPage state=state /> }.into_any(),
                    ActiveTab::Scenarios => {
                        view! { <ScenariosPage /> }.into_any()
                    }
                }
            }}
        </div>
    }
}

#[component]
fn Header(state: AppState) -> impl IntoView {
    let infra = state.infra;
    let conn = state.connection;

    let toggle_theme = move |_| {
        if let Some(window) = web_sys::window() {
            if let Some(doc) = window.document() {
                if let Some(body) = doc.body() {
                    let cl = body.class_list();
                    let _ = cl.toggle("light");
                }
            }
        }
    };

    let conn_class = move || match conn.get() {
        ConnectionStatus::Connected => "infra-dot",
        ConnectionStatus::Reconnecting => "infra-dot warn",
        ConnectionStatus::Disconnected => "infra-dot err",
    };

    let conn_label = "WS";

    view! {
        <div class="header">
            <div class="header-logo">
                "CANON"
                <span>"Fleet Ops"</span>
            </div>
            <div class="header-right">
                <div class="infra-dots">
                    <span class="infra-label">{conn_label}</span>
                    <span class=conn_class></span>
                    <span class="infra-label">"KAFKA"</span>
                    <span class=move || {
                        if infra.get().kafka { "infra-dot" } else { "infra-dot err" }
                    }></span>
                    <span class="infra-label">"YUGABYTE"</span>
                    <span class=move || {
                        if infra.get().yugabyte { "infra-dot" } else { "infra-dot err" }
                    }></span>
                    <span class="infra-label">"CASSANDRA"</span>
                    <span class=move || {
                        if infra.get().cassandra { "infra-dot" } else { "infra-dot err" }
                    }></span>
                </div>
                <button class="theme-toggle" on:click=toggle_theme>
                    {move || {
                        let is_light = web_sys::window()
                            .and_then(|w| w.document())
                            .and_then(|d| d.body())
                            .map(|b| b.class_list().contains("light"))
                            .unwrap_or(false);
                        if is_light { "\u{2600} Light" } else { "\u{263E} Dark" }
                    }}
                </button>
            </div>
        </div>
    }
}

#[component]
fn TopNav(state: AppState) -> impl IntoView {
    let active = state.active_tab;

    view! {
        <div class="top-nav">
            <div
                class=move || {
                    if active.get() == ActiveTab::LiveFleet {
                        "nav-tab active"
                    } else {
                        "nav-tab"
                    }
                }
                on:click=move |_| active.set(ActiveTab::LiveFleet)
            >
                "Live Fleet"
            </div>
            <div
                class=move || {
                    if active.get() == ActiveTab::Scenarios {
                        "nav-tab active"
                    } else {
                        "nav-tab"
                    }
                }
                on:click=move |_| active.set(ActiveTab::Scenarios)
            >
                "Scenarios"
            </div>
        </div>
    }
}

/// Which scenario is currently open (None = card grid, Some = runner).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveScenario {
    Oversight,
    Snapshot,
    Resupply,
    DeadLetter,
    Idempotency,
}

/// Mission card tuple: (number, name, ship_line, description, tags, scenario_id).
type MissionInfo = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    Vec<&'static str>,
    ActiveScenario,
);

/// Scenarios page with hero section, mission card grid, and scenario runner overlay.
#[component]
fn ScenariosPage() -> impl IntoView {
    let active_scenario: RwSignal<Option<ActiveScenario>> = RwSignal::new(None);

    let missions: Vec<MissionInfo> = vec![
        (
            "MISSION 01",
            "The Stranded Cargo",
            "\u{1f6f8} VSS ARGO \u{00b7} BETA RELAY",
            "VSS Argo has arrived at Beta Relay but unloading is blocked \u{2014} the cargo manifest hasn't been filed yet. The oversight gate is holding the command back. You need to supply the missing piece.",
            vec!["Oversight Gates", "Command Assembly", "NotReady \u{2192} Ready"],
            ActiveScenario::Oversight,
        ),
        (
            "MISSION 02",
            "The Ghost Ship",
            "\u{1f480} VSS HERALD \u{00b7} deep space",
            "VSS Herald has been drifting offline for years, accumulating hundreds of logged events. Reactivating it means replaying every event from scratch \u{2014} unless you snapshot first.",
            vec!["Snapshotting", "Hydration Performance", "Event Replay"],
            ActiveScenario::Snapshot,
        ),
        (
            "MISSION 03",
            "The Resupply Crisis",
            "\u{26a0} GAMMA OUTPOST \u{00b7} critical stock",
            "Gamma Outpost has hit critical stock levels. A resupply chain needs to fire across all five services. Watch how a single event cascades into coordinated action across the fleet.",
            vec!["Cross-service Events", "Event Fan-out", "5-service Chain"],
            ActiveScenario::Resupply,
        ),
        (
            "MISSION 04",
            "The Cassandra Incident",
            "\u{1f534} FLEET-WIDE \u{00b7} storage failure",
            "A Cassandra node goes dark mid-flight. Events start failing, retry counts tick up, and the dead letter queue fills. Can you requeue the failures and bring the fleet back to order?",
            vec!["Dead Letters", "Retry Handling", "Recovery"],
            ActiveScenario::DeadLetter,
        ),
        (
            "MISSION 05",
            "The Duplicate Signal",
            "\u{1f501} VSS KRONOS \u{00b7} inbox",
            "A network hiccup causes VSS Kronos's departure command to arrive twice. Watch Canon's inbox idempotency layer silently deduplicate the second command \u{2014} no duplicate state, no double-fire.",
            vec!["Inbox Idempotency", "Deduplication", "Duplicate Commands"],
            ActiveScenario::Idempotency,
        ),
    ];

    let close_signal: RwSignal<bool> = RwSignal::new(false);

    // Watch close_signal: when it becomes true, clear the active scenario and reset.
    Effect::new(move || {
        if close_signal.get() {
            active_scenario.set(None);
            close_signal.set(false);
        }
    });

    view! {
        <div class="sc-hero">
            <div class="sc-hero-title">"Canon Feature Scenarios"</div>
            <div class="sc-hero-sub">
                "Each mission showcases a specific Canon capability. Follow the narrative, trigger the events, and watch the framework respond in real time."
            </div>
        </div>
        <div class="sc-grid">
            {missions
                .into_iter()
                .map(|(num, name, ship, desc, tags, scenario_id)| {
                    let on_launch = move |_| {
                        active_scenario.set(Some(scenario_id));
                    };
                    view! {
                        <div class="sc-card" on:click=on_launch>
                            <div class="sc-num">{num}</div>
                            <div class="sc-name">{name}</div>
                            <div class="sc-ship">{ship}</div>
                            <div class="sc-desc">{desc}</div>
                            <div class="sc-tags">
                                {tags
                                    .into_iter()
                                    .map(|tag| {
                                        view! { <span class="sc-tag">{tag}</span> }
                                    })
                                    .collect::<Vec<_>>()}
                            </div>
                            <div class="sc-launch">"Launch Mission \u{2192}"</div>
                        </div>
                    }
                })
                .collect::<Vec<_>>()}
        </div>

        // Scenario runners
        <Show when=move || active_scenario.get() == Some(ActiveScenario::Oversight)>
            <OversightScenario close_signal=close_signal />
        </Show>
        <Show when=move || active_scenario.get() == Some(ActiveScenario::Snapshot)>
            <SnapshotScenario close_signal=close_signal />
        </Show>
        <Show when=move || active_scenario.get() == Some(ActiveScenario::Resupply)>
            <ResupplyScenario close_signal=close_signal />
        </Show>
        <Show when=move || active_scenario.get() == Some(ActiveScenario::DeadLetter)>
            <DeadLetterScenario close_signal=close_signal />
        </Show>
        <Show when=move || active_scenario.get() == Some(ActiveScenario::Idempotency)>
            <IdempotencyScenario close_signal=close_signal />
        </Show>
    }
}
