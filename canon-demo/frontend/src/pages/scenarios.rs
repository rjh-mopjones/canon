use leptos::prelude::*;

use crate::state::{
    mission_cards, scenario_definitions, AppState, LogEntry, MissionCard, ScenarioId,
};

/// The Scenarios page: hero section + mission card grid.
#[component]
pub fn ScenariosPage() -> impl IntoView {
    let state = expect_context::<AppState>();
    let cards = mission_cards();

    view! {
        <div class="sc-hero">
            <div class="sc-hero-title">"Canon Feature Scenarios"</div>
            <div class="sc-hero-sub">
                "Each mission showcases a specific Canon capability. Follow the narrative, trigger the events, and watch the framework respond in real time."
            </div>
        </div>
        <div class="sc-grid">
            {cards.into_iter().map(|card| {
                let state = state.clone();
                view! { <ScenarioCard card=card state=state /> }
            }).collect::<Vec<_>>()}
        </div>
    }
}

/// A single mission card in the grid.
#[component]
fn ScenarioCard(card: MissionCard, state: AppState) -> impl IntoView {
    let id = card.id;
    let active_scenario = state.active_scenario;
    let is_active = move || active_scenario.get() == Some(id);

    let on_launch = {
        let state = state.clone();
        move |_| {
            state.active_scenario.set(Some(id));
            state.scenario_step.set(0);
            state.scenario_completed.set(false);
            state.scenario_log.set(Vec::new());
        }
    };

    view! {
        <div
            class=move || if is_active() { "sc-card active" } else { "sc-card" }
        >
            <div class="sc-num">{card.number}</div>
            <div class="sc-name">{card.name}</div>
            <div class="sc-ship">{card.context_line}</div>
            <div class="sc-desc">{card.description}</div>
            <div class="sc-tags">
                {card.tags.iter().map(|tag| {
                    view! { <span class="sc-tag">{*tag}</span> }
                }).collect::<Vec<_>>()}
            </div>
            <div class="sc-launch" on:click=on_launch>"Launch Mission \u{2192}"</div>
        </div>
    }
}

/// The full-screen scenario runner modal.
#[component]
pub fn ScenarioRunner() -> impl IntoView {
    let state = expect_context::<AppState>();
    let definitions = scenario_definitions();

    let is_open = move || state.active_scenario.get().is_some();

    let runner_class = move || {
        if is_open() {
            "sc-runner open"
        } else {
            "sc-runner"
        }
    };

    let current_def = {
        let definitions = definitions.clone();
        move || {
            state
                .active_scenario
                .get()
                .and_then(|id| definitions.iter().find(|d| d.id == id).cloned())
        }
    };

    let on_close = {
        let state = state.clone();
        move |_| {
            state.active_scenario.set(None);
            state.scenario_log.set(Vec::new());
            state.scenario_step.set(0);
            state.scenario_completed.set(false);
        }
    };

    view! {
        <div class=runner_class>
            <div class="sc-runner-hdr">
                <div class="sc-runner-title">
                    {move || current_def().map(|d| d.title).unwrap_or("")}
                </div>
                <button class="sc-close" on:click=on_close>
                    "\u{2715} Close"
                </button>
            </div>
            <div class="sc-body">
                <div class="sc-stage">
                    <StepBar />
                    <NarrativeSection />
                    <SuccessBanner />
                    <ActionArea />
                </div>
                <div class="sc-log-wrap">
                    <div class="sb-bar">
                        <div class="bar-lbl">"Event Log"</div>
                        <div class="live-badge">
                            <div class="dot"></div>
                            "Live"
                        </div>
                    </div>
                    <ScenarioEventLog />
                </div>
            </div>
        </div>
    }
}

/// Step progress bar at the top of the stage.
#[component]
fn StepBar() -> impl IntoView {
    let state = expect_context::<AppState>();
    let definitions = scenario_definitions();

    let steps_view = move || {
        let active_step = state.scenario_step.get();
        let completed = state.scenario_completed.get();
        let definitions = definitions.clone();

        state.active_scenario.get().and_then(|id| {
            definitions.iter().find(|d| d.id == id).map(|def| {
                def.steps
                    .iter()
                    .enumerate()
                    .map(|(i, step)| {
                        let step_class = if completed || i < active_step {
                            "step done"
                        } else if i == active_step {
                            "step active"
                        } else {
                            "step"
                        };

                        let dot_content = if completed || i < active_step {
                            "\u{2713}".to_string()
                        } else {
                            (i + 1).to_string()
                        };

                        let is_last = i == def.steps.len() - 1;

                        view! {
                            <div class=step_class>
                                <div class="step-dot">{dot_content}</div>
                                <div class="step-lbl">{step.label}</div>
                            </div>
                            {if !is_last {
                                Some(view! { <div class="step-line"></div> })
                            } else {
                                None
                            }}
                        }
                    })
                    .collect::<Vec<_>>()
            })
        })
    };

    view! {
        <div class="step-bar">
            {steps_view}
        </div>
    }
}

/// Narrative section showing step title and body text.
#[component]
fn NarrativeSection() -> impl IntoView {
    let state = expect_context::<AppState>();
    let definitions = scenario_definitions();

    let narrative = move || {
        let step = state.scenario_step.get();
        let definitions = definitions.clone();

        state.active_scenario.get().and_then(|id| {
            definitions
                .iter()
                .find(|d| d.id == id)
                .and_then(|def| def.steps.get(step))
                .map(|s| (s.title, s.body))
        })
    };

    let narrative_title = {
        let narrative = narrative.clone();
        move || narrative().map(|(t, _)| t).unwrap_or("")
    };

    let narrative_body = move || narrative().map(|(_, b)| b).unwrap_or("");

    view! {
        <div class="sc-narr">
            <div class="sc-narr-title">
                {narrative_title}
            </div>
            <div class="sc-narr-body">
                {narrative_body}
            </div>
        </div>
    }
}

/// Success banner, shown when the scenario is completed.
#[component]
fn SuccessBanner() -> impl IntoView {
    let state = expect_context::<AppState>();
    let definitions = scenario_definitions();

    let banner_class = move || {
        if state.scenario_completed.get() {
            "success-banner show"
        } else {
            "success-banner"
        }
    };

    let success_content = move || {
        let definitions = definitions.clone();
        state.active_scenario.get().and_then(|id| {
            definitions
                .iter()
                .find(|d| d.id == id)
                .map(|def| (def.success_title, def.success_body))
        })
    };

    let success_title = {
        let success_content = success_content.clone();
        move || success_content().map(|(t, _)| t).unwrap_or("")
    };

    let success_body = move || success_content().map(|(_, b)| b).unwrap_or("");

    view! {
        <div class=banner_class>
            <strong>{success_title}</strong>
            {success_body}
        </div>
    }
}

/// Get the button label for a given scenario and step.
fn get_button_label(id: ScenarioId, step: usize) -> &'static str {
    match id {
        ScenarioId::Oversight => match step {
            0 => "File Cargo Manifest \u{2192}",
            _ => "Continue \u{2192}",
        },
        ScenarioId::Snapshot => match step {
            0 => "Reactivate VSS Herald (Cold) \u{2192}",
            2 => "Write Snapshot at v247 \u{2192}",
            _ => "Continue \u{2192}",
        },
        ScenarioId::Resupply => match step {
            0 => "Trigger Stock Alert \u{2192}",
            _ => "Continue \u{2192}",
        },
        ScenarioId::DeadLetter => match step {
            0 => "Take Cassandra Node Offline \u{2192}",
            _ => "Continue \u{2192}",
        },
        ScenarioId::Idempotency => match step {
            0 => "Send Departure Command \u{2192}",
            1 => "Simulate Network Retry (duplicate) \u{2192}",
            _ => "Continue \u{2192}",
        },
    }
}

/// Action area placeholder. Scenario-specific visualisations will be added in issue #102.
/// For now, this renders the primary action button for advancing steps.
#[component]
fn ActionArea() -> impl IntoView {
    let state = expect_context::<AppState>();
    let definitions = scenario_definitions();

    let on_advance = {
        let state = state.clone();
        let definitions = definitions.clone();
        move |_| {
            let current_step = state.scenario_step.get();

            if let Some(id) = state.active_scenario.get() {
                if let Some(def) = definitions.iter().find(|d| d.id == id) {
                    let total_steps = def.steps.len();

                    if current_step + 1 < total_steps {
                        state.scenario_step.set(current_step + 1);
                        push_scenario_step_event(&state, id, current_step + 1);
                    } else {
                        state.scenario_completed.set(true);
                    }
                }
            }
        }
    };

    let on_return = {
        let state = state.clone();
        move |_| {
            state.active_scenario.set(None);
            state.scenario_log.set(Vec::new());
            state.scenario_step.set(0);
            state.scenario_completed.set(false);
        }
    };

    let show_return = move || state.scenario_completed.get();

    let action_content = move || {
        let completed = state.scenario_completed.get();
        let step = state.scenario_step.get();

        if completed {
            return None;
        }

        state
            .active_scenario
            .get()
            .map(|id| get_button_label(id, step))
    };

    view! {
        <div class="sc-action-area">
            <div style="font-family:var(--mono);font-size:9px;color:var(--txtlo);">
                {move || if !state.scenario_completed.get() {
                    "// visualisation area \u{2014} issue #102"
                } else {
                    ""
                }}
            </div>

            {move || {
                action_content().map(|label| {
                    view! {
                        <button class="sc-big-btn" on:click=on_advance.clone()>
                            {label}
                        </button>
                    }
                })
            }}

            {move || {
                if show_return() {
                    Some(view! {
                        <button class="sc-sub-btn" on:click=on_return>
                            "Return to Scenarios"
                        </button>
                    })
                } else {
                    None
                }
            }}
        </div>
    }
}

/// Scenario event log (right column of the runner).
#[component]
fn ScenarioEventLog() -> impl IntoView {
    let state = expect_context::<AppState>();

    let log_entries = move || {
        state
            .scenario_log
            .get()
            .into_iter()
            .take(60)
            .collect::<Vec<_>>()
    };

    view! {
        <div class="sc-log-inner">
            <For
                each=move || {
                    log_entries().into_iter().enumerate().collect::<Vec<_>>()
                }
                key=|(i, entry)| format!("{}-{}-{}", i, entry.timestamp, entry.name)
                children=move |(i, entry)| {
                    let item_class = if i == 0 && entry.fresh { "log-item fresh" } else { "log-item" };
                    view! {
                        <div class=item_class>
                            <div class="log-ts">
                                {format!("{}\u{00b7}v{}", entry.timestamp, entry.version)}
                            </div>
                            <div class="log-row">
                                <span class={format!("svc {}", entry.svc_class)}>
                                    {entry.svc}
                                </span>
                                <span class="log-name">{entry.name.clone()}</span>
                            </div>
                            <div class="log-agg">{entry.aggregate.clone()}</div>
                        </div>
                        <div class="log-div"></div>
                    }
                }
            />
        </div>
    }
}

/// Push a simulated event for a scenario step advancement.
fn push_scenario_step_event(state: &AppState, scenario: ScenarioId, step: usize) {
    let (svc, svc_class, name, aggregate) = match scenario {
        ScenarioId::Oversight => match step {
            1 => ("cargo", "sc", "ManifestCreated", "MNF-7291"),
            2 => ("cargo", "sc", "UnloadingStarted", "VSS ARGO"),
            3 => ("cargo", "sc", "CargoUnloaded", "VSS ARGO"),
            4 => ("cargo", "sc", "ManifestClosed", "MNF-7291"),
            _ => ("fleet", "sf", "EventProcessed", "VSS ARGO"),
        },
        ScenarioId::Snapshot => match step {
            1 => ("fleet", "sf", "ReactivationRequested", "VSS HERALD"),
            2 => ("fleet", "sf", "AggregateHydrated", "VSS HERALD v247"),
            3 => ("fleet", "sf", "SnapshotWritten", "VSS HERALD v247"),
            4 => ("fleet", "sf", "SnapshotLoaded", "VSS HERALD v247"),
            _ => ("fleet", "sf", "EventProcessed", "VSS HERALD"),
        },
        ScenarioId::Resupply => match step {
            1 => ("station", "ss", "StationStockLow", "GAMMA OUTPOST"),
            2 => ("supply", "su", "ResupplyDispatched", "SUP-7832"),
            3 => ("fleet", "sf", "ResupplyScheduled", "VSS ECLIPSE"),
            4 => ("fleet", "sf", "ShipDeparted", "VSS ECLIPSE"),
            _ => ("station", "ss", "EventProcessed", "GAMMA"),
        },
        ScenarioId::DeadLetter => match step {
            1 => ("fleet", "sf", "CassandraNodeDown", "CLUSTER-01"),
            2 => ("cargo", "sc", "RetryAttempt3:CargoUnloaded", "VSS MERIDIAN"),
            3 => ("nav", "sn", "DeadLetterCreated", "VSS ARGO"),
            4 => ("station", "ss", "DeadLetterRequeued", "DELTA PRIME"),
            _ => ("fleet", "sf", "EventProcessed", "CLUSTER-01"),
        },
        ScenarioId::Idempotency => match step {
            1 => ("fleet", "sf", "InboxAccepted", "VSS KRONOS"),
            2 => ("fleet", "sf", "InboxDeduplicated", "VSS KRONOS"),
            3 => ("fleet", "sf", "ShipDeparted", "VSS KRONOS"),
            4 => ("nav", "sn", "RoutePlanned", "ROUTE-DELTA"),
            _ => ("fleet", "sf", "EventProcessed", "VSS KRONOS"),
        },
    };

    let now = js_sys::Date::new_0();
    let hours = now.get_hours();
    let minutes = now.get_minutes();
    let seconds = now.get_seconds();
    let millis = now.get_milliseconds();

    let timestamp = format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis);

    let version = (js_sys::Math::random() * 50.0) as u32 + 1;

    let entry = LogEntry {
        svc,
        svc_class,
        name: name.to_string(),
        aggregate: aggregate.to_string(),
        correlation_id: format!("SC-{:03X}", (js_sys::Math::random() * 4095.0) as u32),
        timestamp,
        version,
        fresh: true,
    };

    state.scenario_log.update(|log| {
        log.insert(0, entry);
        if log.len() > 60 {
            log.pop();
        }
    });
}
