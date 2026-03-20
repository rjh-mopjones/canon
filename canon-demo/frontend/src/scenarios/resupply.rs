use crate::scenarios::runner::{fresh_corr, push_sc_log, ScenarioLogEntry, ScenarioRunner};
use leptos::prelude::*;
use std::collections::VecDeque;

struct PipelineNode {
    service: &'static str,
    svc_class: &'static str,
    event_name: &'static str,
    aggregate: &'static str,
}

fn cascade_chain() -> Vec<PipelineNode> {
    vec![
        PipelineNode {
            service: "station",
            svc_class: "ss",
            event_name: "StationStockLow",
            aggregate: "GAMMA OUTPOST",
        },
        PipelineNode {
            service: "supply",
            svc_class: "su",
            event_name: "ResupplyRequested",
            aggregate: "SUP-GAMMA",
        },
        PipelineNode {
            service: "supply",
            svc_class: "su",
            event_name: "InventoryChecked",
            aggregate: "SUP-GAMMA",
        },
        PipelineNode {
            service: "supply",
            svc_class: "su",
            event_name: "ResupplyCalculated",
            aggregate: "SUP-GAMMA",
        },
        PipelineNode {
            service: "supply",
            svc_class: "su",
            event_name: "ResupplyDispatched",
            aggregate: "SUP-7832",
        },
        PipelineNode {
            service: "fleet",
            svc_class: "sf",
            event_name: "ResupplyScheduled",
            aggregate: "VSS ECLIPSE",
        },
        PipelineNode {
            service: "nav",
            svc_class: "sn",
            event_name: "RoutePlanned",
            aggregate: "ROUTE-4491",
        },
        PipelineNode {
            service: "cargo",
            svc_class: "sc",
            event_name: "ManifestCreated",
            aggregate: "MNF-9102",
        },
        PipelineNode {
            service: "cargo",
            svc_class: "sc",
            event_name: "CargoLoaded",
            aggregate: "MNF-9102",
        },
        PipelineNode {
            service: "fleet",
            svc_class: "sf",
            event_name: "ShipDeparted",
            aggregate: "VSS ECLIPSE",
        },
    ]
}

struct DisplayNode {
    service: &'static str,
    svc_class: &'static str,
    label: &'static str,
}

fn display_nodes() -> Vec<DisplayNode> {
    vec![
        DisplayNode {
            service: "station",
            svc_class: "ss",
            label: "Station Service",
        },
        DisplayNode {
            service: "supply",
            svc_class: "su",
            label: "Supply Service",
        },
        DisplayNode {
            service: "fleet",
            svc_class: "sf",
            label: "Fleet Service",
        },
        DisplayNode {
            service: "nav",
            svc_class: "sn",
            label: "Navigation Service",
        },
        DisplayNode {
            service: "cargo",
            svc_class: "sc",
            label: "Cargo Service",
        },
    ]
}

/// Mission 03 -- The Resupply Crisis (Cross-service cascade)
#[component]
pub fn ResupplyScenario(close_signal: RwSignal<bool>) -> impl IntoView {
    let current_step: RwSignal<usize> = RwSignal::new(0);
    let log: RwSignal<VecDeque<ScenarioLogEntry>> = RwSignal::new(VecDeque::new());
    let narr_title = RwSignal::new(String::from("Gamma Outpost \u{2014} critical stock levels"));
    let narr_body = RwSignal::new(String::from(
        "Gamma Outpost has exhausted its fuel and parts reserves. The station service has just \
         fired a StationStockLow event. Five services need to coordinate to respond: station \
         detects the crisis, supply calculates requirements, fleet schedules a resupply mission, \
         navigation plots the route, cargo prepares the manifest.",
    ));
    let success_title: RwSignal<String> = RwSignal::new(String::new());
    let success_body: RwSignal<String> = RwSignal::new(String::new());

    let active_nodes = RwSignal::new(0usize);
    let cascade_done = RwSignal::new(false);
    let button_disabled = RwSignal::new(false);
    let summary_visible = RwSignal::new(false);

    let start_cascade = move |_| {
        if button_disabled.get_untracked() {
            return;
        }
        button_disabled.set(true);
        current_step.set(1);
        narr_title.set("Cross-service cascade in progress".into());
        narr_body.set(
            "Watch the events propagate across all five services in sequence. Each service \
             reacts to the previous one's output \u{2014} no orchestrator, no central \
             coordinator. Pure event-driven choreography."
                .into(),
        );

        let chain = cascade_chain();
        let corr = fresh_corr();

        let node_map: Vec<usize> = chain
            .iter()
            .map(|n| match n.service {
                "station" => 0,
                "supply" => 1,
                "fleet" => 2,
                "nav" => 3,
                "cargo" => 4,
                _ => 0,
            })
            .collect();

        for (i, node) in chain.iter().enumerate() {
            let delay = (i as u32) * 600;
            let svc = node.service;
            let cls = node.svc_class;
            let name = node.event_name.to_string();
            let agg = node.aggregate.to_string();
            let corr = corr.clone();
            let node_idx = node_map[i];
            let is_last = i == chain.len() - 1;

            let cb = gloo_timers::callback::Timeout::new(delay, move || {
                push_sc_log(log, svc, cls, &name, &agg, &corr);
                let current = active_nodes.get_untracked();
                if node_idx >= current {
                    active_nodes.set(node_idx + 1);
                }

                if is_last {
                    cascade_done.set(true);
                    let cb2 = gloo_timers::callback::Timeout::new(600, move || {
                        summary_visible.set(true);
                        current_step.set(4);
                        success_title.set("Resupply mission launched".into());
                        success_body.set(
                            "A single StationStockLow event triggered coordinated action \
                             across 5 independent services, producing 10 events across fleet, \
                             supply, navigation and cargo \u{2014} with no central orchestrator. \
                             Each service reacted to what it knew, nothing more."
                                .into(),
                        );
                    });
                    cb2.forget();
                }
            });
            cb.forget();
        }
    };

    let nodes = display_nodes();

    view! {
        <ScenarioRunner
            title="Mission 03 \u{2014} The Resupply Crisis"
            steps=vec!["Crisis", "Cascade", "Supply", "Dispatch", "Resolved"]
            current_step=current_step
            log=log
            close_signal=close_signal
            narr_title=narr_title
            narr_body=narr_body
            success_title=success_title
            success_body=success_body
        >
            <div class="pipeline-viz">
                {nodes
                    .into_iter()
                    .enumerate()
                    .map(|(i, node)| {
                        let is_last = i == 4;
                        let node_class = move || {
                            if i < active_nodes.get() { "pipeline-node active" } else { "pipeline-node" }
                        };
                        let arrow_class = move || {
                            if i < active_nodes.get() { "pipeline-arrow active" } else { "pipeline-arrow" }
                        };
                        view! {
                            <div class=node_class>
                                <span class=format!("svc {}", node.svc_class)>{node.service}</span>
                                <span class="pipeline-label">{node.label}</span>
                            </div>
                            {if !is_last {
                                Some(view! {
                                    <div class=arrow_class>
                                        <div class="pipeline-arrow-line"></div>
                                        <div class="pipeline-arrow-dot"></div>
                                    </div>
                                })
                            } else {
                                None
                            }}
                        }
                    })
                    .collect::<Vec<_>>()}
            </div>

            <Show when=move || summary_visible.get()>
                <div class="pipeline-summary">
                    "10 events \u{00b7} 5 services \u{00b7} zero coordination code"
                </div>
            </Show>

            <Show when=move || !button_disabled.get()>
                <button class="sc-big-btn" on:click=start_cascade>
                    "Trigger Stock Alert \u{2192}"
                </button>
            </Show>
            <Show when=move || cascade_done.get()>
                <button class="sc-sub-btn" on:click=move |_| close_signal.set(true)>
                    "Return to Scenarios"
                </button>
            </Show>
        </ScenarioRunner>
    }
}
