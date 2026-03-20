use crate::scenarios::runner::{fresh_corr, push_sc_log, ScenarioLogEntry, ScenarioRunner};
use leptos::prelude::*;
use std::collections::VecDeque;

/// Mission 01 -- The Stranded Cargo (Oversight Gates)
#[component]
pub fn OversightScenario(close_signal: RwSignal<bool>) -> impl IntoView {
    let current_step: RwSignal<usize> = RwSignal::new(0);
    let log: RwSignal<VecDeque<ScenarioLogEntry>> = RwSignal::new(VecDeque::new());
    let narr_title = RwSignal::new(String::from("VSS Argo has arrived at Beta Relay"));
    let narr_body = RwSignal::new(String::from(
        "The ship docked three minutes ago, but the cargo bay doors are still sealed. \
         The oversight gate is holding the unload command back \u{2014} it needs two things \
         to be true before it will fire: a confirmed arrival signal from navigation, and a \
         cargo manifest from the cargo service. The arrival came through. The manifest has not.",
    ));
    let success_title: RwSignal<String> = RwSignal::new(String::new());
    let success_body: RwSignal<String> = RwSignal::new(String::new());

    let manifest_met = RwSignal::new(false);
    let gate_ready = RwSignal::new(false);
    let button_disabled = RwSignal::new(false);
    let completed = RwSignal::new(false);

    // Shared correlation ID for the entire event chain
    let corr = fresh_corr();

    // Fire initial arrival events, then advance step bar past "Arrive"
    {
        let corr_a = corr.clone();
        let corr_b = corr.clone();
        push_sc_log(
            log,
            "nav",
            "sn",
            "ShipArrivedAtStation",
            "VSS ARGO",
            &corr_a,
        );
        let cb = gloo_timers::callback::Timeout::new(400, move || {
            push_sc_log(log, "fleet", "sf", "ShipDocked", "BETA RELAY", &corr_b);
        });
        cb.forget();
        let cb2 = gloo_timers::callback::Timeout::new(800, move || {
            current_step.set(1);
        });
        cb2.forget();
    }

    let on_file_manifest = {
        let corr = corr.clone();
        move |_| {
            if button_disabled.get_untracked() {
                return;
            }
            button_disabled.set(true);
            current_step.set(2);
            narr_title.set("Manifest filed \u{2014} gate evaluating".into());
            narr_body.set(
                "The ManifestCreated event has been submitted. The oversight gate now has both \
                 conditions met. Watch it flip from NotReady to Ready and dispatch the \
                 BeginUnloading command."
                    .into(),
            );

            let corr_m = corr.clone();
            push_sc_log(log, "cargo", "sc", "ManifestCreated", "MNF-7291", &corr_m);

            // After 600ms: gate flips to Ready
            let cb1 = gloo_timers::callback::Timeout::new(600, move || {
                manifest_met.set(true);
                gate_ready.set(true);
            });
            cb1.forget();

            // After 900ms: oversight gate dispatches BeginUnloading command
            let corr_bu = corr.clone();
            let cb_bu = gloo_timers::callback::Timeout::new(900, move || {
                push_sc_log(log, "cargo", "sc", "BeginUnloading", "VSS ARGO", &corr_bu);
                current_step.set(3);
            });
            cb_bu.forget();

            // Downstream events: UnloadingStarted, CargoUnloaded (x2), CargoReceived, ManifestClosed
            let corr_1 = corr.clone();
            let corr_2 = corr.clone();
            let corr_3 = corr.clone();
            let corr_4 = corr.clone();
            let corr_5 = corr.clone();

            let cb2 = gloo_timers::callback::Timeout::new(1400, move || {
                push_sc_log(log, "cargo", "sc", "UnloadingStarted", "VSS ARGO", &corr_1);
            });
            cb2.forget();

            let cb3 = gloo_timers::callback::Timeout::new(1900, move || {
                push_sc_log(log, "cargo", "sc", "CargoUnloaded", "VSS ARGO", &corr_2);
            });
            cb3.forget();

            let cb4 = gloo_timers::callback::Timeout::new(2300, move || {
                push_sc_log(log, "cargo", "sc", "CargoUnloaded", "VSS ARGO", &corr_3);
            });
            cb4.forget();

            let cb5 = gloo_timers::callback::Timeout::new(2600, move || {
                push_sc_log(log, "station", "ss", "CargoReceived", "BETA RELAY", &corr_4);
            });
            cb5.forget();

            let cb6 = gloo_timers::callback::Timeout::new(3000, move || {
                push_sc_log(log, "cargo", "sc", "ManifestClosed", "MNF-7291", &corr_5);
            });
            cb6.forget();

            let cb7 = gloo_timers::callback::Timeout::new(3200, move || {
                current_step.set(4);
                completed.set(true);
                success_title.set("Cargo unloaded successfully".into());
                success_body.set(
                    "The oversight gate assembled both required events, dispatched the unloading \
                     command, and cargo has been transferred to Beta Relay. The gate consumed the \
                     window and closed. This is Canon\u{2019}s oversight system: conditional command \
                     dispatch without polling, without race conditions, without application code."
                        .into(),
                );
            });
            cb7.forget();
        }
    };

    let gate_style = move || {
        if gate_ready.get() {
            "border-color: var(--green);"
        } else {
            "border-color: var(--amber);"
        }
    };

    let manifest_row_class = move || {
        if manifest_met.get() {
            "gv-req met"
        } else {
            "gv-req pend"
        }
    };

    let manifest_icon = move || {
        if manifest_met.get() {
            "\u{2713}"
        } else {
            "\u{25cb}"
        }
    };

    let manifest_icon_style = move || {
        if manifest_met.get() {
            "display:inline-block;animation:check-pop 0.4s ease;"
        } else {
            "display:inline-block;animation:pulse-amber 1.5s ease-in-out infinite;"
        }
    };

    let badge_class = move || {
        if gate_ready.get() {
            "gv-badge gv-rdy"
        } else {
            "gv-badge gv-nr"
        }
    };

    let badge_text = move || {
        if gate_ready.get() {
            "Ready"
        } else {
            "Not Ready"
        }
    };

    view! {
        <ScenarioRunner
            title="Mission 01 \u{2014} The Stranded Cargo"
            steps=vec!["Arrive", "File Manifest", "Gate Opens", "Unload", "Complete"]
            current_step=current_step
            log=log
            close_signal=close_signal
            narr_title=narr_title
            narr_body=narr_body
            success_title=success_title
            success_body=success_body
        >
            <div class="gate-viz" style=gate_style>
                <div class="gv-title">"Unloading gate \u{2014} VSS ARGO at BETA RELAY"</div>
                <div class="gv-req met">
                    <span class="gv-icon">{"\u{2713}"}</span>
                    <span class="gv-lbl">"ShipArrivedAtStation (navigation)"</span>
                </div>
                <div class=manifest_row_class>
                    <span class="gv-icon" style=manifest_icon_style>{manifest_icon}</span>
                    <span class="gv-lbl">"ManifestCreated (cargo)"</span>
                </div>
                <div class=badge_class>{badge_text}</div>
            </div>
            <button
                class="sc-big-btn"
                on:click=on_file_manifest
                disabled=move || button_disabled.get()
            >
                "File Cargo Manifest \u{2192}"
            </button>
            <Show when=move || completed.get()>
                <button class="sc-sub-btn" on:click=move |_| close_signal.set(true)>
                    "Return to Scenarios"
                </button>
            </Show>
            <Show when=move || !button_disabled.get()>
                <div style="font-family:var(--mono);font-size:9px;color:var(--txtlo);">
                    "The manifest is missing. File it to unlock the gate."
                </div>
            </Show>
        </ScenarioRunner>
    }
}
