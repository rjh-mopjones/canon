use crate::scenarios::runner::{fresh_corr, push_sc_log, ScenarioLogEntry, ScenarioRunner};
use leptos::prelude::*;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Initial,
    Accepted,
    Deduplicated,
    Complete,
}

/// Mission 05 -- The Duplicate Signal (Idempotency)
#[component]
pub fn IdempotencyScenario(close_signal: RwSignal<bool>) -> impl IntoView {
    let current_step: RwSignal<usize> = RwSignal::new(0);
    let log: RwSignal<VecDeque<ScenarioLogEntry>> = RwSignal::new(VecDeque::new());
    let narr_title = RwSignal::new(String::from("VSS Kronos \u{2014} departure command sent"));
    let narr_body = RwSignal::new(String::from(
        "Kronos has been issued a departure command to Delta Prime. The command enters the \
         inbox tagged with a unique message_id. Now watch what happens when the same command \
         arrives a second time due to a network hiccup.",
    ));
    let success_title: RwSignal<String> = RwSignal::new(String::new());
    let success_body: RwSignal<String> = RwSignal::new(String::new());

    let phase = RwSignal::new(Phase::Initial);
    let message_id = RwSignal::new(format!(
        "CMD-{}",
        (js_sys::Math::random() * 90000.0) as u32 + 10000
    ));
    let button_text = RwSignal::new(String::from("Send Departure Command \u{2192}"));
    let button_disabled = RwSignal::new(false);
    let show_conflict_note = RwSignal::new(false);

    let on_click = move |_| {
        if button_disabled.get_untracked() {
            return;
        }

        match phase.get_untracked() {
            Phase::Initial => {
                current_step.set(1);
                phase.set(Phase::Accepted);

                let corr = fresh_corr();
                let mid = message_id.get_untracked();
                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "DepartForStation",
                    "VSS KRONOS \u{2192} DELTA PRIME",
                    &corr,
                );
                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "InboxAccepted",
                    &format!("msg_id: {}", mid),
                    &corr,
                );

                narr_title.set("Command accepted \u{2014} now simulate a duplicate".into());
                narr_body.set(format!(
                    "The first command was accepted and stamped with message_id {}. A network \
                     retry is about to send the exact same command again with the same \
                     message_id. Canon's inbox will see the duplicate and silently discard it.",
                    mid
                ));

                let cb = gloo_timers::callback::Timeout::new(800, move || {
                    button_text.set("Simulate Network Retry \u{2192}".into());
                });
                cb.forget();
            }
            Phase::Accepted => {
                button_disabled.set(true);
                current_step.set(2);
                phase.set(Phase::Deduplicated);

                let corr = fresh_corr();
                let mid = message_id.get_untracked();
                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "DepartForStation",
                    "VSS KRONOS \u{2192} DELTA PRIME (duplicate)",
                    &corr,
                );

                let corr2 = corr.clone();
                let cb1 = gloo_timers::callback::Timeout::new(600, move || {
                    push_sc_log(
                        log,
                        "fleet",
                        "sf",
                        "InboxDeduplicated",
                        &format!("msg_id: {} \u{2014} already processed", mid),
                        &corr2,
                    );
                    show_conflict_note.set(true);

                    narr_title.set("Duplicate silently discarded".into());
                    narr_body.set(
                        "The second command arrived with the same message_id. Canon's inbox \
                         performed an INSERT \u{2026} ON CONFLICT DO NOTHING against the \
                         (handler_id, message_id) composite primary key. The row already \
                         existed \u{2014} the insert was a no-op. Zero downstream effects."
                            .into(),
                    );
                });
                cb1.forget();

                let cb2 = gloo_timers::callback::Timeout::new(1400, move || {
                    let corr3 = fresh_corr();
                    push_sc_log(log, "fleet", "sf", "ShipDeparted", "VSS KRONOS", &corr3);
                    let corr3b = fresh_corr();
                    let cb3 = gloo_timers::callback::Timeout::new(400, move || {
                        push_sc_log(log, "nav", "sn", "RoutePlanned", "ROUTE-DELTA", &corr3b);
                    });
                    cb3.forget();
                });
                cb2.forget();

                let cb4 = gloo_timers::callback::Timeout::new(2200, move || {
                    phase.set(Phase::Complete);
                    current_step.set(4);
                    success_title.set("Idempotency demonstrated".into());
                    success_body.set(
                        "Two identical commands arrived. One was processed. One was silently \
                         discarded. The ship departed exactly once. Canon's inbox uses a \
                         composite primary key (handler_id + message_id) with INSERT \u{2026} \
                         ON CONFLICT DO NOTHING \u{2014} the simplest possible idempotency \
                         mechanism, with zero application code required."
                            .into(),
                    );
                });
                cb4.forget();
            }
            _ => {}
        }
    };

    let card1_badge_class = move || match phase.get() {
        Phase::Initial => "idem-badge idem-pending",
        _ => "idem-badge idem-accepted",
    };
    let card1_badge_text = move || match phase.get() {
        Phase::Initial => "PENDING",
        _ => "ACCEPTED",
    };

    let card2_badge_class = move || match phase.get() {
        Phase::Deduplicated | Phase::Complete => "idem-badge idem-dedup",
        _ => "idem-badge idem-pending",
    };
    let card2_badge_text = move || match phase.get() {
        Phase::Deduplicated | Phase::Complete => "DEDUPLICATED",
        _ => "PENDING",
    };

    let card2_style = move || match phase.get() {
        Phase::Deduplicated | Phase::Complete => "opacity:0.4;transition:opacity 0.5s ease;",
        _ => "opacity:1;transition:opacity 0.5s ease;",
    };

    let show_x_overlay = move || matches!(phase.get(), Phase::Deduplicated | Phase::Complete);

    view! {
        <ScenarioRunner
            title="Mission 05 \u{2014} The Duplicate Signal"
            steps=vec!["Depart", "Duplicate", "Deduplicate", "Confirm", "Complete"]
            current_step=current_step
            log=log
            close_signal=close_signal
            narr_title=narr_title
            narr_body=narr_body
            success_title=success_title
            success_body=success_body
        >
            <div class="idem-cards">
                <div class="idem-card">
                    <div class="idem-card-label">"Command 1"</div>
                    <div class="idem-card-type">"DepartForStation"</div>
                    <div class="idem-card-mid">
                        <span class="idem-mid-label">"msg_id:"</span>
                        <span class="idem-mid-value">{move || message_id.get()}</span>
                    </div>
                    <div class=card1_badge_class>{card1_badge_text}</div>
                </div>

                <div class="idem-card" style=card2_style>
                    <Show when=show_x_overlay>
                        <div class="idem-x-overlay">{"\u{2715}"}</div>
                    </Show>
                    <div class="idem-card-label">"Command 2 (duplicate)"</div>
                    <div class="idem-card-type">"DepartForStation"</div>
                    <div class="idem-card-mid">
                        <span class="idem-mid-label">"msg_id:"</span>
                        <span class="idem-mid-value">{move || message_id.get()}</span>
                    </div>
                    <div class=card2_badge_class>{card2_badge_text}</div>
                </div>
            </div>

            <Show when=move || show_conflict_note.get()>
                <div class="idem-conflict-note">
                    "INSERT \u{2026} ON CONFLICT DO NOTHING \u{2014} row already exists"
                </div>
            </Show>

            <Show when=move || phase.get() != Phase::Complete>
                <button
                    class="sc-big-btn"
                    on:click=on_click
                    disabled=move || button_disabled.get()
                >
                    {move || button_text.get()}
                </button>
            </Show>

            <Show when=move || phase.get() == Phase::Complete>
                <button class="sc-sub-btn" on:click=move |_| close_signal.set(true)>
                    "Return to Scenarios"
                </button>
            </Show>
        </ScenarioRunner>
    }
}
