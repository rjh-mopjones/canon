use crate::scenarios::runner::{fresh_corr, push_sc_log, ScenarioLogEntry, ScenarioRunner};
use leptos::prelude::*;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CardState {
    Active,
    Requeued,
    Discarded,
}

struct DeadLetterInfo {
    event_name: &'static str,
    aggregate: &'static str,
    error: &'static str,
}

fn dead_letter_entries() -> Vec<DeadLetterInfo> {
    vec![
        DeadLetterInfo {
            event_name: "CargoUnloaded",
            aggregate: "VSS MERIDIAN",
            error: "cassandra write timeout after 3 retries",
        },
        DeadLetterInfo {
            event_name: "PositionUpdated",
            aggregate: "VSS ARGO",
            error: "cassandra write timeout after 3 retries",
        },
        DeadLetterInfo {
            event_name: "CargoReceived",
            aggregate: "DELTA PRIME",
            error: "cassandra write timeout after 3 retries",
        },
    ]
}

/// Mission 04 -- The Cassandra Incident (Dead Letters)
#[component]
pub fn DeadLetterScenario(close_signal: RwSignal<bool>) -> impl IntoView {
    let current_step: RwSignal<usize> = RwSignal::new(0);
    let log: RwSignal<VecDeque<ScenarioLogEntry>> = RwSignal::new(VecDeque::new());
    let narr_title = RwSignal::new(String::from("Cassandra node failure detected"));
    let narr_body = RwSignal::new(String::from(
        "A Cassandra storage node has gone dark. Events that need to be written to the event \
         store are failing. Canon's event store consumer will retry each event up to 3 times, \
         tracking the attempt count in a crash-safe retry_attempts table. After 3 failures, \
         the event is parked in the dead letter store rather than dropped.",
    ));
    let success_title: RwSignal<String> = RwSignal::new(String::new());
    let success_body: RwSignal<String> = RwSignal::new(String::new());

    let card_states: [RwSignal<CardState>; 3] = [
        RwSignal::new(CardState::Active),
        RwSignal::new(CardState::Active),
        RwSignal::new(CardState::Active),
    ];

    let button_disabled = RwSignal::new(false);
    let cards_visible = RwSignal::new(false);
    let requeue_count = RwSignal::new(0u32);
    let completed = RwSignal::new(false);

    let start_failures = move |_| {
        if button_disabled.get_untracked() {
            return;
        }
        button_disabled.set(true);
        current_step.set(1);
        narr_title.set("Failures accumulating \u{2014} retry counts ticking up".into());
        narr_body.set(
            "Three events have failed all 3 retry attempts and been dead-lettered. The \
             Cassandra node is still down. You can requeue them once the node recovers \u{2014} \
             Canon will re-enter them into the inbox with a fresh TTL."
                .into(),
        );

        let corr = fresh_corr();
        let entries = dead_letter_entries();

        let mut total_delay = 0u32;
        for entry in &entries {
            let name = entry.event_name;
            let agg = entry.aggregate;
            for attempt in 1..=3 {
                let corr = corr.clone();
                let msg = format!("RetryAttempt{}:{}", attempt, name);
                let agg = agg.to_string();
                let delay = total_delay;
                let cb = gloo_timers::callback::Timeout::new(delay, move || {
                    push_sc_log(log, "fleet", "sf", &msg, &agg, &corr);
                });
                cb.forget();
                total_delay += 400;
            }
            let corr = corr.clone();
            let msg = format!("{} \u{2192} DeadLetter", name);
            let agg = agg.to_string();
            let delay = total_delay;
            let cb = gloo_timers::callback::Timeout::new(delay, move || {
                push_sc_log(log, "fleet", "sf", &msg, &agg, &corr);
            });
            cb.forget();
            total_delay += 400;
        }

        let show_delay = total_delay + 200;
        let cb = gloo_timers::callback::Timeout::new(show_delay, move || {
            cards_visible.set(true);
            current_step.set(2);
        });
        cb.forget();
    };

    let check_all_requeued = move || {
        let count = requeue_count.get_untracked();
        if count >= 3 {
            let cb = gloo_timers::callback::Timeout::new(1500, move || {
                current_step.set(4);
                completed.set(true);
                success_title.set("All events recovered".into());
                success_body.set(
                    "Three dead-lettered events were requeued, re-entered the inbox, passed \
                     through oversight, and were successfully written to Cassandra. No data \
                     was lost. The dead letter store is the safety net \u{2014} it catches \
                     what retry logic cannot."
                        .into(),
                );
            });
            cb.forget();
        }
    };

    let entries = dead_letter_entries();

    view! {
        <ScenarioRunner
            title="Mission 04 \u{2014} The Cassandra Incident"
            steps=vec!["Node Down", "Failures", "Dead Letters", "Requeue", "Recovered"]
            current_step=current_step
            log=log
            close_signal=close_signal
            narr_title=narr_title
            narr_body=narr_body
            success_title=success_title
            success_body=success_body
        >
            <Show when=move || !cards_visible.get()>
                <button
                    class="sc-big-btn"
                    on:click=start_failures
                    disabled=move || button_disabled.get()
                >
                    "Take Cassandra Node Offline \u{2192}"
                </button>
            </Show>

            <Show when=move || cards_visible.get()>
                <div style="width:100%;max-width:380px;">
                    <div style="font-family:var(--sans);font-size:12px;font-weight:600;letter-spacing:.1em;color:var(--txthi);margin-bottom:12px;text-transform:uppercase;">
                        "Dead Letter Store \u{2014} 3 entries"
                    </div>
                    {entries
                        .iter()
                        .enumerate()
                        .map(|(i, entry)| {
                            let card_state = card_states[i];
                            let event_name = entry.event_name;
                            let aggregate = entry.aggregate;
                            let error_msg = entry.error;
                            let card_class = move || {
                                match card_state.get() {
                                    CardState::Active => "dl-card",
                                    CardState::Requeued => "dl-card requeued",
                                    CardState::Discarded => "dl-card discarded",
                                }
                            };
                            let on_requeue = move |_| {
                                if card_state.get_untracked() != CardState::Active {
                                    return;
                                }
                                card_state.set(CardState::Requeued);
                                current_step.set(3);
                                let corr = fresh_corr();
                                let corr2 = corr.clone();
                                let corr3 = corr.clone();
                                push_sc_log(
                                    log,
                                    "fleet",
                                    "sf",
                                    &format!("{} \u{2192} Requeued", event_name),
                                    aggregate,
                                    &corr,
                                );
                                let cb1 = gloo_timers::callback::Timeout::new(600, move || {
                                    push_sc_log(
                                        log,
                                        "fleet",
                                        "sf",
                                        &format!("{} \u{2192} Processing", event_name),
                                        aggregate,
                                        &corr2,
                                    );
                                });
                                cb1.forget();
                                let cb2 = gloo_timers::callback::Timeout::new(1100, move || {
                                    push_sc_log(
                                        log,
                                        "fleet",
                                        "sf",
                                        &format!("{} \u{2192} Written", event_name),
                                        aggregate,
                                        &corr3,
                                    );
                                });
                                cb2.forget();
                                requeue_count.update(|c| *c += 1);
                                check_all_requeued();
                            };
                            let on_discard = move |_| {
                                if card_state.get_untracked() != CardState::Active {
                                    return;
                                }
                                card_state.set(CardState::Discarded);
                            };
                            view! {
                                <div class=card_class>
                                    <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                                        <span style="font-family:var(--body);font-size:11px;font-weight:600;color:var(--red);">
                                            {event_name}
                                        </span>
                                        <span style="font-family:var(--mono);font-size:9px;color:var(--txtlo);">
                                            "3 attempts"
                                        </span>
                                    </div>
                                    <div style="font-family:var(--mono);font-size:9px;color:var(--txtlo);margin-bottom:6px;text-overflow:ellipsis;overflow:hidden;white-space:nowrap;">
                                        {error_msg}
                                    </div>
                                    <Show when=move || card_state.get() == CardState::Active>
                                        <div style="display:flex;gap:6px;">
                                            <button class="sc-sub-btn" on:click=on_requeue>
                                                "Requeue"
                                            </button>
                                            <button
                                                class="sc-sub-btn"
                                                style="border-color:var(--reddim);color:var(--reddim);"
                                                on:click=on_discard
                                            >
                                                "Discard"
                                            </button>
                                        </div>
                                    </Show>
                                    <Show when=move || card_state.get() == CardState::Requeued>
                                        <div style="font-family:var(--mono);font-size:9px;color:var(--greendim);">
                                            {"\u{2713} requeued"}
                                        </div>
                                    </Show>
                                </div>
                            }
                        })
                        .collect::<Vec<_>>()}
                </div>
            </Show>

            <Show when=move || completed.get()>
                <button class="sc-sub-btn" on:click=move |_| close_signal.set(true)>
                    "Return to Scenarios"
                </button>
            </Show>
        </ScenarioRunner>
    }
}
