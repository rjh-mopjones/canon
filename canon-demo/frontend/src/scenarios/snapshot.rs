use crate::scenarios::runner::{fresh_corr, push_sc_log, ScenarioLogEntry, ScenarioRunner};
use leptos::prelude::*;
use std::collections::VecDeque;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Assess,
    ColdHydration,
    SnapshotWrite,
    HotHydration,
    Compare,
}

/// Mission 02 -- The Ghost Ship (Snapshotting)
#[component]
pub fn SnapshotScenario(close_signal: RwSignal<bool>) -> impl IntoView {
    let current_step: RwSignal<usize> = RwSignal::new(0);
    let log: RwSignal<VecDeque<ScenarioLogEntry>> = RwSignal::new(VecDeque::new());
    let narr_title = RwSignal::new(String::from(
        "VSS Herald \u{2014} 247 logged events, no snapshot",
    ));
    let narr_body = RwSignal::new(String::from(
        "This ship has been drifting offline for years. When we reactivate it, Canon must \
         hydrate its aggregate state by replaying every single event from version 0 through to \
         version 247. There is no snapshot. Watch what that costs.",
    ));
    let success_title: RwSignal<String> = RwSignal::new(String::new());
    let success_body: RwSignal<String> = RwSignal::new(String::new());

    let phase = RwSignal::new(Phase::Assess);
    let cold_counter = RwSignal::new(0u64);
    let cold_done = RwSignal::new(false);
    let cold_ms = RwSignal::new(0u32);
    let snap_progress = RwSignal::new(0u32);
    let snap_done = RwSignal::new(false);
    let hot_done = RwSignal::new(false);
    let hot_ms = RwSignal::new(0u32);
    let slow_bar_width = RwSignal::new(String::from("0%"));
    let fast_bar_width = RwSignal::new(String::from("0%"));
    let speedup_text = RwSignal::new(String::new());
    let button_disabled = RwSignal::new(false);

    // Start cold hydration
    let start_cold = move |_| {
        if button_disabled.get_untracked() {
            return;
        }
        button_disabled.set(true);
        phase.set(Phase::ColdHydration);
        current_step.set(1);
        narr_title.set("Replaying 247 events from scratch".into());
        narr_body.set(
            "Canon is hydrating the aggregate by walking every event in order, applying each \
             one to rebuild current state. Watch the counter. This is what happens without a \
             snapshot."
                .into(),
        );

        let corr = fresh_corr();
        push_sc_log(
            log,
            "fleet",
            "sf",
            "ReactivationRequested",
            "VSS HERALD",
            &corr,
        );

        let corr_for_tick = corr.clone();
        let tick_cb = Closure::<dyn FnMut()>::new(move || {
            let current = cold_counter.get_untracked();
            let increment = (js_sys::Math::random() * 4.0) as u64 + 3;
            let next = (current + increment).min(247);
            cold_counter.set(next);

            if next.is_multiple_of(30) && next > 0 && next < 247 {
                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "EventReplayed",
                    &format!("VSS HERALD v{}", next),
                    &corr_for_tick,
                );
            }

            if next >= 247 {
                cold_counter.set(247);
                let display_ms = 620 + (js_sys::Math::random() * 120.0) as u32;
                cold_ms.set(display_ms);
                cold_done.set(true);

                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "AggregateHydrated",
                    "VSS HERALD v247",
                    &corr_for_tick,
                );

                let cb = gloo_timers::callback::Timeout::new(1200, move || {
                    phase.set(Phase::SnapshotWrite);
                    current_step.set(2);
                    narr_title.set("247 events replayed. Now take a snapshot.".into());
                    narr_body.set(
                        "That took 640ms to replay 247 events. Now we'll write a snapshot at \
                         the current version. Next time Herald needs to hydrate, it will load \
                         the snapshot directly and only replay events since the snapshot \u{2014} \
                         in this case, zero."
                            .into(),
                    );
                    button_disabled.set(false);
                });
                cb.forget();
            }
        });

        if let Some(window) = web_sys::window() {
            let interval_id = window
                .set_interval_with_callback_and_timeout_and_arguments_0(
                    tick_cb.as_ref().unchecked_ref(),
                    40,
                )
                .unwrap_or(0);

            let clear_cb = Closure::<dyn FnMut()>::new(move || {
                if cold_counter.get_untracked() >= 247 {
                    if let Some(w) = web_sys::window() {
                        w.clear_interval_with_handle(interval_id);
                    }
                }
            });
            let _ = window.set_interval_with_callback_and_timeout_and_arguments_0(
                clear_cb.as_ref().unchecked_ref(),
                50,
            );
            clear_cb.forget();
        }
        tick_cb.forget();
    };

    // Start snapshot write
    let start_snapshot = move |_| {
        if button_disabled.get_untracked() {
            return;
        }
        button_disabled.set(true);
        current_step.set(3);

        let corr = fresh_corr();
        push_sc_log(
            log,
            "fleet",
            "sf",
            "SnapshotWriting",
            "VSS HERALD v247",
            &corr,
        );

        let corr_for_done = corr.clone();
        let tick_cb = Closure::<dyn FnMut()>::new(move || {
            let current = snap_progress.get_untracked();
            let next = (current + 8).min(100);
            snap_progress.set(next);

            if next >= 100 {
                snap_done.set(true);
                push_sc_log(
                    log,
                    "fleet",
                    "sf",
                    "SnapshotWritten",
                    "VSS HERALD v247",
                    &corr_for_done,
                );

                let cb = gloo_timers::callback::Timeout::new(900, move || {
                    phase.set(Phase::HotHydration);
                    current_step.set(3);
                    narr_title.set("Snapshot written. Now reactivate from cold again.".into());
                    narr_body.set(
                        "We'll send Herald offline and reactivate it a second time. This time \
                         Canon loads the snapshot at v247, skips all 247 events, and hydrates \
                         instantly. Compare the two numbers."
                            .into(),
                    );

                    let corr2 = fresh_corr();
                    push_sc_log(
                        log,
                        "fleet",
                        "sf",
                        "ReactivationRequested",
                        "VSS HERALD",
                        &corr2,
                    );

                    let corr2a = corr2.clone();
                    let corr2b = corr2.clone();

                    let cb2 = gloo_timers::callback::Timeout::new(600, move || {
                        push_sc_log(
                            log,
                            "fleet",
                            "sf",
                            "SnapshotLoaded",
                            "VSS HERALD v247",
                            &corr2a,
                        );
                    });
                    cb2.forget();

                    let cb3 = gloo_timers::callback::Timeout::new(900, move || {
                        let fast = 28 + (js_sys::Math::random() * 12.0) as u32;
                        hot_ms.set(fast);
                        hot_done.set(true);
                        push_sc_log(
                            log,
                            "fleet",
                            "sf",
                            "AggregateHydrated",
                            "VSS HERALD v247",
                            &corr2b,
                        );

                        let cb4 = gloo_timers::callback::Timeout::new(600, move || {
                            phase.set(Phase::Compare);
                            current_step.set(4);

                            let cb5 = gloo_timers::callback::Timeout::new(100, move || {
                                slow_bar_width.set("100%".into());
                            });
                            cb5.forget();

                            let fast_pct = ((fast as f64 / 640.0) * 100.0).round() as u32;
                            let cb6 = gloo_timers::callback::Timeout::new(600, move || {
                                fast_bar_width.set(format!("{}%", fast_pct));
                            });
                            cb6.forget();

                            let speedup = 640 / fast.max(1);
                            let cb7 = gloo_timers::callback::Timeout::new(1800, move || {
                                speedup_text.set(format!("{}\u{00d7} faster hydration", speedup));
                                success_title.set("Snapshot performance demonstrated".into());
                                success_body.set(format!(
                                    "Without snapshot: 640ms replaying 247 events. \
                                     With snapshot: {}ms loading state directly. Canon writes \
                                     snapshots automatically every 50 events via the event \
                                     store consumer \u{2014} zero application code required.",
                                    fast
                                ));
                            });
                            cb7.forget();
                        });
                        cb4.forget();
                    });
                    cb3.forget();
                });
                cb.forget();
            }
        });

        if let Some(window) = web_sys::window() {
            let interval_id = window
                .set_interval_with_callback_and_timeout_and_arguments_0(
                    tick_cb.as_ref().unchecked_ref(),
                    30,
                )
                .unwrap_or(0);

            let clear_cb = Closure::<dyn FnMut()>::new(move || {
                if snap_progress.get_untracked() >= 100 {
                    if let Some(w) = web_sys::window() {
                        w.clear_interval_with_handle(interval_id);
                    }
                }
            });
            let _ = window.set_interval_with_callback_and_timeout_and_arguments_0(
                clear_cb.as_ref().unchecked_ref(),
                50,
            );
            clear_cb.forget();
        }
        tick_cb.forget();
    };

    let cold_counter_style = move || {
        if cold_done.get() {
            "color:var(--amber)"
        } else {
            "color:var(--cyan)"
        }
    };

    let cold_status_text = move || {
        if cold_done.get() {
            format!("hydration complete \u{2014} {}ms", cold_ms.get())
        } else {
            let c = cold_counter.get();
            if c == 0 {
                "awaiting reactivation command".to_string()
            } else {
                format!("replaying event v{}\u{2026}", c)
            }
        }
    };

    let cold_bar_width = move || format!("{}%", (cold_counter.get() as f64 / 247.0 * 100.0) as u32);

    let snap_count_display = move || ((snap_progress.get() as f64 / 100.0) * 247.0).round() as u32;

    let snap_bar_width = move || format!("{}%", snap_progress.get());

    let snap_status_text = move || {
        if snap_done.get() {
            "snapshot written \u{2014} YugabyteDB".to_string()
        } else {
            "ready to snapshot".to_string()
        }
    };

    let slow_bar_style =
        move || format!("width:{};transition:width 1.2s ease;", slow_bar_width.get());

    let fast_bar_style =
        move || format!("width:{};transition:width 0.8s ease;", fast_bar_width.get());

    let fast_ms_display = move || hot_ms.get();

    view! {
        <ScenarioRunner
            title="Mission 02 \u{2014} The Ghost Ship"
            steps=vec!["Assess", "Replay Cold", "Snapshot", "Replay Hot", "Compare"]
            current_step=current_step
            log=log
            close_signal=close_signal
            narr_title=narr_title
            narr_body=narr_body
            success_title=success_title
            success_body=success_body
        >
            <Show when=move || matches!(phase.get(), Phase::Assess | Phase::ColdHydration)>
                <div class="hydrate-viz">
                    <div class="hv-counter" style=cold_counter_style>
                        {move || cold_counter.get()}
                    </div>
                    <div class="hv-label">"events to replay"</div>
                    <div class="hv-bar">
                        <div class="hv-fill" style=move || format!("width:{}", cold_bar_width())></div>
                    </div>
                    <div class="hv-status">{cold_status_text}</div>
                </div>
                <Show when=move || phase.get() == Phase::Assess>
                    <button class="sc-big-btn" on:click=start_cold>
                        "Reactivate VSS Herald (Cold) \u{2192}"
                    </button>
                </Show>
            </Show>

            <Show when=move || phase.get() == Phase::SnapshotWrite>
                <div class="hydrate-viz">
                    <div class="hv-counter" style="color:var(--green)">{snap_count_display}</div>
                    <div class="hv-label">"events written to snapshot"</div>
                    <div class="hv-bar">
                        <div
                            class="hv-fill"
                            style=move || format!("width:{};background:var(--green)", snap_bar_width())
                        ></div>
                    </div>
                    <div class="hv-status">{snap_status_text}</div>
                </div>
                <Show when=move || !button_disabled.get()>
                    <button class="sc-big-btn" on:click=start_snapshot>
                        "Write Snapshot at v247 \u{2192}"
                    </button>
                </Show>
            </Show>

            <Show when=move || phase.get() == Phase::HotHydration>
                <div class="hydrate-viz">
                    <div
                        class="hv-counter"
                        style=move || {
                            if hot_done.get() {
                                "color:var(--green);animation:flash 0.4s ease-out;"
                            } else {
                                "color:var(--cyan)"
                            }
                        }
                    >
                        {move || if hot_done.get() { 247u64 } else { 0u64 }}
                    </div>
                    <div class="hv-label">"events (from snapshot)"</div>
                    <div class="hv-bar">
                        <div
                            class="hv-fill"
                            style=move || {
                                if hot_done.get() {
                                    "width:100%;background:var(--green);transition:none;"
                                } else {
                                    "width:0%;background:var(--green);"
                                }
                            }
                        ></div>
                    </div>
                    <div class="hv-status">
                        {move || {
                            if hot_done.get() {
                                format!("{}ms \u{2014} snapshot loaded", hot_ms.get())
                            } else {
                                "loading snapshot\u{2026}".to_string()
                            }
                        }}
                    </div>
                </div>
            </Show>

            <Show when=move || phase.get() == Phase::Compare>
                <div class="perf-viz">
                    <div class="perf-row">
                        <div class="perf-lbl">"Without snapshot"</div>
                        <div class="perf-bar-wrap">
                            <div class="perf-bar slow" style=slow_bar_style>
                                <span class="perf-bar-txt">"640ms \u{2014} 247 events"</span>
                            </div>
                        </div>
                    </div>
                    <div class="perf-row">
                        <div class="perf-lbl">"With snapshot"</div>
                        <div class="perf-bar-wrap">
                            <div class="perf-bar fast" style=fast_bar_style>
                                <span class="perf-bar-txt">
                                    {move || format!("{}ms \u{2014} 0 events", fast_ms_display())}
                                </span>
                            </div>
                        </div>
                    </div>
                    <Show when=move || !speedup_text.get().is_empty()>
                        <div class="perf-speedup">{move || speedup_text.get()}</div>
                    </Show>
                </div>
                <button class="sc-sub-btn" on:click=move |_| close_signal.set(true)>
                    "Return to Scenarios"
                </button>
            </Show>
        </ScenarioRunner>
    }
}
