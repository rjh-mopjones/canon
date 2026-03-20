use leptos::prelude::*;
use std::collections::VecDeque;

/// A log entry in the scenario event log.
#[derive(Debug, Clone)]
pub struct ScenarioLogEntry {
    pub service: &'static str,
    pub svc_class: &'static str,
    pub event_name: String,
    pub aggregate_id: String,
    pub correlation_id: String,
    pub timestamp: String,
    pub version: u32,
    pub fresh: bool,
}

/// Maximum number of scenario log entries to keep.
const MAX_SC_LOG: usize = 60;

/// Generate a simple correlation ID for scenario events.
fn new_corr() -> String {
    let pool = ["A4F", "B7C", "C2E", "D9A", "E5B", "F1D", "G8F", "H3C"];
    let idx = (js_sys::Math::random() * pool.len() as f64) as usize;
    let suffix = js_sys::Math::random().to_string();
    let short = if suffix.len() > 5 {
        &suffix[2..5]
    } else {
        "000"
    };
    format!("{}-{}", pool.get(idx).unwrap_or(&"X0X"), short)
}

/// Get current timestamp string.
fn now_ts() -> String {
    let d = js_sys::Date::new_0();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        d.get_hours(),
        d.get_minutes(),
        d.get_seconds(),
        d.get_milliseconds(),
    )
}

/// Push a log entry into the scenario log signal.
pub fn push_sc_log(
    log: RwSignal<VecDeque<ScenarioLogEntry>>,
    service: &'static str,
    svc_class: &'static str,
    event_name: &str,
    aggregate_id: &str,
    correlation_id: &str,
) {
    log.update(|entries| {
        entries.push_front(ScenarioLogEntry {
            service,
            svc_class,
            event_name: event_name.to_string(),
            aggregate_id: aggregate_id.to_string(),
            correlation_id: correlation_id.to_string(),
            timestamp: now_ts(),
            version: (js_sys::Math::random() * 50.0) as u32 + 1,
            fresh: true,
        });
        while entries.len() > MAX_SC_LOG {
            entries.pop_back();
        }
    });
}

/// Generate a fresh correlation ID.
pub fn fresh_corr() -> String {
    new_corr()
}

/// Scenario runner -- the full-screen modal that wraps each scenario's visualization.
///
/// Uses `close_signal` (RwSignal<bool>) instead of a callback: set to `true` to request close.
#[component]
pub fn ScenarioRunner(
    title: &'static str,
    steps: Vec<&'static str>,
    current_step: RwSignal<usize>,
    log: RwSignal<VecDeque<ScenarioLogEntry>>,
    close_signal: RwSignal<bool>,
    narr_title: RwSignal<String>,
    narr_body: RwSignal<String>,
    success_title: RwSignal<String>,
    success_body: RwSignal<String>,
    children: Children,
) -> impl IntoView {
    let steps_clone = steps.clone();
    let step_count = steps.len();

    view! {
        <div class="sc-runner open">
            <div class="sc-runner-hdr">
                <div class="sc-runner-title">{title}</div>
                <div class="sc-close" on:click=move |_| close_signal.set(true)>
                    {"\u{2715} Close"}
                </div>
            </div>
            <div class="sc-body">
                <div class="sc-stage">
                    // Step bar
                    <div class="step-bar">
                        {(0..step_count)
                            .map(|i| {
                                let label = steps_clone[i];
                                let step_class = move || {
                                    let cs = current_step.get();
                                    if i < cs {
                                        "step done"
                                    } else if i == cs {
                                        "step active"
                                    } else {
                                        "step"
                                    }
                                };
                                let dot_text = move || {
                                    if current_step.get() > i {
                                        "\u{2713}".to_string()
                                    } else {
                                        (i + 1).to_string()
                                    }
                                };
                                let show_line = i < step_count - 1;
                                view! {
                                    <div class=step_class>
                                        <div class="step-dot">{dot_text}</div>
                                        <div class="step-lbl">{label}</div>
                                    </div>
                                    {if show_line {
                                        Some(view! { <div class="step-line"></div> })
                                    } else {
                                        None
                                    }}
                                }
                            })
                            .collect::<Vec<_>>()}
                    </div>
                    // Narrative
                    <div class="sc-narr">
                        <div class="sc-narr-title">{move || narr_title.get()}</div>
                        <div class="sc-narr-body">{move || narr_body.get()}</div>
                    </div>
                    // Success banner
                    <div class=move || {
                        if success_title.get().is_empty() {
                            "success-banner"
                        } else {
                            "success-banner show"
                        }
                    }>
                        <strong>{move || success_title.get()}</strong>
                        {move || success_body.get()}
                    </div>
                    // Action area
                    <div class="sc-action-area">{children()}</div>
                </div>
                // Scenario event log
                <div class="sc-log-wrap">
                    <div class="sb-bar">
                        <div class="bar-lbl">"Event Log"</div>
                        <div class="live-badge">
                            <div class="dot"></div>
                            "Live"
                        </div>
                    </div>
                    <div class="sc-log-inner">
                        <For
                            each=move || {
                                log.get().iter().cloned().enumerate().collect::<Vec<_>>()
                            }
                            key=|(_i, e)| {
                                format!("{}-{}-{}", e.timestamp, e.event_name, e.aggregate_id)
                            }
                            children=move |(i, entry)| {
                                let item_class = if i == 0 && entry.fresh {
                                    "log-item fresh"
                                } else {
                                    "log-item"
                                };
                                view! {
                                    <div class=item_class>
                                        <div class="log-ts">
                                            {format!(
                                                "{}\u{00b7}v{}",
                                                entry.timestamp,
                                                entry.version,
                                            )}
                                        </div>
                                        <div class="log-row">
                                            <span class=format!(
                                                "svc {}",
                                                entry.svc_class,
                                            )>{entry.service}</span>
                                            <span class="log-name">
                                                {entry.event_name.clone()}
                                            </span>
                                        </div>
                                        <div class="log-agg">
                                            {entry.aggregate_id.clone()}
                                        </div>
                                    </div>
                                    <div class="log-div"></div>
                                }
                            }
                        />
                    </div>
                </div>
            </div>
        </div>
    }
}
