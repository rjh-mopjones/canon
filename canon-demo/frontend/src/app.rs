use crate::state::{AppState, Page};
use leptos::prelude::*;

/// Provide AppState to the entire component tree.
#[component]
pub fn App(state: AppState) -> impl IntoView {
    view! {
        <Header state=state.clone() />
        <TopNav state=state.clone() />
        <PageContent state=state />
    }
}

/// Top header bar: CANON wordmark, infra status dots, theme toggle.
#[component]
fn Header(state: AppState) -> impl IntoView {
    let infra = state.infra_status;
    let light_mode = state.light_mode;

    let toggle_theme = move |_| {
        let new_mode = !light_mode.get_untracked();
        light_mode.set(new_mode);
        if let Some(doc) = web_sys::window().and_then(|w| w.document()) {
            if let Some(body) = doc.body() {
                if new_mode {
                    let _ = body.class_list().add_1("light");
                } else {
                    let _ = body.class_list().remove_1("light");
                }
            }
        }
    };

    let theme_label = move || {
        if light_mode.get() {
            "Dark"
        } else {
            "Light"
        }
    };

    let kafka_class = move || {
        if infra.get().kafka {
            "dot"
        } else {
            "dot r"
        }
    };
    let yugabyte_class = move || {
        if infra.get().yugabyte {
            "dot"
        } else {
            "dot r"
        }
    };
    let cassandra_class = move || {
        if infra.get().cassandra {
            "dot"
        } else {
            "dot r"
        }
    };

    view! {
        <header>
            <div style="display:flex;align-items:baseline;">
                <div class="logo">
                    "C"<span class="a">"A"</span>"NON"
                </div>
                <div class="logo-sub">"/ fleet ops demo"</div>
            </div>
            <div class="hdr-r">
                <div class="infra">
                    <div class="ii">
                        <div class=kafka_class></div>
                        "Kafka"
                    </div>
                    <div class="ii">
                        <div class=yugabyte_class></div>
                        "YugabyteDB"
                    </div>
                    <div class="ii">
                        <div class=cassandra_class></div>
                        "Cassandra"
                    </div>
                </div>
                <div class="theme-btn" on:click=toggle_theme>
                    <div class="trk">
                        <div class="thmb"></div>
                    </div>
                    <span>{theme_label}</span>
                </div>
            </div>
        </header>
    }
}

/// Top navigation tab bar: Live Fleet / Scenarios.
#[component]
fn TopNav(state: AppState) -> impl IntoView {
    let active_page = state.active_page;

    let live_class = move || {
        if active_page.get() == Page::LiveFleet {
            "tnav-tab active"
        } else {
            "tnav-tab"
        }
    };
    let scenarios_class = move || {
        if active_page.get() == Page::Scenarios {
            "tnav-tab active"
        } else {
            "tnav-tab"
        }
    };

    let set_live = move |_| active_page.set(Page::LiveFleet);
    let set_scenarios = move |_| active_page.set(Page::Scenarios);

    view! {
        <div class="top-nav">
            <div class=live_class on:click=set_live>"Live Fleet"</div>
            <div class=scenarios_class on:click=set_scenarios>"Scenarios"</div>
        </div>
    }
}

/// Page content: switches between Live Fleet and Scenarios based on active_page.
#[component]
fn PageContent(state: AppState) -> impl IntoView {
    let active_page = state.active_page;

    let live_class = move || {
        if active_page.get() == Page::LiveFleet {
            "page live-page active"
        } else {
            "page live-page"
        }
    };
    let scenarios_class = move || {
        if active_page.get() == Page::Scenarios {
            "page scenarios-page active"
        } else {
            "page scenarios-page"
        }
    };

    view! {
        <div class=live_class id="page-live">
            <LiveFleetPage state=state.clone() />
        </div>
        <div class=scenarios_class id="page-scenarios">
            <ScenariosPage />
        </div>
    }
}

/// Live Fleet page placeholder with map and sidebar structure.
#[component]
fn LiveFleetPage(state: AppState) -> impl IntoView {
    let ships = state.ships;
    let events = state.events;
    let highlighted_correlation = state.highlighted_correlation;

    let docked_count = move || {
        ships
            .get()
            .iter()
            .filter(|s| s.status == crate::state::ShipStatus::Docked)
            .count()
    };
    let transit_count = move || {
        ships
            .get()
            .iter()
            .filter(|s| s.status == crate::state::ShipStatus::Transit)
            .count()
    };

    view! {
        <div class="map-wrap">
            <div class="map-bar">
                <div class="bar-lbl">"Active Fleet"</div>
                <div class="pills">
                    <span class="pill pg">
                        {move || format!("\u{25b2} Docked {}", docked_count())}
                    </span>
                    <span class="pill pc">
                        {move || format!("\u{25b6} Transit {}", transit_count())}
                    </span>
                    <span class="pill pr">{"\u{2715} Offline 1"}</span>
                </div>
            </div>
            <div class="map-canvas" id="mapCanvas">
                // Map canvas content will be rendered by future components
                // (stations, ships, SVG route lines, popup, oversight strip)
            </div>
        </div>
        <div class="sidebar">
            <div class="sb-bar">
                <div class="bar-lbl">"Live Activity"</div>
                <div class="live-badge">
                    <div class="dot"></div>
                    "Live"
                </div>
            </div>
            <div class="log-body" id="logBody">
                <For
                    each=move || {
                        events.get().iter().cloned().enumerate().collect::<Vec<_>>()
                    }
                    key=|(_i, e)| format!("{}-{}", e.timestamp, e.event_name)
                    children=move |(i, event)| {
                        let hl_corr = highlighted_correlation;
                        let corr_id = event.correlation_id.clone();
                        let corr_for_click = corr_id.clone();
                        let is_lit = move || {
                            hl_corr
                                .get()
                                .as_ref()
                                .map(|h| h == &corr_id)
                                .unwrap_or(false)
                        };
                        let item_class = move || {
                            let mut cls = String::from("log-item");
                            if is_lit() {
                                cls.push_str(" lit");
                            }
                            if i == 0 && event.fresh {
                                cls.push_str(" fresh");
                            }
                            cls
                        };
                        let on_click = move |_| {
                            let current = hl_corr.get_untracked();
                            if current.as_ref() == Some(&corr_for_click) {
                                hl_corr.set(None);
                            } else {
                                hl_corr.set(Some(corr_for_click.clone()));
                            }
                        };
                        view! {
                            <div class=item_class on:click=on_click>
                                <div class="log-ts">
                                    {format!(
                                        "{}\u{00b7}v{}",
                                        event.timestamp,
                                        event.version,
                                    )}
                                </div>
                                <div class="log-row">
                                    <span class=format!(
                                        "svc {}",
                                        event.service.css_class(),
                                    )>{event.service.display_name()}</span>
                                    <span class="log-name">
                                        {event.event_name.clone()}
                                    </span>
                                </div>
                                <div class="log-agg">
                                    {event.aggregate_id.clone()}
                                </div>
                                <div class="log-corr">
                                    {format!(
                                        "\u{21b3}{}",
                                        event.correlation_id.clone(),
                                    )}
                                </div>
                            </div>
                            <div class="log-div"></div>
                        }
                    }
                />
            </div>
            <div class="sb-footer">
                "Click any event to trace its "
                <a>"correlation chain"</a>
            </div>
        </div>
    }
}

/// Scenarios page placeholder with hero section and mission card grid.
#[component]
fn ScenariosPage() -> impl IntoView {
    let missions = vec![
        (
            "MISSION 01",
            "The Stranded Cargo",
            "\u{1f6f8} VSS ARGO \u{00b7} BETA RELAY",
            "VSS Argo has arrived at Beta Relay but unloading is blocked \u{2014} the cargo manifest hasn't been filed yet. The oversight gate is holding the command back. You need to supply the missing piece.",
            vec!["Oversight Gates", "Command Assembly", "NotReady \u{2192} Ready"],
        ),
        (
            "MISSION 02",
            "The Ghost Ship",
            "\u{1f480} VSS HERALD \u{00b7} deep space",
            "VSS Herald has been drifting offline for years, accumulating hundreds of logged events. Reactivating it means replaying every event from scratch \u{2014} unless you snapshot first.",
            vec!["Snapshotting", "Hydration Performance", "Event Replay"],
        ),
        (
            "MISSION 03",
            "The Resupply Crisis",
            "\u{26a0} GAMMA OUTPOST \u{00b7} critical stock",
            "Gamma Outpost has hit critical stock levels. A resupply chain needs to fire across all five services. Watch how a single event cascades into coordinated action across the fleet.",
            vec!["Cross-service Events", "Event Fan-out", "5-service Chain"],
        ),
        (
            "MISSION 04",
            "The Cassandra Incident",
            "\u{1f534} FLEET-WIDE \u{00b7} storage failure",
            "A Cassandra node goes dark mid-flight. Events start failing, retry counts tick up, and the dead letter queue fills. Can you requeue the failures and bring the fleet back to order?",
            vec!["Dead Letters", "Retry Handling", "Recovery"],
        ),
        (
            "MISSION 05",
            "The Duplicate Signal",
            "\u{1f501} VSS KRONOS \u{00b7} inbox",
            "A network hiccup causes VSS Kronos's departure command to arrive twice. Watch Canon's inbox idempotency layer silently deduplicate the second command \u{2014} no duplicate state, no double-fire.",
            vec!["Inbox Idempotency", "Deduplication", "Duplicate Commands"],
        ),
    ];

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
                .map(|(num, name, ship, desc, tags)| {
                    view! {
                        <div class="sc-card">
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
    }
}
