use leptos::prelude::*;

use crate::pages::scenarios::{ScenarioRunner, ScenariosPage};
use crate::state::{AppState, InfraStatus, Page};

/// Root application component.
#[component]
pub fn App() -> impl IntoView {
    let state = AppState::new();
    provide_context(state.clone());

    view! {
        <Header />
        <TopNav />
        <LiveFleetPage />
        <ScenariosPageWrapper />
        <ScenarioRunner />
    }
}

/// Application header with logo, infra status dots, and theme toggle.
#[component]
fn Header() -> impl IntoView {
    let state = expect_context::<AppState>();

    let toggle_theme = move |_| {
        state.light_mode.update(|lm| *lm = !*lm);
        // Toggle body class for CSS
        if let Some(document) = web_sys::window().and_then(|w| w.document()) {
            if let Some(body) = document.body() {
                if state.light_mode.get() {
                    let _ = body.class_list().add_1("light");
                } else {
                    let _ = body.class_list().remove_1("light");
                }
            }
        }
    };

    let theme_label = move || {
        if state.light_mode.get() {
            "Dark"
        } else {
            "Light"
        }
    };

    let kafka_dot_class = move || {
        if state.kafka_status.get() == InfraStatus::Connected {
            "dot"
        } else {
            "dot r"
        }
    };

    let yugabyte_dot_class = move || {
        if state.yugabyte_status.get() == InfraStatus::Connected {
            "dot"
        } else {
            "dot r"
        }
    };

    let cassandra_dot_class = move || {
        if state.cassandra_status.get() == InfraStatus::Connected {
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
                        <div class=kafka_dot_class></div>
                        "Kafka"
                    </div>
                    <div class="ii">
                        <div class=yugabyte_dot_class></div>
                        "YugabyteDB"
                    </div>
                    <div class="ii">
                        <div class=cassandra_dot_class></div>
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

/// Top navigation tab bar.
#[component]
fn TopNav() -> impl IntoView {
    let state = expect_context::<AppState>();

    let on_live = {
        let state = state.clone();
        move |_| state.active_page.set(Page::LiveFleet)
    };

    let on_scenarios = {
        let state = state.clone();
        move |_| state.active_page.set(Page::Scenarios)
    };

    let live_class = move || {
        if state.active_page.get() == Page::LiveFleet {
            "tnav-tab active"
        } else {
            "tnav-tab"
        }
    };

    let scenarios_class = move || {
        if state.active_page.get() == Page::Scenarios {
            "tnav-tab active"
        } else {
            "tnav-tab"
        }
    };

    view! {
        <div class="top-nav">
            <div class=live_class on:click=on_live>"Live Fleet"</div>
            <div class=scenarios_class on:click=on_scenarios>"Scenarios"</div>
        </div>
    }
}

/// Live Fleet page placeholder. Full implementation in a separate issue.
#[component]
fn LiveFleetPage() -> impl IntoView {
    let state = expect_context::<AppState>();

    let page_class = move || {
        if state.active_page.get() == Page::LiveFleet {
            "page live-page active"
        } else {
            "page live-page"
        }
    };

    view! {
        <div class=page_class>
            <div class="map-wrap">
                <div class="map-bar">
                    <div class="bar-lbl">"Active Fleet"</div>
                    <div class="pills">
                        <span class="pill pg">"\u{25b2} Docked 0"</span>
                        <span class="pill pc">"\u{25b6} Transit 0"</span>
                        <span class="pill pr">"\u{2715} Offline 1"</span>
                    </div>
                </div>
                <div class="map-canvas">
                    // Map content will be rendered here (separate issue)
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
                <div class="log-body">
                    // Live event log entries will go here
                </div>
                <div class="sb-footer">
                    "Click any event to trace its correlation chain"
                </div>
            </div>
        </div>
    }
}

/// Wrapper for the Scenarios page with page-level show/hide.
#[component]
fn ScenariosPageWrapper() -> impl IntoView {
    let state = expect_context::<AppState>();

    let page_class = move || {
        if state.active_page.get() == Page::Scenarios {
            "page scenarios-page active"
        } else {
            "page scenarios-page"
        }
    };

    view! {
        <div class=page_class>
            <ScenariosPage />
        </div>
    }
}
