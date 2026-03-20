use leptos::prelude::*;

use crate::hydrate::hydrate_from_gateway;
use crate::pages::live_fleet::LiveFleetPage;
use crate::state::{create_app_state, ActiveTab, AppState};
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
                        view! {
                            <div class="content-area" style="padding: 36px 40px;">
                                <h2 style="font-family: var(--sans); color: var(--txthi);">
                                    "Canon Feature Scenarios"
                                </h2>
                                <p style="font-family: var(--body); color: var(--txt); margin-top: 8px;">
                                    "Scenario missions will be implemented in a future PR."
                                </p>
                            </div>
                        }
                            .into_any()
                    }
                }
            }}
        </div>
    }
}

#[component]
fn Header(state: AppState) -> impl IntoView {
    let infra = state.infra;

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

    view! {
        <div class="header">
            <div class="header-logo">
                "CANON"
                <span>"Fleet Ops"</span>
            </div>
            <div class="header-right">
                <div class="infra-dots">
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
