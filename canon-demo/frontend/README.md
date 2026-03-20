# canon-demo / frontend

Leptos 0.7 CSR WASM frontend for the Canon Fleet Ops demo.

## Overview

This crate provides the browser-based UI for the Canon demo application. It connects to the gateway via REST (initial hydration) and WebSocket (live updates) to display real-time fleet operations and interactive Canon feature scenarios.

## Architecture

- **Leptos 0.7** with client-side rendering (CSR), built with **Trunk**
- **Two pages**: Live Fleet (autonomous ship map + event log) and Scenarios (5 interactive Canon feature missions)
- **Reactive state** via `RwSignal`-backed `AppState` — ships, stations, events, oversight windows, dead letters, infra status
- **WebSocket** connection to `ws://<gateway>/events` with 2-second reconnect backoff
- **REST hydration** on mount from `/ships`, `/stations`, `/admin/oversight/windows`, `/admin/deadletters`

## Design System

All styling uses CSS custom properties defined in `style/main.css`. Dark theme is the default; light theme is toggled via a `light` class on `<body>`.

Fonts (loaded from Google Fonts in `index.html`):
- **Share Tech Mono** — monospace readouts, timestamps, badges, labels
- **Rajdhani** — headings, panel titles, ship names, nav tabs
- **Exo 2** — body text, scenario narrative, descriptions

## Modules

| Module | Purpose |
|--------|---------|
| `app` | Component tree: Header, TopNav, PageContent, LiveFleetPage, ScenariosPage |
| `state` | Domain types and `AppState` struct with reactive signals |
| `ws` | WebSocket connection, `WsMessage` deserialization, signal dispatch |
| `hydrate` | Initial REST fetch from gateway endpoints on mount |

## Building

```bash
# Development
trunk serve

# Production
trunk build --release
```

Requires [Trunk](https://trunkrs.dev/) and the `wasm32-unknown-unknown` target:

```bash
cargo install trunk
rustup target add wasm32-unknown-unknown
```

## Reference

The authoritative visual reference is `reference/mockup.html`. Open it in a browser to see the target design.
