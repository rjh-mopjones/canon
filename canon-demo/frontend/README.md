# canon-demo / frontend

Leptos 0.7 CSR WASM application for the Canon Fleet Ops demo. Built with Trunk.

## Structure

- `src/app.rs` — Root `App` component, header, top nav, page wrappers
- `src/pages/scenarios.rs` — Scenarios page with mission cards and runner modal
- `src/state.rs` — Global `AppState` (Leptos signals), scenario definitions, types
- `src/ws.rs` — WebSocket connection to the gateway (`WS /events`)
- `src/hydrate.rs` — Initial state hydration from gateway REST endpoints
- `style/main.css` — Complete CSS design system (dark/light themes)
- `index.html` — HTML shell with Google Fonts and Trunk asset links

## Pages

- **Live Fleet** — Autonomous ship simulation with map, oversight strip, and live event log
- **Scenarios** — Five interactive missions demonstrating Canon features (oversight gates, snapshotting, cross-service cascade, dead letters, idempotency)

## Building

```sh
trunk build --release
```

Requires `trunk` and `wasm-bindgen-cli` installed. Output goes to `dist/`.

## Design system

All colours are defined as CSS custom properties on `:root` (dark) and `body.light` (light theme). Fonts: Inter (sans-serif) and JetBrains Mono (monospace), loaded from Google Fonts via `--sans` and `--mono` CSS variables. Theme toggle adds/removes the `light` class on `<body>`.

## Dependencies

See `Cargo.toml`. Key crates: `leptos` (CSR), `web-sys`, `gloo-net`, `gloo-timers`, `serde`, `uuid`, `canon-demo-shared`.
