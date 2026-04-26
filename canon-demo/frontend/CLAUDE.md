# frontend — Claude Code guide

Leptos 0.7 CSR WASM, built with Trunk. Demo specifics: `../CLAUDE.md`. Framework: `../../CLAUDE.md`.

---

## Mockups (source of truth)

- Demo app (`/demo`): `reference/mockup.html` — Live Fleet + Scenarios.
- Landing page (`/`): `../../canon-site/reference/site-mockup.html`.

Open the relevant mockup in a browser before writing code.

- **Mockup is correct.** App differs → app is wrong. Extract CSS vars, colours, fonts, spacing, casing.
- **Don't mimic** mockup DOM/inline JS. Use idiomatic reactive signals + composable components. Behaviour identical.
- **Don't "improve"** away: no add/remove/rearrange of UI elements, fonts, colours, casing. A11y/responsive only if appearance/interaction unchanged.

---

## Hard rules

- **No local simulation.** Every state change (ship movement, stock, oversight, event log) is driven by real events from the Canon pipeline (command → outbox → Kafka → event store → WebSocket). No local timers, no hardcoded event chains, no fire-and-forget POST fallbacks. Gateway down → show connection error.
- **No interactive UI before `ready=true`.** Buttons/handlers disabled until the game projection returns `ready: true` (3–8s bootstrap). Disable all action buttons when `state.ships` is empty.
- **No silent early returns** in `depart_ship`, `load_cargo`, `deliver_cargo`. Every `return` must set `pending_command`, set `command_error`, or log to console. Click that does nothing = bug.
- **No hardcoded colours.** All via CSS custom properties.
- **No `unwrap()`/`expect()`** outside tests.

---

## Design system

Fonts (Google Fonts in `index.html`):
- `Inter` 400/500/600/700 — headings, body, labels.
- `JetBrains Mono` 400/600 — readouts, timestamps, badges, IDs, code.

CSS vars in `style/main.css` (`:root` dark, `body.light` light): `--bg`, `--panel`, `--raised`, `--border`, `--borderhi`, `--cyan`, `--green`, `--amber`, `--red`, `--purple`, `--txt`, `--txthi`, `--txtlo`, `--mono`, `--sans`. Light is default. Theme toggle adds/removes `.light` on `<body>`. Starfield fades to `opacity:0` in light.

---

## Layout

Two pages, nav tabs **inside the 56px header bar** (no separate nav row).

- **Live Fleet** (default tab): single ship (VSS Meridian), user-controlled. Stations drain over time. Layout: header → map bar → canvas map (planets/ship/routes drawn on canvas, oversight strip absolute-bottom) → station cards (4 equal-width) → ship action bar → event log strip (≤160px, scrollable).
- **Scenarios**: 5 mission cards → full-screen runner (step progress + narrative + action area + event log).

Station positions (% canvas): Alpha 18% 26% green r32 · Beta 68% 14% purple r22 (ringed) · Gamma 76% 68% coral r28 · Delta 24% 74% blue r20.

Supply loop Alpha→Beta→Gamma→Delta→Alpha. Drain per 3s tick: Alpha 0.15 (start 85%), Beta 0.20 (60%), Gamma 0.25 (40%), Delta 0.18 (75%). Correct delivery → +35%. 0% = game over.

---

## Scenario visualisations (animated, polished)

- **01 Stranded Cargo (Oversight)** — gate card, two requirement rows ○→✓, badge amber→green.
- **02 Ghost Ship (Snapshotting)** — counter 0→247 (~40ms tick) vs snapshot instant. Bar chart with speedup multiplier.
- **03 Resupply Crisis (Cascade)** — 5 service nodes light up in sequence with animated arrows.
- **04 Cassandra Incident (Dead letters)** — 3 DLQ cards with Requeue/Discard. Requeue red→green, discard fades.
- **05 Duplicate Signal (Idempotency)** — two identical envelope cards. Trigger dedups second with red X. "ON CONFLICT DO NOTHING".

---

## Data sources

Initial hydration:
```
GET /ships, /stations, /admin/oversight/windows, /admin/deadletters
```

Live: `WS /events`, 2s reconnect backoff. Tagged enum:
```rust
#[serde(tag = "type")]
pub enum WsMessage {
    Event(LiveEvent),
    ShipUpdate(ShipState),
    StationUpdate(StationState),
    OversightUpdate(OversightWindow),
    DeadLetter(DeadLetterEntry),
    InfraStatus(InfraStatusMsg),
}
```

In-memory signals = source of truth for rendering; the WS patches them. Signals only mutate from real WS messages or initial hydration.

---

## Build

`Cargo.toml` and `Trunk.toml` are authoritative. `trunk build --release` must pass with zero errors.

---

## Acceptance (before merge)

- `trunk build --release` clean.
- Visual matches `reference/mockup.html` (colours/fonts/proportions/casing).
- Fonts: Inter + JetBrains Mono only.
- Ship moves only on user command, only when real WS event arrives.
- Ship popup shows correct version/snapshot data.
- Every UI state change driven by real pipeline events. Zero local simulation.
- Gateway unreachable → connection error visible (no faked events).
- Oversight strip shows live requirement state during voyages.
- Correlation highlighting in event log via real correlation IDs.
- All 5 scenarios complete, animated as specced.
- Theme toggle works; starfield fades in light.
- WS `/events` patches signals; initial 4 endpoints fetched on mount.
- No hardcoded colours; no `unwrap`/`expect` outside tests.
- `make k8s-up` deploys clean; `make k8s-test-e2e` Playwright passes.

---

## Frontend debugging

If the API is correct (`curl /game/$SID`) but the UI is broken:

- Buttons disabled/enabled correctly? Check `has_ship`, `is_pending`, `is_disconnected`.
- `apply_snapshot` returning early because `ready=false`?
- `depart_ship`/`load_cargo`/`deliver_cargo` silently returning?
- Browser console errors?
