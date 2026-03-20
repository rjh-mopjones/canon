# gateway

Axum REST and WebSocket gateway for the Canon demo. The gateway owns no domain logic; it routes REST commands to service inboxes, streams Kafka events to WebSocket clients, and proxies read-model queries.

## Endpoints

### Command routes (POST)

| Route | Command | Target service |
|---|---|---|
| `/fleet/ships` | RegisterShip | fleet |
| `/fleet/ships/:id/route` | AssignRoute | fleet |
| `/fleet/ships/:id/depart` | DepartForStation | fleet |
| `/fleet/ships/:id/resupply` | ScheduleResupply | fleet |
| `/fleet/ships/:id/decommission` | DecommissionShip | fleet |
| `/cargo/manifests` | CreateManifest | cargo |
| `/cargo/manifests/:id/load` | LoadCargo | cargo |
| `/navigation/routes` | PlanRoute | navigation |
| `/supply/resupply` | RequestResupply | supply |
| `/stations/:id/register` | RegisterStation | station |

### Read routes (GET)

| Route | Description |
|---|---|
| `/ships` | List all ships with hydrated state |
| `/ships/:id/history` | Event history from Cassandra |
| `/stations` | List all stations from projection |
| `/stations/:id/inventory` | Station inventory projection |
| `/cargo/manifests/:id` | Manifest event history |
| `/replay/counterfactual` | Simplified counterfactual diff |

### Admin routes

| Route | Method | Description |
|---|---|---|
| `/admin/oversight/windows` | GET | Pending inbox windows |
| `/admin/deadletters` | GET | List dead letters |
| `/admin/deadletters/:id/requeue` | POST | Requeue a dead letter (204) |
| `/admin/deadletters/:id` | DELETE | Discard a dead letter (204) |

### WebSocket

| Route | Description |
|---|---|
| `/events` | Broadcasts `WsEnvelope` tagged JSON (events, ship updates, infra status) |

## Modules

- `main.rs` -- tokio entry point, infrastructure connections, Kafka consumer spawn
- `routes/` -- axum route handlers (fleet, cargo, navigation, supply, station, admin, replay, ws)
- `command.rs` -- `CommandEnvelope` builder and transactional submission to commands + inbox
- `correlation.rs` -- `X-Correlation-Id` header extraction
- `error.rs` -- `GatewayError` using `thiserror`, implements `IntoResponse`
- `kafka.rs` -- per-topic Kafka consumers broadcasting to WebSocket, infra status broadcaster
- `state.rs` -- `AppState` shared across handlers
- `types.rs` -- request/response types, `WsEnvelope` protocol enum

## Infrastructure dependencies

- **YugabyteDB** -- commands table, inbox, projections, dead letters, oversight windows
- **Cassandra** -- event history (read-through via `CassandraEventStore`)
- **Kafka** -- one consumer per service topic, manual offset commit
