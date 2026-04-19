# fleet-service

Reference implementation of a Canon service. Demonstrates the full macro surface: aggregate definition, event combiners, command handlers with validation, event handlers for cross-service event consumption, and projections.

## Domain

| Concept | Types |
|---|---|
| Aggregate | `Ship` (snapshot every 50 events) |
| Commands | `RegisterShip`, `AssignRoute`, `DepartForStation` (includes `voyage_id`), `ScheduleResupply`, `DecommissionShip` |
| Events | `ShipRegistered`, `RouteAssigned`, `ShipDeparted` (includes `voyage_id`), `ResupplyScheduled`, `ShipDecommissioned` |
| Event handlers | `ResupplyHandler` (consumes `ResupplyDispatched` from supply-service) |
| Projections | `ShipReadModel` |
| Error type | `FleetError` |

## Cross-service flows

- **Inbound**: `Supply:ResupplyDispatched` -> `ResupplyHandler` -> produces `ScheduleResupply` command
- **Outbound**: `Fleet:ShipDeparted` -> consumed by navigation-service, carrying a per-voyage `voyage_id` so repeat visits to the same station remain distinct

## Modules

- `aggregate.rs` -- `Ship` aggregate with `ShipStatus` enum
- `combiners.rs` -- event combiners for all five fleet events
- `handlers.rs` -- command handlers with validation (e.g., `DepartForStation` rejects if not docked)
- `event_handlers.rs` -- `ResupplyHandler` for cross-service event consumption
- `projection.rs` -- `ShipReadModel` projection with idempotent apply and rebuild
- `error.rs` -- `FleetError` using `thiserror`
- `main.rs` -- tokio entry point wired via `ServiceBuilder` with in-memory infrastructure (production infra crates not yet implemented)
