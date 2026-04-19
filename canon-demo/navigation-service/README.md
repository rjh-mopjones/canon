# navigation-service

Navigation service for the Canon demo. Owns the **Route** aggregate and handles route planning, departure recording, position tracking, and arrival recording.

## Aggregate: Route

| Field | Type | Description |
|-------|------|-------------|
| `ship_id` | `Option<Uuid>` | Ship assigned to this route |
| `waypoints` | `Vec<Uuid>` | Ordered list of waypoint IDs |
| `current_waypoint_index` | `usize` | Index into waypoints |
| `arrived` | `bool` | Whether the ship has arrived at its destination |

Snapshot cadence: every 50 events.

## Commands and Events

| Command | Produces | Validation |
|---------|----------|------------|
| `PlanRoute` | `RoutePlanned` | Waypoints must be non-empty |
| `RecordDeparture` | `PositionUpdated` | Route must have a ship assigned; command ship must match route's ship |
| `UpdatePosition` | `PositionUpdated` | Route must not have already arrived |
| `RecordArrival` | `ShipArrivedAtStation` | Route must not have already arrived; must have a ship assigned |

## Event Handler

| Handler | Consumes | From Topic | Produces |
|---------|----------|------------|----------|
| `DepartureHandler` | `ShipDeparted` (fleet, with `voyage_id`) | `canon.fleet.events` | `PlanRoute` command keyed to that voyage |

## Projection

| Projection | Table | Description |
|------------|-------|-------------|
| `RouteReadModel` | `route_read_models` | Denormalized route state for queries |

## Cross-service flows

- **Consumes:** `canon.fleet.events` -- `ShipDeparted` triggers `DepartureHandler`
- **Publishes:** `ShipArrivedAtStation` to `canon.navigation.events` -- consumed by cargo-service and station-service

## Dependencies

- `canon-core` -- framework traits and proc-macros
- `canon-demo-shared` -- shared domain types, events, commands, topic constants
