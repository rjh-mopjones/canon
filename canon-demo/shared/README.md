# canon-demo-shared

Shared domain types, events, commands, and Kafka topic constants for the Canon demo services. This crate is the single source of truth for the demo domain vocabulary. It contains zero logic -- types and constants only.

## Modules

| Module | Contents |
|--------|----------|
| `events` | 25 event structs across 5 domains with `#[event]` macros, plus domain enum wrappers (`FleetEvent`, `CargoEvent`, etc.) and top-level `DemoEvent` |
| `commands` | 20 command structs across 5 domains with `#[command]` macros |
| `topics` | Kafka topic constants (`canon.fleet.events`, `canon.cargo.events`, etc.) |

## Domains

| Service | Aggregate | Commands | Events |
|---------|-----------|----------|--------|
| fleet | Ship | RegisterShip, AssignRoute, DepartForStation, ScheduleResupply, DecommissionShip | ShipRegistered, RouteAssigned, ShipDeparted, ResupplyScheduled, ShipDecommissioned |
| cargo | Manifest | CreateManifest, LoadCargo, BeginUnloading, RecordUnloaded, CloseManifest | ManifestCreated, CargoLoaded, UnloadingStarted, CargoUnloaded, ManifestClosed |
| navigation | Route | PlanRoute, RecordDeparture, UpdatePosition, RecordArrival | RoutePlanned, PositionUpdated, ShipArrivedAtStation |
| supply | Inventory | RecordStock, RequestResupply, DispatchResupply, ConfirmDelivery | StockRecorded, ResupplyRequested, ResupplyDispatched, DeliveryConfirmed |
| station | Station | RegisterStation, RecordDocking, RecordCargoReceived, UpdateCapacity | StationRegistered, ShipDocked, CargoReceived, StationStockLow, CapacityUpdated |

## Dependencies

- `canon-core` -- proc-macros (`#[event]`, `#[command]`) and re-exported serde derives
- `uuid` -- identity fields
- `serde` -- serialization for domain enums
