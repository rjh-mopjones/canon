# station-service

Station aggregate implementation for the canon-demo. Owns the primary read-ready
projection -- the station inventory materialised view.

## Aggregate: Station

The `Station` aggregate tracks physical stations with docking and cargo capabilities.

**State:**
- `name` -- station name
- `capacity_kg` -- maximum cargo capacity in kilograms
- `current_stock_kg` -- current cargo stock level in kilograms
- `docked_ships` -- list of ship UUIDs currently docked
- `registered` -- whether the station has been registered

**Snapshot cadence:** every 50 events.

## Commands

| Command | Produces | Description |
|---|---|---|
| `RegisterStation` | `StationRegistered` | Registers a new station with name and capacity |
| `RecordDocking` | `ShipDocked` | Records a ship docking at the station |
| `RecordCargoReceived` | `CargoReceived` | Records cargo received at the station |
| `UpdateCapacity` | `CapacityUpdated` | Updates the station's cargo capacity |
| `CheckStockLevel` | `StationStockLow` | Internal command: checks if stock exceeds 80% threshold |

## Events

| Event | State change |
|---|---|
| `StationRegistered` | Sets name, capacity, marks registered |
| `ShipDocked` | Adds ship to docked list |
| `CargoReceived` | Increases current stock |
| `StationStockLow` | Notification only (no state change) |
| `CapacityUpdated` | Updates capacity |

## 80% Stock Threshold

When cargo is received, a `StockLevelMonitorHandler` event handler produces a
`CheckStockLevel` command. The `CheckStockLevelHandler` command handler has access
to aggregate state and emits `StationStockLow` if `current_stock_kg > capacity_kg * 0.8`.
If the stock is within normal range, the command is rejected (no event emitted).

## Station Inventory Projection

The showcase read-ready projection. Maintains a materialised view:

```sql
CREATE TABLE station_inventory (
    station_id       UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    capacity_kg      INT NOT NULL,
    current_stock_kg INT NOT NULL DEFAULT 0,
    last_docking     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Projection handlers update the view idempotently for `StationRegistered`,
`ShipDocked`, `CargoReceived`, and `CapacityUpdated` events.

## Cross-Service Event Handlers

| Handler | Consumes | From | Produces |
|---|---|---|---|
| `ShipArrivedHandler` | `ShipArrivedAtStation` | `canon.navigation.events` | `RecordDocking` command |
| `CargoUnloadedHandler` | `CargoUnloaded` | `canon.cargo.events` | `RecordCargoReceived` command |
| `StockLevelMonitorHandler` | `CargoReceived` | Internal | `CheckStockLevel` command |

## Published Events

`StationStockLow` is published to `canon.station.events` and consumed by the
supply-service to trigger resupply flows.

## Error Types

All errors use `thiserror`. Variants:
- `AlreadyRegistered` -- station already registered
- `NotRegistered` -- station not yet registered
- `EmptyName` -- name must not be empty
- `InvalidCapacity` -- capacity must be greater than zero
- `InvalidWeight` -- cargo weight must be greater than zero
- `ShipAlreadyDocked` -- ship is already docked
- `StockLevelNormal` -- stock within normal range (command rejection, no event)
- `Serialization` -- serialization error
