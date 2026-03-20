# station-service

Station aggregate implementation for the canon-demo. Owns the primary read-ready
projection -- the station inventory materialised view.

## Aggregate: Station

The `Station` aggregate tracks physical stations with docking and cargo capabilities.

**State:**
- `name` -- station name
- `capacity_kg` -- maximum cargo capacity in kilograms
- `current_stock_kg` -- current cargo stock level in kilograms
- `drain_rate_kg_per_s` -- configurable stock drain rate per second
- `supplied_by` -- UUID of the station that supplies this one (supply chain ring)
- `docked_ships` -- list of ship UUIDs currently docked
- `registered` -- whether the station has been registered
- `offline` -- whether the station is offline (0% stock game-over condition)

**Snapshot cadence:** every 50 events.

## Stations

Four stations are created on startup:

| Station | Capacity | Drain Rate | Supplied By |
|---|---|---|---|
| Alpha Depot (18%, 26%) | 5000 kg | 2.0 kg/s | Delta Prime |
| Beta Relay (68%, 14%) | 3000 kg | 1.5 kg/s | Alpha Depot |
| Gamma Outpost (76%, 68%) | 2000 kg | 3.0 kg/s | Beta Relay |
| Delta Prime (24%, 74%) | 4000 kg | 1.0 kg/s | Gamma Outpost |

Supply chain ring: Alpha←Delta, Beta←Alpha, Gamma←Beta, Delta←Gamma.

## Commands

| Command | Produces | Description |
|---|---|---|
| `RegisterStation` | `StationRegistered` | Registers a new station with name and capacity |
| `RecordDocking` | `ShipDocked` | Records a ship docking at the station |
| `RecordCargoReceived` | `CargoReceived` | Records cargo received at the station |
| `UpdateCapacity` | `CapacityUpdated` | Updates the station's cargo capacity |
| `CheckStockLevel` | `StationStockLow` | Internal: checks if stock drops below 20% threshold |
| `CheckStationOffline` | `StationOffline` | Internal: checks if stock has reached 0% (game-over) |

## Events

| Event | State change |
|---|---|
| `StationRegistered` | Sets name, capacity, marks registered |
| `ShipDocked` | Adds ship to docked list |
| `CargoReceived` | Increases current stock |
| `StationStockLow` | Notification only (no state change) |
| `CapacityUpdated` | Updates capacity |
| `StationOffline` | Marks station offline, zeroes stock |

## 20% Low-Stock Threshold

When cargo is received, a `StockLevelMonitorHandler` event handler produces a
`CheckStockLevel` command. The `CheckStockLevelHandler` command handler has access
to aggregate state and emits `StationStockLow` if `current_stock_kg < capacity_kg * 0.2`.
If the stock is within normal range, the command is rejected (no event emitted).

`StationStockLow` is published to `canon.station.events` and consumed by the
supply-service to trigger the resupply chain.

## Station Offline (Game-Over)

When stock reaches 0%, `CheckStationOffline` produces `StationOffline`. An offline
station rejects `RecordDocking` and `RecordCargoReceived` commands.

## Station Inventory Projection

The showcase read-ready projection. Maintains a materialised view:

```sql
CREATE TABLE station_inventory (
    station_id       UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    capacity_kg      INT NOT NULL,
    current_stock_kg INT NOT NULL DEFAULT 0,
    last_docking     TIMESTAMPTZ,
    offline          BOOLEAN NOT NULL DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Projection handlers update the view idempotently for `StationRegistered`,
`ShipDocked`, `CargoReceived`, `CapacityUpdated`, and `StationOffline` events.

## Cross-Service Event Handlers

| Handler | Consumes | From | Produces |
|---|---|---|---|
| `ShipArrivedHandler` | `ShipArrivedAtStation` | `canon.navigation.events` | `RecordDocking` command |
| `CargoUnloadedHandler` | `CargoUnloaded` | `canon.cargo.events` | `RecordCargoReceived` command |
| `StockLevelMonitorHandler` | `CargoReceived` | Internal | `CheckStockLevel` command |

## Error Types

All errors use `thiserror`. Variants:
- `AlreadyRegistered` -- station already registered
- `NotRegistered` -- station not yet registered
- `EmptyName` -- name must not be empty
- `InvalidCapacity` -- capacity must be greater than zero
- `InvalidWeight` -- cargo weight must be greater than zero
- `ShipAlreadyDocked` -- ship is already docked
- `StockLevelNormal` -- stock within normal range (command rejection, no event)
- `AlreadyOffline` -- station is already offline
- `StationOffline` -- station is offline, cannot accept commands
- `Serialization` -- serialization error
