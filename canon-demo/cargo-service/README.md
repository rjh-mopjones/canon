# cargo-service

Canon demo service implementing the **Manifest** aggregate for cargo tracking.

## Aggregate: ManifestState

Tracks cargo manifests through their lifecycle: Open -> Unloading -> Closed.

### Commands

| Command | Version | Produces |
|---|---|---|
| `CreateManifest` | 1 | `ManifestCreated` |
| `LoadCargo` | 1 | `CargoLoaded` |
| `BeginUnloading` | 1 | `UnloadingStarted` |
| `RecordUnloaded` | 1 | `CargoUnloaded` |
| `CloseManifest` | 1 | `ManifestClosed` |

### Events

| Event | Version | Notes |
|---|---|---|
| `ManifestCreated` | 1 | |
| `CargoLoaded` | 2 | v1 upcast adds `description` field |
| `UnloadingStarted` | 1 | |
| `CargoUnloaded` | 1 | |
| `ManifestClosed` | 1 | |

## Schema upcasting: CargoLoaded v1 -> v2

The `CargoLoaded` event gained a `description` field in v2. The `upcast` module handles migration: v1 payloads are deserialized into the old schema and converted to v2 with `description` set to `"(migrated from v1)"`.

## UnloadingHandler (event handler with oversight)

Demonstrates the most sophisticated oversight pattern in the demo:

- **window_ttl**: 30 minutes
- **Ready**: when both `ShipArrivedAtStation` (external) AND `ManifestCreated` (internal) are present
- **NotReady**: when either event is missing
- **Discard**: when `ShipDecommissioned` arrives (takes priority over Ready)

### Cross-service flows

- **Consumes**: `canon.navigation.events` -- `ShipArrivedAtStation` submitted as ExternalEvent
- **Publishes**: `CargoUnloaded` -> `canon.cargo.events` -> consumed by station-service

## Projection: ManifestReadModel

Read model tracking manifest status and total cargo weight:

```sql
CREATE TABLE manifest_read_models (
    manifest_id     UUID PRIMARY KEY,
    ship_id         UUID NOT NULL,
    status          TEXT NOT NULL,
    total_weight_kg INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Modules

| Module | Description |
|---|---|
| `aggregate` | `ManifestState` aggregate, event combiners, hydration with upcast |
| `commands` | Command handler impls for all five commands |
| `error` | `CargoError`, `UnloadingError`, `ProjectionError`, `UpcastError` |
| `events` | Re-exports from `canon-demo-shared` |
| `handlers` | `UnloadingHandler` with oversight logic |
| `projection` | `ManifestReadModel` and projection handlers |
| `upcast` | `CargoLoaded` v1 -> v2 schema migration |
