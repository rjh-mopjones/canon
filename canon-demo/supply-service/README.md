# supply-service

Supply-service manages fuel inventory and resupply logistics for the Canon demo. It is the showcase for **dead letter handling** in the framework.

## Aggregate: Inventory

Tracks fuel stock levels at a station and manages the resupply dispatch lifecycle.

**State:**

| Field              | Type             | Description                              |
|--------------------|------------------|------------------------------------------|
| `station_id`       | `Option<Uuid>`   | Station this inventory belongs to        |
| `fuel_kg`          | `u32`            | Current fuel stock in kilograms          |
| `pending_resupply` | `Option<Uuid>`   | Ship ID of an in-flight resupply, if any |

Snapshots every 50 events.

## Commands

| Command            | Produces            | Validation                                  |
|--------------------|---------------------|---------------------------------------------|
| `RecordStock`      | `StockRecorded`     | Always succeeds                             |
| `RequestResupply`  | `ResupplyRequested` | Always succeeds                             |
| `DispatchResupply` | `ResupplyDispatched`| Fails if `pending_resupply` is already set  |
| `ConfirmDelivery`  | `DeliveryConfirmed` | Fails if no `pending_resupply` is set       |

## Events

| Event                | State effect                        |
|----------------------|-------------------------------------|
| `StockRecorded`      | Sets `station_id` and `fuel_kg`     |
| `ResupplyRequested`  | No state change (request recorded)  |
| `ResupplyDispatched` | Sets `pending_resupply` to ship ID  |
| `DeliveryConfirmed`  | Clears `pending_resupply`           |

## Event handler: StockAlertHandler

Consumes `StationStockLow` events from `canon.station.events` and produces `RequestResupply` commands targeting the local Inventory aggregate.

## Projection: InventoryReadModel

Read model mirroring the `inventory_read_models` table:

- `inventory_id`, `station_id`, `fuel_kg`, `resupply_pending`, `updated_at`

Projection handlers: `StockRecordedProjectionHandler`, `ResupplyDispatchedProjectionHandler`, `DeliveryConfirmedProjectionHandler`.

## Dead letter showcase

`DispatchResupply` can fail when a resupply is already pending. This demonstrates the dead letter flow:

1. Event store consumer encounters processing failure
2. Retry count persisted in `retry_attempts` table (YugabyteDB)
3. After max retries (default 3), written to dead letter store
4. `retry_attempts` row cleaned up after dead lettering
5. Dead letter requeue is manual only (via gateway admin API)
6. Requeue re-inserts messages into `inbox_windows` with fresh `expires_at`

## Cross-service flows

- **Consumes:** `canon.station.events` -- `StationStockLow` via `StockAlertHandler`
- **Publishes:** `ResupplyDispatched` to `canon.supply.events` -- consumed by fleet-service
