# canon-command-store-yugabyte

YugabyteDB-backed implementation of the `CommandStore` trait from `canon-core`.

## Overview

This crate provides `YugabyteCommandStore`, which persists every command submitted to the system as an audit trail in YugabyteDB (PostgreSQL wire-compatible). Commands are written as part of the single YugabyteDB ACID transaction alongside the outbox.

## Trait implementation

- **`append(envelope)`** — Idempotent INSERT via `ON CONFLICT DO NOTHING`. Duplicate `command_id` is silently ignored.
- **`load_range(aggregate_id, from, to)`** — SELECT by `aggregate_id` with optional timestamp bounds, ordered by `created_at ASC`. Used by the counterfactual replay engine.

## Transactional append

The command handler write path requires a **single ACID transaction** covering both the
command INSERT and the outbox INSERT(s). Use `append_in_tx` for this:

```rust
let mut tx = command_store.pool().begin().await?;
command_store.append_in_tx(&mut tx, envelope).await?;
outbox_store.insert_in_tx(&mut tx, outbox_entries).await?;
tx.commit().await?;
```

The standalone `append()` (from the `CommandStore` trait) still works for cases where
transactional grouping is not needed, but the write path **must** use `append_in_tx`.

## Additional methods

- **`append_in_tx(tx, envelope)`** — Append a command within an existing `sqlx::Transaction`. Idempotent via `ON CONFLICT DO NOTHING`.
- **`load(command_id)`** — Load a single command by its UUID.
- **`load_for_aggregate(aggregate_id)`** — Load all commands for an aggregate, ordered by `created_at ASC`.
- **`update_status(command_id, status)`** — Update the status column of a command (e.g., `pending` → `executed`).
- **`pool()`** — Access the underlying `PgPool` to start transactions.

## Configuration

Connection string is read from the `YUGABYTE_URL` environment variable:

```
YUGABYTE_URL=yugabyte://canon:canon@yugabyte:5433/canon
```

## Schema

```sql
CREATE TABLE commands (
    command_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    command_type TEXT NOT NULL DEFAULT '',
    command_version INT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL,
    correlation_id UUID,
    causation_id UUID,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX commands_aggregate_idx ON commands (aggregate_id, created_at);
```

## Dependencies

- `canon-core` — Core types (`AggregateId`, `CommandEnvelope`) and `CommandStore` trait
- `canon-command-store` — Trait crate (re-exports)
- `sqlx` — Async database driver (PostgreSQL wire-compatible) with `runtime-tokio-rustls`
