# canon-deadletter-yugabyte

Part of the [Canon](https://github.com/rjh-mopjones/canon) event sourcing framework.

## Overview

YugabyteDB-backed implementation of the `DeadLetterStore` trait from `canon-deadletter`. Stores messages that have exhausted their retry budget in the `dead_letters` table and provides admin operations for inspection, requeue, and discard.

## Operations

| Method    | SQL                                                        | Behaviour                                      |
|-----------|------------------------------------------------------------|-------------------------------------------------|
| `store`   | `INSERT INTO dead_letters (...) ON CONFLICT (id) DO NOTHING` | Persists a dead letter with attempts = 1        |
| `list`    | `SELECT ... FROM dead_letters [WHERE handler_id = $1]`     | Returns all (or filtered) dead letters          |
| `requeue` | `DELETE FROM dead_letters WHERE id = $1`                   | Removes the row; caller re-enters into inbox    |
| `discard` | `DELETE FROM dead_letters WHERE id = $1`                   | Permanently removes the row                     |

## Error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum YugabyteDeadLetterStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("environment error: {0}")]
    Env(#[from] std::env::VarError),
    #[error("dead letter not found: {id}")]
    NotFound { id: Uuid },
}
```

## Construction

```rust
// From an existing sqlx::PgPool
let store = YugabyteDeadLetterStore::new(pool);

// From the YUGABYTE_URL environment variable
let store = YugabyteDeadLetterStore::from_env().await?;
```

## Dependencies

```toml
[dependencies]
canon-core = { path = "../canon-core" }
canon-deadletter = { path = "../canon-deadletter" }
async-trait = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
bytes = { workspace = true }
sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "uuid", "chrono"] }
tracing = { workspace = true }
```
