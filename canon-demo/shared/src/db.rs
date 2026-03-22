//! Per-service database pool creation with schema isolation.
//!
//! Each demo service uses its own YugabyteDB schema (e.g. `canon_fleet`,
//! `canon_cargo`) so that outbox, commands, inbox, and other tables are
//! fully isolated between services. This module provides a helper that
//! creates a `PgPool` whose connections automatically set `search_path`
//! to the requested schema, so all existing SQL queries work unchanged.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Create a connection pool that sets `search_path` to `{schema}, public`
/// on every newly acquired connection.
///
/// This ensures all SQL queries issued through this pool read and write
/// tables in the service-specific schema. The `public` schema is included
/// as a fallback for shared extensions like `pgcrypto`.
///
/// # Example
///
/// ```rust,ignore
/// let pool = create_service_pool(
///     "postgres://canon:canon@yugabytedb:5433/canon",
///     "canon_fleet",
/// ).await?;
/// ```
pub async fn create_service_pool(database_url: &str, schema: &str) -> Result<PgPool, sqlx::Error> {
    // Validate schema name to prevent SQL injection — only allow alphanumeric
    // and underscore characters (matching PostgreSQL unquoted identifier rules).
    if schema.is_empty()
        || !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(sqlx::Error::Configuration(
            format!(
                "invalid schema name: {schema:?} — must be non-empty and contain only [a-zA-Z0-9_]"
            )
            .into(),
        ));
    }

    let schema = schema.to_owned();
    PgPoolOptions::new()
        .after_connect(move |conn, _meta| {
            let schema = schema.clone();
            Box::pin(async move {
                // SET search_path applies to the session, so every query on
                // this connection will resolve unqualified table names to the
                // service-specific schema first, then public.
                sqlx::Executor::execute(
                    &mut *conn,
                    sqlx::query(&format!("SET search_path TO {schema}, public")),
                )
                .await?;
                tracing::debug!(schema = %schema, "connection search_path configured");
                Ok(())
            })
        })
        .connect(database_url)
        .await
}
