// Canon Cassandra schema initialisation — per-service keyspaces
// Runs as an init container before services start.
//
// Each demo service gets its own keyspace (canon_fleet, canon_cargo, etc.)
// to ensure complete domain isolation for event stores.
//
// Creates both prod and staging keyspaces (10 total). Staging keyspaces use
// the canon_staging_ prefix and share the same Cassandra cluster.

// ── Fleet service keyspace (prod) ───────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_fleet
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_fleet.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Cargo service keyspace (prod) ───────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_cargo
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_cargo.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Navigation service keyspace (prod) ──────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_navigation
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_navigation.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Supply service keyspace (prod) ──────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_supply
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_supply.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Station service keyspace (prod) ─────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_station
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_station.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ════════════════════════════════════════════════════════════════════════════
// Staging keyspaces — same tables, canon_staging_ prefix
// ════════════════════════════════════════════════════════════════════════════

// ── Fleet service keyspace (staging) ────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_staging_fleet
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_staging_fleet.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Cargo service keyspace (staging) ────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_staging_cargo
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_staging_cargo.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Navigation service keyspace (staging) ───────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_staging_navigation
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_staging_navigation.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Supply service keyspace (staging) ───────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_staging_supply
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_staging_supply.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

// ── Station service keyspace (staging) ──────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS canon_staging_station
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon_staging_station.events (
    aggregate_id UUID,
    version BIGINT,
    event_id UUID,
    event_type TEXT,
    event_version INT,
    payload BLOB,
    correlation_id UUID,
    causation_id UUID,
    created_at TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);
