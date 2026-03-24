-- Canon YugabyteDB schema initialisation — per-service isolation
-- Runs as an init container before services start.
--
-- Each demo service gets its own schema (canon_fleet, canon_cargo, etc.)
-- with identical table definitions. This ensures complete domain isolation:
-- each service's outbox, commands, inbox, and other tables are fully
-- separate, preventing event leaking and aggregate ID collisions.

-- pgcrypto provides gen_random_uuid() used by inbox_windows, outbox, and dead_letters
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── Per-service schema creation ────────────────────────────────────────────
-- Creates all 5 service schemas and populates them with Canon tables.

DO $$
DECLARE
    schema_name TEXT;
    schemas TEXT[] := ARRAY['canon_fleet', 'canon_cargo', 'canon_navigation', 'canon_supply', 'canon_station'];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);

        -- inbox
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.inbox_messages (
                handler_id TEXT NOT NULL,
                message_id UUID NOT NULL,
                aggregate_id UUID,
                message_type TEXT,
                payload BYTEA,
                received_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (handler_id, message_id)
            )', schema_name);

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.inbox_windows (
                handler_id TEXT NOT NULL,
                correlation_key UUID NOT NULL,
                window_id UUID DEFAULT gen_random_uuid(),
                messages JSONB DEFAULT ''[]'',
                status TEXT DEFAULT ''pending'',
                expires_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (handler_id, correlation_key)
            )', schema_name);

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.processed_windows (
                window_id UUID PRIMARY KEY,
                handler_id TEXT,
                processed_at TIMESTAMPTZ DEFAULT now()
            )', schema_name);

        -- command store
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.commands (
                command_id UUID PRIMARY KEY,
                aggregate_id UUID,
                command_type TEXT NOT NULL DEFAULT '''',
                command_version INT DEFAULT 1,
                payload BYTEA,
                correlation_id UUID,
                causation_id UUID,
                status TEXT NOT NULL DEFAULT ''pending'',
                created_at TIMESTAMPTZ DEFAULT now()
            )', schema_name);

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS commands_aggregate_idx ON %I.commands (aggregate_id, created_at)',
            schema_name);

        -- outbox
        EXECUTE format(
            'DO $seq$ BEGIN CREATE SEQUENCE %I.outbox_seq; EXCEPTION WHEN duplicate_table THEN NULL; END $seq$',
            schema_name);

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.outbox (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                sequence_number BIGINT DEFAULT nextval(''%I.outbox_seq''),
                aggregate_id UUID,
                payload BYTEA,
                created_at TIMESTAMPTZ DEFAULT now(),
                delivered_at TIMESTAMPTZ
            )', schema_name, schema_name);

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS outbox_seq_idx ON %I.outbox (sequence_number) WHERE delivered_at IS NULL',
            schema_name);

        -- snapshots
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.snapshots (
                aggregate_id UUID NOT NULL,
                version BIGINT NOT NULL,
                state BYTEA NOT NULL,
                taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (aggregate_id, version)
            )', schema_name);

        -- projection checkpoints
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.projection_checkpoints (
                projection_id TEXT PRIMARY KEY,
                last_version BIGINT DEFAULT 0,
                rebuilding BOOLEAN DEFAULT false,
                updated_at TIMESTAMPTZ DEFAULT now()
            )', schema_name);

        -- projections (materialised read models)
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.projections (
                projection_id TEXT NOT NULL,
                aggregate_id UUID NOT NULL,
                state JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (projection_id, aggregate_id)
            )', schema_name);

        -- dead letters
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.dead_letters (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                message_id UUID,
                handler_id TEXT,
                aggregate_id UUID,
                payload BYTEA,
                error TEXT,
                attempts INT DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT now(),
                last_attempted TIMESTAMPTZ DEFAULT now()
            )', schema_name);

        -- retry attempts
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.retry_attempts (
                message_id UUID PRIMARY KEY,
                handler_id TEXT,
                attempts INT DEFAULT 0,
                last_attempted TIMESTAMPTZ DEFAULT now()
            )', schema_name);

        -- kafka consumer offset tracking (performance optimisation, not correctness)
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.kafka_consumer_offsets (
                consumer_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                partition_id INT NOT NULL DEFAULT 0,
                last_offset BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )', schema_name);

        RAISE NOTICE 'Schema % initialised', schema_name;
    END LOOP;
END $$;

-- ── Service-specific projection tables ─────────────────────────────────────

-- station inventory projection (read-ready materialised view) — station schema
CREATE TABLE IF NOT EXISTS canon_station.station_inventory (
    station_id       UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    capacity_kg      REAL NOT NULL DEFAULT 0,
    current_stock_kg REAL NOT NULL DEFAULT 0,
    last_docking     TIMESTAMPTZ,
    offline          BOOLEAN NOT NULL DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- supply inventory projection — supply schema
CREATE TABLE IF NOT EXISTS canon_supply.supply_inventory (
    inventory_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    station_id   UUID NOT NULL,
    fuel_kg      REAL NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
