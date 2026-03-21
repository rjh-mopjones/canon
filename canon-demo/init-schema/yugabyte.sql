-- Canon YugabyteDB schema initialisation
-- Runs as an init container before services start.

-- inbox
CREATE TABLE IF NOT EXISTS inbox_messages (
    handler_id TEXT NOT NULL,
    message_id UUID NOT NULL,
    aggregate_id UUID,
    message_type TEXT,
    payload BYTEA,
    received_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (handler_id, message_id)
);

CREATE TABLE IF NOT EXISTS inbox_windows (
    handler_id TEXT NOT NULL,
    correlation_key UUID NOT NULL,
    window_id UUID DEFAULT gen_random_uuid(),
    messages JSONB DEFAULT '[]',
    status TEXT DEFAULT 'pending',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (handler_id, correlation_key)
);

CREATE TABLE IF NOT EXISTS processed_windows (
    window_id UUID PRIMARY KEY,
    handler_id TEXT,
    processed_at TIMESTAMPTZ DEFAULT now()
);

-- command store
CREATE TABLE IF NOT EXISTS commands (
    command_id UUID PRIMARY KEY,
    aggregate_id UUID,
    command_type TEXT NOT NULL DEFAULT '',
    command_version INT DEFAULT 1,
    payload BYTEA,
    correlation_id UUID,
    causation_id UUID,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS commands_aggregate_idx ON commands (aggregate_id, created_at);

-- outbox
DO $$
BEGIN
    CREATE SEQUENCE outbox_seq;
EXCEPTION WHEN duplicate_table THEN
    -- sequence already exists, skip
END $$;

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_number BIGINT DEFAULT nextval('outbox_seq'),
    aggregate_id UUID,
    payload BYTEA,
    created_at TIMESTAMPTZ DEFAULT now(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_seq_idx ON outbox (sequence_number) WHERE delivered_at IS NULL;

-- snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID NOT NULL,
    version BIGINT NOT NULL,
    state BYTEA NOT NULL,
    taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (aggregate_id, version)
);

-- projection checkpoints
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_id TEXT PRIMARY KEY,
    last_version BIGINT DEFAULT 0,
    rebuilding BOOLEAN DEFAULT false,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- projections (materialised read models)
CREATE TABLE IF NOT EXISTS projections (
    projection_id TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_id, aggregate_id)
);

-- station inventory projection (read-ready materialised view)
CREATE TABLE IF NOT EXISTS station_inventory (
    station_id       UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    capacity_kg      REAL NOT NULL DEFAULT 0,
    current_stock_kg REAL NOT NULL DEFAULT 0,
    last_docking     TIMESTAMPTZ,
    offline          BOOLEAN NOT NULL DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- dead letters
CREATE TABLE IF NOT EXISTS dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id UUID,
    handler_id TEXT,
    aggregate_id UUID,
    payload BYTEA,
    error TEXT,
    attempts INT DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_attempted TIMESTAMPTZ DEFAULT now()
);

-- retry attempts
CREATE TABLE IF NOT EXISTS retry_attempts (
    message_id UUID PRIMARY KEY,
    handler_id TEXT,
    attempts INT DEFAULT 0,
    last_attempted TIMESTAMPTZ DEFAULT now()
);
