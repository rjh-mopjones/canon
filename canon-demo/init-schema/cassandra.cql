-- Canon Cassandra schema initialisation
-- Runs as an init container before services start.

CREATE KEYSPACE IF NOT EXISTS canon
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS canon.events (
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
