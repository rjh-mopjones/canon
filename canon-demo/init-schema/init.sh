#!/usr/bin/env bash
# Schema initialisation script for Canon demo infrastructure.
# Runs YugabyteDB and Cassandra DDL before services start.
set -euo pipefail

echo "==> Initialising YugabyteDB schema..."
PGPASSWORD="${YSQL_PASSWORD:-canon}" psql \
    -h "${YUGABYTE_HOST:-yugabytedb}" \
    -p "${YUGABYTE_PORT:-5433}" \
    -U "${YSQL_USER:-canon}" \
    -d "${YSQL_DB:-canon}" \
    --set ON_ERROR_STOP=1 \
    -f /schema/yugabyte.sql
echo "==> YugabyteDB schema ready."

echo "==> Initialising Cassandra schema..."
cqlsh "${CASSANDRA_HOST:-cassandra}" "${CASSANDRA_PORT:-9042}" \
    -f /schema/cassandra.cql
echo "==> Cassandra schema ready."

echo "==> Creating Kafka topics..."
KAFKA_BOOTSTRAP="${KAFKA_HOST:-kafka}:${KAFKA_PORT:-9092}"
for topic in canon.fleet.events canon.cargo.events canon.navigation.events canon.supply.events canon.station.events; do
    kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1
    echo "    created topic: $topic"
done
echo "==> Kafka topics ready."

echo "==> All schemas initialised."
