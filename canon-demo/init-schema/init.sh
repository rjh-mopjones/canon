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
    -f /schema/yugabyte.sql
echo "==> YugabyteDB schema ready."

echo "==> Initialising Cassandra schema..."
cqlsh "${CASSANDRA_HOST:-cassandra}" "${CASSANDRA_PORT:-9042}" \
    -f /schema/cassandra.cql
echo "==> Cassandra schema ready."

echo "==> All schemas initialised."
