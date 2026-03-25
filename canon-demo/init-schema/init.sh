#!/usr/bin/env bash
# Schema initialisation script for Canon demo infrastructure.
# Runs YugabyteDB and Cassandra DDL before services start.
#
# Supports both minikube (local) and GKE (production) via env var overrides:
#   YUGABYTE_HOST  — default: yb-tservers.canon-infra.svc.cluster.local
#   YUGABYTE_PORT  — default: 5433
#   YSQL_USER      — default: yugabyte
#   YSQL_PASSWORD  — default: yugabyte
#   YSQL_DB        — default: yugabyte
#   CASSANDRA_HOST — default: cassandra.canon-infra.svc.cluster.local
#   CASSANDRA_PORT — default: 9042
#   KAFKA_TOPICS_VIA_STRIMZI — default: true (applies KafkaTopic CRDs via kubectl)
set -euo pipefail

echo "==> Initialising YugabyteDB schema (10 schemas: 5 prod + 5 staging)..."
PGPASSWORD="${YSQL_PASSWORD:-yugabyte}" psql \
    -h "${YUGABYTE_HOST:-yb-tservers.canon-infra.svc.cluster.local}" \
    -p "${YUGABYTE_PORT:-5433}" \
    -U "${YSQL_USER:-yugabyte}" \
    -d "${YSQL_DB:-yugabyte}" \
    --set ON_ERROR_STOP=1 \
    -f /schema/yugabyte.sql
echo "==> YugabyteDB schema ready."

echo "==> Initialising Cassandra schema (10 keyspaces: 5 prod + 5 staging)..."
cqlsh "${CASSANDRA_HOST:-cassandra.canon-infra.svc.cluster.local}" \
    "${CASSANDRA_PORT:-9042}" \
    -f /schema/cassandra.cql
echo "==> Cassandra schema ready."

# Kafka topic creation via Strimzi KafkaTopic CRDs.
# When running on GKE with Strimzi, kubectl apply the CRDs.
# On minikube, the init-kafka-topics Job handles topic creation via kafka-topics.sh.
if [ "${KAFKA_TOPICS_VIA_STRIMZI:-true}" = "true" ] && [ -f /schema/kafka-topics.yaml ]; then
    echo "==> Creating Kafka topics via Strimzi KafkaTopic CRDs (30 topics: 15 prod + 15 staging)..."
    kubectl apply -f /schema/kafka-topics.yaml
    echo "==> Kafka topics submitted to Strimzi."
else
    echo "==> Skipping Kafka topic creation (KAFKA_TOPICS_VIA_STRIMZI=${KAFKA_TOPICS_VIA_STRIMZI:-true}, handled by init-kafka-topics Job)."
fi

echo "==> All schemas initialised."
