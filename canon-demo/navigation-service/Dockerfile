# Stage 1: Build
FROM rust:1.83-bookworm AS builder
WORKDIR /usr/src/canon

# Install system dependencies for rdkafka (cmake-build) and OpenSSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy entire workspace — needed for inter-crate dependencies
COPY Cargo.toml Cargo.lock ./
COPY canon-core/ canon-core/
COPY canon-event-store/ canon-event-store/
COPY canon-event-store-cassandra/ canon-event-store-cassandra/
COPY canon-command-store/ canon-command-store/
COPY canon-command-store-yugabyte/ canon-command-store-yugabyte/
COPY canon-snapshot-store/ canon-snapshot-store/
COPY canon-snapshot-store-yugabyte/ canon-snapshot-store-yugabyte/
COPY canon-inbox/ canon-inbox/
COPY canon-inbox-yugabyte/ canon-inbox-yugabyte/
COPY canon-inbound-queue/ canon-inbound-queue/
COPY canon-inbound-queue-kafka/ canon-inbound-queue-kafka/
COPY canon-outbound-queue/ canon-outbound-queue/
COPY canon-outbound-queue-kafka/ canon-outbound-queue-kafka/
COPY canon-projection-store/ canon-projection-store/
COPY canon-projection-store-yugabyte/ canon-projection-store-yugabyte/
COPY canon-publisher/ canon-publisher/
COPY canon-publisher-kafka/ canon-publisher-kafka/
COPY canon-adaptor/ canon-adaptor/
COPY canon-adaptor-kafka/ canon-adaptor-kafka/
COPY canon-deadletter/ canon-deadletter/
COPY canon-deadletter-yugabyte/ canon-deadletter-yugabyte/
COPY canon-test/ canon-test/
COPY canon-demo/ canon-demo/

RUN cargo build --release -p navigation-service

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/canon/target/release/navigation-service /usr/local/bin/navigation-service
CMD ["navigation-service"]
