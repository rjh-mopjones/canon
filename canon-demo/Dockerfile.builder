# Stage 1: Plan — capture dependency graph
FROM rust:latest AS planner
WORKDIR /usr/src/canon
RUN cargo install cargo-chef

# Copy only manifests and lock file (faster context hashing than COPY . .)
COPY Cargo.toml Cargo.lock ./
COPY canon-core/Cargo.toml canon-core/Cargo.toml
COPY canon-core/canon-core-macros/Cargo.toml canon-core/canon-core-macros/Cargo.toml
COPY canon-adaptor/Cargo.toml canon-adaptor/Cargo.toml
COPY canon-adaptor-kafka/Cargo.toml canon-adaptor-kafka/Cargo.toml
COPY canon-command-store/Cargo.toml canon-command-store/Cargo.toml
COPY canon-command-store-yugabyte/Cargo.toml canon-command-store-yugabyte/Cargo.toml
COPY canon-deadletter/Cargo.toml canon-deadletter/Cargo.toml
COPY canon-deadletter-yugabyte/Cargo.toml canon-deadletter-yugabyte/Cargo.toml
COPY canon-event-store/Cargo.toml canon-event-store/Cargo.toml
COPY canon-event-store-cassandra/Cargo.toml canon-event-store-cassandra/Cargo.toml
COPY canon-inbound-queue/Cargo.toml canon-inbound-queue/Cargo.toml
COPY canon-inbound-queue-kafka/Cargo.toml canon-inbound-queue-kafka/Cargo.toml
COPY canon-inbox/Cargo.toml canon-inbox/Cargo.toml
COPY canon-inbox-yugabyte/Cargo.toml canon-inbox-yugabyte/Cargo.toml
COPY canon-outbound-queue/Cargo.toml canon-outbound-queue/Cargo.toml
COPY canon-outbound-queue-kafka/Cargo.toml canon-outbound-queue-kafka/Cargo.toml
COPY canon-projection-store/Cargo.toml canon-projection-store/Cargo.toml
COPY canon-projection-store-yugabyte/Cargo.toml canon-projection-store-yugabyte/Cargo.toml
COPY canon-publisher/Cargo.toml canon-publisher/Cargo.toml
COPY canon-publisher-kafka/Cargo.toml canon-publisher-kafka/Cargo.toml
COPY canon-snapshot-store/Cargo.toml canon-snapshot-store/Cargo.toml
COPY canon-snapshot-store-yugabyte/Cargo.toml canon-snapshot-store-yugabyte/Cargo.toml
COPY canon-test/Cargo.toml canon-test/Cargo.toml
COPY canon-demo/shared/Cargo.toml canon-demo/shared/Cargo.toml
COPY canon-demo/fleet-service/Cargo.toml canon-demo/fleet-service/Cargo.toml
COPY canon-demo/cargo-service/Cargo.toml canon-demo/cargo-service/Cargo.toml
COPY canon-demo/navigation-service/Cargo.toml canon-demo/navigation-service/Cargo.toml
COPY canon-demo/station-service/Cargo.toml canon-demo/station-service/Cargo.toml
COPY canon-demo/supply-service/Cargo.toml canon-demo/supply-service/Cargo.toml
COPY canon-demo/gateway/Cargo.toml canon-demo/gateway/Cargo.toml
COPY canon-demo/frontend/Cargo.toml canon-demo/frontend/Cargo.toml

# Create dummy source files so cargo-chef can parse the workspace
RUN find . -name "Cargo.toml" -not -path "./Cargo.toml" -exec sh -c ' \
    dir=$(dirname "$1"); \
    if grep -q "proc-macro" "$1" 2>/dev/null; then \
        mkdir -p "$dir/src" && touch "$dir/src/lib.rs"; \
    elif grep -q "\\[\\[bin\\]\\]" "$1" 2>/dev/null || grep -q "src/main.rs" "$1" 2>/dev/null; then \
        mkdir -p "$dir/src" && touch "$dir/src/main.rs" && touch "$dir/src/lib.rs"; \
    else \
        mkdir -p "$dir/src" && touch "$dir/src/lib.rs"; \
    fi' _ {} \;

RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Cook — build dependencies (cached unless Cargo.toml/Cargo.lock change)
# No C toolchain needed — rskafka is pure Rust (no cmake, libssl, libsasl2)
FROM rust:latest AS cacher
WORKDIR /usr/src/canon
RUN cargo install cargo-chef
COPY --from=planner /usr/src/canon/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json \
    -p gateway \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p station-service \
    -p supply-service

# Stage 3: Build — compile application code only
# No C toolchain needed — all Kafka crates use rskafka (pure Rust)
FROM rust:latest AS builder
WORKDIR /usr/src/canon
COPY --from=cacher /usr/local/cargo/registry /usr/local/cargo/registry
COPY --from=cacher /usr/src/canon/target target
COPY . .
RUN cargo build --release \
    -p gateway \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p station-service \
    -p supply-service
