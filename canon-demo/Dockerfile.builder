# Stage 1: Plan — capture dependency graph
FROM rust:latest AS planner
WORKDIR /usr/src/canon
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Cook — build dependencies (cached unless Cargo.toml/Cargo.lock change)
FROM rust:latest AS cacher
WORKDIR /usr/src/canon
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef
COPY --from=planner /usr/src/canon/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build — compile application code only
FROM rust:latest AS builder
WORKDIR /usr/src/canon
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*
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
