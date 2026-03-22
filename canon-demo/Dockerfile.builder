FROM rust:latest AS builder
WORKDIR /usr/src/canon

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release \
    -p gateway \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p station-service \
    -p supply-service
