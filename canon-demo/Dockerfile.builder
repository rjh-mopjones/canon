FROM rust:latest AS builder
WORKDIR /usr/src/canon

# Install system dependencies for rdkafka (cmake-build) and OpenSSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy entire workspace
COPY . .

# Build all 6 demo services in one pass
RUN cargo build --release \
    -p gateway \
    -p fleet-service \
    -p cargo-service \
    -p navigation-service \
    -p station-service \
    -p supply-service
