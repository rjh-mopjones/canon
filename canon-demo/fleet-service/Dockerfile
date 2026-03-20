FROM rust:1.86 AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p fleet-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/fleet-service /usr/local/bin/fleet-service
CMD ["fleet-service"]
