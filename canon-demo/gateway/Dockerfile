FROM rustlang/rust:nightly AS builder
RUN apt-get update && apt-get install -y cmake build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release -p gateway

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/gateway /usr/local/bin/gateway
CMD ["gateway"]
