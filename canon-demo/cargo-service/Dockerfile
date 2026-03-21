FROM rustlang/rust:nightly AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p cargo-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/cargo-service /usr/local/bin/cargo-service
CMD ["cargo-service"]
