FROM rustlang/rust:nightly AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p supply-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/supply-service /usr/local/bin/supply-service
CMD ["supply-service"]
