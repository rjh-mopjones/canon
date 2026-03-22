FROM canon-builder:latest AS builder
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 libsasl2-2 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/canon/target/release/supply-service /usr/local/bin/supply-service
CMD ["supply-service"]
