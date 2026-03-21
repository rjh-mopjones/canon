FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY target/aarch64-unknown-linux-gnu/release/cargo-service /usr/local/bin/cargo-service
CMD ["cargo-service"]
