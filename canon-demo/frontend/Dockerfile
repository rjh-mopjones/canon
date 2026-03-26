# Stage 1: Build WASM with Trunk
FROM rust:1.87-bookworm AS builder

RUN rustup target add wasm32-unknown-unknown
RUN cargo install trunk --locked

WORKDIR /build

# Copy workspace files needed for dependency resolution
COPY Cargo.toml Cargo.lock ./
COPY canon-core/ canon-core/
COPY canon-core-macros/ canon-core-macros/
COPY canon-event-store/ canon-event-store/
COPY canon-command-store/ canon-command-store/
COPY canon-snapshot-store/ canon-snapshot-store/
COPY canon-inbox/ canon-inbox/
COPY canon-inbound-queue/ canon-inbound-queue/
COPY canon-outbound-queue/ canon-outbound-queue/
COPY canon-projection-store/ canon-projection-store/
COPY canon-publisher/ canon-publisher/
COPY canon-adaptor/ canon-adaptor/
COPY canon-deadletter/ canon-deadletter/
COPY canon-event-store-cassandra/ canon-event-store-cassandra/
COPY canon-command-store-yugabyte/ canon-command-store-yugabyte/
COPY canon-snapshot-store-yugabyte/ canon-snapshot-store-yugabyte/
COPY canon-inbox-yugabyte/ canon-inbox-yugabyte/
COPY canon-inbound-queue-kafka/ canon-inbound-queue-kafka/
COPY canon-outbound-queue-kafka/ canon-outbound-queue-kafka/
COPY canon-projection-store-yugabyte/ canon-projection-store-yugabyte/
COPY canon-publisher-kafka/ canon-publisher-kafka/
COPY canon-adaptor-kafka/ canon-adaptor-kafka/
COPY canon-deadletter-yugabyte/ canon-deadletter-yugabyte/
COPY canon-test/ canon-test/
COPY canon-demo/ canon-demo/

WORKDIR /build/canon-demo/frontend
RUN trunk build --release

# Stage 2: Serve with nginx
FROM nginx:alpine
COPY --from=builder /build/canon-demo/frontend/dist /usr/share/nginx/html
COPY canon-demo/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY canon-demo/frontend/docker-entrypoint.sh /docker-entrypoint.d/40-canon-config.sh
RUN chmod +x /docker-entrypoint.d/40-canon-config.sh
EXPOSE 80
