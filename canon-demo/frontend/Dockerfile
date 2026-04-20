# syntax=docker/dockerfile:1
#
# Multi-stage: build the WASM bundle with Trunk, then serve via nginx.
# dist/ is NOT committed — it is produced here from source every build.

FROM rust:1-bookworm AS builder
ARG TRUNK_VERSION=0.21.14
RUN rustup target add wasm32-unknown-unknown \
 && curl -fsSL "https://github.com/trunk-rs/trunk/releases/download/v${TRUNK_VERSION}/trunk-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C /usr/local/bin trunk \
 && chmod +x /usr/local/bin/trunk
WORKDIR /build
COPY . .
WORKDIR /build/canon-demo/frontend
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    trunk build --release

FROM nginx:alpine
COPY --from=builder /build/canon-demo/frontend/dist /usr/share/nginx/html
COPY canon-demo/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY canon-demo/frontend/docker-entrypoint.sh /docker-entrypoint.d/40-canon-config.sh
RUN chmod +x /docker-entrypoint.d/40-canon-config.sh
EXPOSE 80
