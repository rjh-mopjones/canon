FROM rustlang/rust:nightly AS builder
RUN rustup target add wasm32-unknown-unknown
RUN cargo install trunk wasm-bindgen-cli
WORKDIR /app
COPY . .
WORKDIR /app/canon-demo/frontend
RUN trunk build --release

FROM nginx:alpine
COPY --from=builder /app/canon-demo/frontend/dist /usr/share/nginx/html
COPY canon-demo/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY canon-demo/frontend/docker-entrypoint.sh /docker-entrypoint.d/40-canon-config.sh
RUN chmod +x /docker-entrypoint.d/40-canon-config.sh
EXPOSE 80
