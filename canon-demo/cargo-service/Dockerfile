FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY target/aarch64-unknown-linux-musl/release/cargo-service /usr/local/bin/cargo-service
ENTRYPOINT ["cargo-service"]
