FROM alpine:3.19
ARG CROSS_TARGET=aarch64-unknown-linux-musl
RUN apk add --no-cache ca-certificates
COPY target/${CROSS_TARGET}/release/cargo-service /usr/local/bin/cargo-service
ENTRYPOINT ["cargo-service"]
