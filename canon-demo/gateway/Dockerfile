FROM alpine:3.19
ARG CROSS_TARGET=aarch64-unknown-linux-musl
RUN apk add --no-cache ca-certificates
COPY target/${CROSS_TARGET}/release/gateway /usr/local/bin/gateway
ENTRYPOINT ["gateway"]
