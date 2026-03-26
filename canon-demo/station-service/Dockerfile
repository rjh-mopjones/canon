FROM alpine:3.19
ARG CROSS_TARGET=aarch64-unknown-linux-musl
RUN apk add --no-cache ca-certificates
COPY target/${CROSS_TARGET}/release/station-service /usr/local/bin/station-service
ENTRYPOINT ["station-service"]
