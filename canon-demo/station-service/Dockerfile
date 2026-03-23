FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY target/aarch64-unknown-linux-musl/release/station-service /usr/local/bin/station-service
ENTRYPOINT ["station-service"]
