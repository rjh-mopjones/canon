FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY target/aarch64-unknown-linux-musl/release/supply-service /usr/local/bin/supply-service
ENTRYPOINT ["supply-service"]
