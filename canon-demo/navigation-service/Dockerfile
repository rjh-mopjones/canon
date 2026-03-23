FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY target/aarch64-unknown-linux-musl/release/navigation-service /usr/local/bin/navigation-service
ENTRYPOINT ["navigation-service"]
