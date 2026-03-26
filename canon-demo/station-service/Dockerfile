FROM alpine:3.19
RUN apk add --no-cache ca-certificates
ARG BINARY_PATH=target/x86_64-unknown-linux-musl/release/station-service
COPY ${BINARY_PATH} /usr/local/bin/station-service
ENTRYPOINT ["station-service"]
