FROM alpine:3.19
RUN apk add --no-cache ca-certificates
ARG BINARY_PATH=target/x86_64-unknown-linux-musl/release/gateway
COPY ${BINARY_PATH} /usr/local/bin/gateway
ENTRYPOINT ["gateway"]
