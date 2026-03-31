# Stage 1: Build mdBook
FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev
RUN cargo install mdbook --version 0.4.40

COPY book.toml /book/book.toml
COPY src/ /book/src/
COPY theme/ /book/theme/

WORKDIR /book
RUN mdbook build

# Stage 2: Serve with nginx
FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /book/book/ /usr/share/nginx/html/

EXPOSE 80
