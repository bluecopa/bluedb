# syntax=docker/dockerfile:1
#
# Multi-stage build of the bluedb-server binary (with the `postgres` lease
# arbiter feature). The host is macOS, so the linux binary must be built in the
# container.

FROM rust:1-slim-bookworm AS builder
# Native deps for the C-backed crates in the tree: zstd-sys / lz4-sys (cc),
# ring (cc + perl), and a few -sys crates (cmake, pkg-config).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential pkg-config cmake perl git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release -p bluedb-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bluedb-server /usr/local/bin/bluedb-server
EXPOSE 8080
ENV BLUEDB_ADDR=0.0.0.0:8080
ENTRYPOINT ["bluedb-server"]
