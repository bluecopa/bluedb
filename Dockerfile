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
# A debug build: the release build of this tree (tantivy fork + slatedb +
# gluesql + ring, with LTO/codegen) is RAM-hungry enough to OOM/wedge the Docker
# VM when it runs alongside the cluster. Debug compiles far faster and lighter,
# and runtime speed is irrelevant for the Jepsen/failover tests this image
# serves. (Flip to `--release` + `target/release` for a production image, and
# build it with the cluster stopped.)
#
# Cache the cargo registry and target dir across builds so a source-only change
# only recompiles the crates that changed. The artifacts live in the cache
# mounts, not image layers, so copy the finished binary out to a real path.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p bluedb-server \
    && cp target/debug/bluedb-server /usr/local/bin/bluedb-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/bluedb-server /usr/local/bin/bluedb-server
EXPOSE 8080
ENV BLUEDB_ADDR=0.0.0.0:8080
ENTRYPOINT ["bluedb-server"]
