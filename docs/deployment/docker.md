# Docker

`bluedb-server` ships as a single container image (see the repository
`Dockerfile`). A node is **stateless** (all authoritative state is in the object
store), so you scale or replace nodes freely.

## Build

```bash
docker build -t bluedb-server:dev .
```

## Run a node

Point a node at an object store and (for multi-node) a lease arbiter, entirely
through environment variables:

```bash
docker run --rm -p 8080:8080 \
  -e BLUEDB_S3_BUCKET=bluedb \
  -e BLUEDB_S3_ENDPOINT=http://minio:9000 \
  -e BLUEDB_S3_REGION=us-east-1 \
  -e BLUEDB_S3_ACCESS_KEY_ID=minioadmin \
  -e BLUEDB_S3_SECRET_ACCESS_KEY=minioadmin \
  -e BLUEDB_DB_PATH=bluedb \
  -e BLUEDB_LEASE_PG_URL=postgresql://postgres:bluedb@postgres:5432/bluedb \
  -e BLUEDB_NODE_ID=node-a \
  bluedb-server:dev
```

The node logs which object store and lease arbiter it selected, and whether it
bootstrapped as the writer or started as a replica.

## Object-store backends

bluedb picks the backend from the environment:

- **S3 / MinIO / any S3-compatible**: set `BLUEDB_S3_BUCKET` (+ `BLUEDB_S3_ENDPOINT`
  for non-AWS, region, and credentials).
- **Local filesystem**: set `BLUEDB_DATA_DIR` (single-node persistence).
- **In-memory**: set neither (ephemeral; single node only).

GCS and Azure Blob are supported by the substrate via the same S3-style
configuration where the provider offers an S3-compatible endpoint; otherwise use
the provider's gateway.

## Multi-node

Run several nodes pointed at the **same** bucket + `BLUEDB_DB_PATH` and the
**same** `BLUEDB_LEASE_PG_URL`, each with a distinct `BLUEDB_NODE_ID`. The lease
arbiter elects one writer; the rest serve reads. This is exactly what the
[Compose stack](local.md) does. See [Configuration](configuration.md) for the
full variable list and [Kubernetes](kubernetes.md) for an orchestrated topology.
