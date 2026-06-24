# Docker

`bluedb-server` ships as a single container image (see the repository
`Dockerfile`). A node is **stateless** (all authoritative state is in the object
store), so you scale or replace nodes freely.

## Build

```bash
docker build -t bluedb-server:dev .
```

For a Kubernetes image that can use the native `coordination.k8s.io/Lease`
arbiter and EndpointSlice node registry:

```bash
docker build \
  --build-arg BLUEDB_CARGO_FEATURES=kubernetes \
  -t bluedb-server:k8s .
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
same lease backend, each with a distinct `BLUEDB_NODE_ID`. Outside Kubernetes,
use the same `BLUEDB_LEASE_PG_URL`. In Kubernetes, build with the `kubernetes`
feature and set `BLUEDB_LEASE_BACKEND=kubernetes`. The lease arbiter elects one
writer; the rest serve reads. See [Configuration](configuration.md) for the full
variable list and [Kubernetes](kubernetes.md) for an orchestrated topology.
