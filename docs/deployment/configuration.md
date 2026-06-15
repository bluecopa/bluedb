# Configuration

`bluedb-server` is configured entirely through environment variables.

## Server

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_ADDR` | `0.0.0.0:8080` | Listen address |
| `BLUEDB_DB_PATH` | `bluedb` | SlateDB path/prefix inside the object store |
| `BLUEDB_NODE_ID` | `node-0` | This node's identity (use the pod name in K8s) |
| `BLUEDB_START_PASSIVE` | unset | Start as a read replica and wait to be promoted (default bootstraps to writer) |

## Object store

Select **one** backend:

| Variable | Meaning |
|----------|---------|
| `BLUEDB_S3_BUCKET` | Use S3/MinIO/S3-compatible (presence selects this backend) |
| `BLUEDB_S3_ENDPOINT` | Endpoint URL for non-AWS (e.g. MinIO, `http://minio:9000`) |
| `BLUEDB_S3_REGION` | Region (default `us-east-1`) |
| `BLUEDB_S3_ACCESS_KEY_ID` | Access key |
| `BLUEDB_S3_SECRET_ACCESS_KEY` | Secret key |
| `BLUEDB_DATA_DIR` | Use a local filesystem object store instead (single-node) |

With none of the above set, the node uses an **in-memory** store (ephemeral,
single node only).

## Lease / high availability

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_LEASE_PG_URL` | unset | Postgres lease arbiter for multi-node election. If unset, an in-process lease is used (single writer) |
| `BLUEDB_LEASE_TTL_SECS` | `15` | Lease lifetime; bounds failover time |
| `BLUEDB_LEASE_MARGIN_SECS` | `5` | Self-fence this far before lease expiry |

See [Active-passive HA](../ha/active-passive.md) for how TTL and margin govern
failover timing and safety.

## Example: one node against MinIO + Postgres

```bash
BLUEDB_S3_BUCKET=bluedb \
BLUEDB_S3_ENDPOINT=http://minio:9000 \
BLUEDB_S3_ACCESS_KEY_ID=minioadmin \
BLUEDB_S3_SECRET_ACCESS_KEY=minioadmin \
BLUEDB_DB_PATH=bluedb \
BLUEDB_LEASE_PG_URL=postgresql://postgres:bluedb@postgres:5432/bluedb \
BLUEDB_NODE_ID=node-a \
  bluedb-server
```

!!! tip
    The same variables drive [Docker](docker.md), [Compose](local.md), and
    [Kubernetes](kubernetes.md) — only how you supply them differs.
