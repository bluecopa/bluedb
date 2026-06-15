# Configuration

`bluedb-server` is configured entirely through environment variables.

## Server

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_ADDR` | `0.0.0.0:8080` | Listen address |
| `BLUEDB_DB_PATH` | `bluedb` | SlateDB path/prefix inside the object store |
| `BLUEDB_NODE_ID` | `node-0` | This node's identity (use the pod name in K8s) |
| `BLUEDB_START_PASSIVE` | unset | Start as a read replica and wait to be promoted (default bootstraps to writer) |
| `BLUEDB_FLUSH_INTERVAL_MS` | `25` | WAL flush interval (ms), set at writer open. Lower = lower write latency but more object-store PUTs under load |
| `BLUEDB_FTS_SEAL_INTERVAL_MS` | `30000` | Interval (ms) for the background [full-text](../sql/full-text-search.md) seal/compaction scheduler — folds the in-memory live segment into durable splits |

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

## API surface & authorization

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_ENABLE_ADMIN_SQL` | unset (off) | Set to `1`/`true` to enable [`POST /admin/sql`](../api/rest.md#post-adminsql-arbitrary-sql-off-by-default) — arbitrary, audited SQL (DDL/txn/multi). Off by default |
| `BLUEDB_AUTHZ_TOKENS` | unset (open mode) | Bearer-token → scope map. When unset the server runs in **open mode** (all requests allowed) — production should always set this |

`BLUEDB_AUTHZ_TOKENS` uses the format `tok1=scope,scope;tok2=scope` —
semicolon-separated token entries, each a token followed by `=` and a
comma-separated scope list. Recognized scopes: `data:read`, `data:write`,
`data:query`, `schema:admin`, `superuser` (which satisfies any required scope).
See the [REST API authorization table](../api/rest.md#authorization) for the
per-route scopes.

```bash
BLUEDB_AUTHZ_TOKENS='reader=data:read;writer=data:read,data:write,data:query;admin=superuser'
```

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
