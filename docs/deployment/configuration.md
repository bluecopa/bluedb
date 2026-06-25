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
| `BLUEDB_FTS_SEAL_INTERVAL_MS` | `30000` | Interval (ms) for the background [full-text](../sql/full-text-search.md) seal/compaction scheduler; folds the in-memory live segment into durable splits |

## Object store

bluedb runs on any of three clouds (plus local disk). Select **one** backend:
the first family whose selector variable is present wins, in the order
**S3 → Azure → GCS → local → in-memory**.

**S3 / MinIO / any S3-compatible store:**

| Variable | Meaning |
|----------|---------|
| `BLUEDB_S3_BUCKET` | Use S3/MinIO/S3-compatible (presence selects this backend) |
| `BLUEDB_S3_ENDPOINT` | Endpoint URL for non-AWS (e.g. MinIO, `http://minio:9000`) |
| `BLUEDB_S3_REGION` | Region (default `us-east-1`) |
| `BLUEDB_S3_ACCESS_KEY_ID` | Access key |
| `BLUEDB_S3_SECRET_ACCESS_KEY` | Secret key |

**Azure Blob Storage:**

| Variable | Meaning |
|----------|---------|
| `BLUEDB_AZURE_CONTAINER` | Use Azure Blob (presence selects this backend) |
| `BLUEDB_AZURE_ACCOUNT` | Storage account name |
| `BLUEDB_AZURE_ACCESS_KEY` | Account access key |
| `BLUEDB_AZURE_ENDPOINT` | Override the blob endpoint (full account URL) for Azurite or any Azure-compatible store; implies plain HTTP |

**Google Cloud Storage:**

| Variable | Meaning |
|----------|---------|
| `BLUEDB_GCS_BUCKET` | Use GCS (presence selects this backend) |
| `BLUEDB_GCS_SERVICE_ACCOUNT` | Path to a service-account JSON key |

**Local / in-memory:**

| Variable | Meaning |
|----------|---------|
| `BLUEDB_DATA_DIR` | Use a local filesystem object store instead (single-node) |

With none of the above set, the node uses an **in-memory** store (ephemeral,
single node only).

!!! note "Cloud verification"
    All three clouds are exercised end-to-end (a real SlateDB `Db`
    write → close → reopen → read, including the conditional-put used for
    single-writer safety). **S3** and **Azure** are covered by emulator
    round-trip tests (MinIO, Azurite). **GCS** is verified against **real GCS**
    Its backend uses the GCS *XML* API, which the common local emulators
    (fake-gcs, storage-testbench) don't fully serve, so the round-trip test
    (`gcs_real_round_trip`) is env-gated against an actual bucket. See
    `crates/bluedb-server/tests/objstore_emulators.rs`.

## Lease / high availability

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_LEASE_BACKEND` | unset | `postgres`, `kubernetes`, or `local`. If unset, `BLUEDB_LEASE_PG_URL` selects Postgres; otherwise an in-process lease is used (single writer) |
| `BLUEDB_LEASE_PG_URL` | unset | Postgres lease arbiter URL for multi-node election when using the Postgres backend |
| `BLUEDB_K8S_LEASE_NAME` | `bluedb-writer` | Kubernetes `coordination.k8s.io/Lease` object name when `BLUEDB_LEASE_BACKEND=kubernetes` |
| `BLUEDB_K8S_NAMESPACE` | `default` | Kubernetes namespace for the Lease object and EndpointSlice registry |
| `BLUEDB_LEASE_TTL_SECS` | `15` | Lease lifetime; bounds failover time |
| `BLUEDB_LEASE_MARGIN_SECS` | `5` | Self-fence this far before lease expiry |

See [Active-passive HA](../ha/active-passive.md) for how TTL and margin govern
failover timing and safety.

## Lakehouse mirror

See [Iceberg mirror](../lakehouse/iceberg-mirror.md). All optional. The mirror is
off until enabled with `PRAGMA lakehouse_mirror`.

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_LAKEHOUSE_ROOT` | `lakehouse` | Object-store key prefix for the Iceberg tables (same bucket as the data) |
| `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS` | `2000` | Coalesce a burst of commits this long before sealing |
| `BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS` | `10000` | Cap on how long a steady write stream delays a seal |
| `BLUEDB_LAKEHOUSE_COMPACTION_INTERVAL_MS` | `60000` | How often the compaction worker runs |
| `BLUEDB_LAKEHOUSE_MAX_DATA_FILES` | `8` | Minor-compact (bin-pack small files) a table once it exceeds this many data files |
| `BLUEDB_LAKEHOUSE_MAX_DELETE_FILES` | `16` | Major-compact (whole-table rewrite, reclaims delete files) once it exceeds this many delete files |
| `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` | `134217728` | Bin-pack target file size; default when no `PRAGMA lakehouse_target_file_bytes` is set |

## API surface & authorization

| Variable | Default | Meaning |
|----------|---------|---------|
| `BLUEDB_ENABLE_ADMIN_SQL` | unset (off) | Set to `1`/`true` to enable [`POST /admin/sql`](../api/rest.md#post-adminsql-arbitrary-sql-off-by-default): arbitrary, audited SQL (DDL/txn/multi). Off by default |
| `BLUEDB_AUTHZ_TOKENS` | unset (open mode) | Bearer-token → scope map. When unset the server runs in **open mode** (all requests allowed); production should always set this |

`BLUEDB_AUTHZ_TOKENS` uses the format `tok1=scope,scope;tok2=scope`:
semicolon-separated token entries, each a token followed by `=` and a
comma-separated scope list. Recognized scopes: `data:read`, `data:write`,
`data:query`, `schema:admin`, `superuser` (which satisfies any required scope).
See the [REST API authorization table](../api/rest.md#authorization) for the
per-route scopes.

```bash
BLUEDB_AUTHZ_TOKENS='reader=data:read;writer=data:read,data:write,data:query;admin=superuser'
```

## Multi-tenancy

Every request is scoped to a **tenant** via the `X-Bluedb-Tenant` request header
(absent ⇒ the default tenant `_`). Tenants have fully isolated keyspaces, and
for the [lakehouse mirror](../lakehouse/iceberg-mirror.md#multi-tenancy), a
separate Iceberg namespace each. Tenant names allow letters, digits, `_`, and
`-`. No env var is needed to enable multi-tenancy; it is always on.

Bind a token to one or more tenants with a `tenant:<name>` entry in
`BLUEDB_AUTHZ_TOKENS` (alongside its scopes). The request's `X-Bluedb-Tenant`
must then match one of the token's tenants; a `superuser` token reaches any
tenant, and a token with **no** `tenant:` entry may reach only the default
tenant (so single-tenant configs keep working). In open mode (no
`BLUEDB_AUTHZ_TOKENS`) the header is trusted.

```bash
# acme-scoped writer; globex-scoped reader; an admin that spans all tenants
BLUEDB_AUTHZ_TOKENS='acme=data:read,data:write,data:query,tenant:acme;\
globex=data:read,tenant:globex;admin=superuser'
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
    [Kubernetes](kubernetes.md); only how you supply them differs.
