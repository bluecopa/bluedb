# Quickstart

Bring up a local three-node bluedb cluster (shared MinIO object store, a
Postgres lease arbiter, and three `bluedb-server` nodes) and run your first
query.

## Prerequisites

- Docker + Docker Compose.

## 1. Start the cluster

From the repository root:

```bash
docker compose up -d --build
```

This starts:

- **MinIO**: the object store (the shared database lives here).
- **Postgres**: the lease arbiter that elects exactly one writer.
- **node1 / node2 / node3**: `bluedb-server` nodes on ports `8081` / `8082` /
  `8083`. One bootstraps as the **writer**; the others follow as **read
  replicas**.

## 2. Find the writer

```bash
curl -s localhost:8081/admin/status
```

The active writer reports it is active; replicas report passive. Writes must go
to the active writer (see [Administration](operations/admin.md)).

## 3. Create a table and run SQL

DDL is **typed JSON** on the structured `/schema/*` endpoints; `/sql` runs one
non-DDL statement. Create the table on the writer, then read and write it through
`/sql`:

```bash
# Create the table (structured DDL)
curl -s localhost:8081/schema/tables -H 'content-type: application/json' \
  -d '{"name": "users", "columns": [
        {"name": "id",   "type": "INTEGER", "primaryKey": true},
        {"name": "name", "type": "TEXT"}
      ]}'

# Insert two rows, then read them back (one statement per /sql request)
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (1, '"'"'ada'"'"')"}'

curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (2, '"'"'lin'"'"')"}'

curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT name FROM users ORDER BY id"}'
```

`/sql` runs a single non-DDL statement (`SELECT`/`INSERT`/`UPDATE`/`DELETE`) and
is the **transactional, read-your-writes** surface: a `SELECT` filtered by the
primary key or an index is served at lookup latency, fresh from your last write.
A read that would scan (a non-indexed filter, a join, an aggregate) is rejected
with `400 NO_INDEX` — run it on the **analytical** surface, `/query`, instead.
DDL (`CREATE`/`DROP`/`ALTER`) goes to the structured
[`/schema/*`](api/rest.md#schema-ddl-endpoints) endpoints (or `/admin/sql` if
enabled). You can also use the PostgREST-style surface at `/tables/{table}`. See
[REST API](api/rest.md) and [Reads and indexes](sql/query-guardrail.md).

## 4. Try a failover

Kill the writer and watch a standby take over within the lease TTL (about 10s):

```bash
docker kill bluedb-node1-1          # or whichever node is active
sleep 12
curl -s localhost:8082/admin/status # a standby has promoted
```

No acknowledged write is lost, because all three nodes share the same object-storage
database. See [Active-passive HA](ha/active-passive.md).

## Next steps

- [SQL reference](sql/README.md)
- [Consistency & guarantees](guarantees/consistency.md)
- [Deployment](deployment/local.md)
