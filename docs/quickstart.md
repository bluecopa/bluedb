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

- **MinIO** — the object store (the shared database lives here).
- **Postgres** — the lease arbiter that elects exactly one writer.
- **node1 / node2 / node3** — `bluedb-server` nodes on ports `8081` / `8082` /
  `8083`. One bootstraps as the **writer**; the others follow as **read
  replicas**.

## 2. Find the writer

```bash
curl -s localhost:8081/admin/status
```

The active writer reports it is active; replicas report passive. Writes must go
to the active writer (see [Administration](operations/admin.md)).

## 3. Run SQL

Send SQL to the writer's `/sql` endpoint:

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"}'

curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "INSERT INTO users VALUES (1, '"'"'ada'"'"'), (2, '"'"'lin'"'"')"}'

curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT name FROM users ORDER BY id"}'
```

You can also use the PostgREST-style REST surface at `/tables/{table}` — see
[REST API](api/rest.md).

## 4. Try a failover

Kill the writer and watch a standby take over within the lease TTL (~10s):

```bash
docker kill bluedb-node1-1          # or whichever node is active
sleep 12
curl -s localhost:8082/admin/status # a standby has promoted
```

No acknowledged write is lost — all three nodes share the same object-storage
database. See [Active-passive HA](ha/active-passive.md).

## Next steps

- [SQL reference](sql/README.md)
- [Consistency & guarantees](guarantees/consistency.md)
- [Deployment](deployment/local.md)
