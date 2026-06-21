# REST API

`bluedb-server` exposes a **PostgREST-style** HTTP API over five capability
surfaces: a data plane (`/tables/{table}`), a transactional SQL surface for
reads and writes (`/sql`), an analytical SQL surface for scans and joins
(`/query`), structured DDL (`/schema/*`), and an off-by-default arbitrary-SQL
admin escape hatch (`/admin/sql`). A [double-entry ledger](ledger.md) is
mounted at `/ledger/*`. The listener speaks HTTP/1.1 and HTTP/2 (h2c,
prior-knowledge), so an h2 client can multiplex many concurrent in-flight
writes over one connection.

The two SQL surfaces split by performance contract: **`/sql`** is your
read-your-writes OLTP surface (index-only reads at lookup latency, plus writes),
and **`/query`** is your analytical/HTAP surface (joins, aggregates, scans — no
index required, at scan latency). See
[Reads and indexes](../sql/query-guardrail.md).

All write methods (`POST`/`PATCH`/`DELETE`, `POST /sql`, every `/schema/*`
endpoint, and the ledger create endpoints) are accepted only by the
[active writer](../operations/admin.md); reads (`GET`) are served by any node.

## Authorization

Each request carries a bearer token:

```
Authorization: Bearer <token>
```

Tokens map to **scopes** via the `BLUEDB_AUTHZ_TOKENS` env var
(format `tok1=scope,scope;tok2=scope`; see
[Configuration](../deployment/configuration.md)). When that variable is **unset**
the server runs in **open mode**: every request is allowed without a token.
Production deployments should always set it. The `superuser` scope satisfies any
required scope.

Per-route scopes:

| Route | Required scope |
|-------|----------------|
| `GET /tables/{table}` | `data:read` |
| `POST` / `PATCH` / `DELETE /tables/{table}` | `data:write` |
| `POST /sql` | `data:query` |
| `POST /query` | `data:query` |
| `POST /admin/sql` | `superuser` |
| `POST` / `DELETE /schema/*` | `schema:admin` |
| `GET /catalog/v1/*` | `data:read` |
| `POST /admin/promote`, `POST /admin/demote` | `superuser` |
| `GET /health`, `GET /admin/status` | public (no token) |

### Tenant selection

Every data, schema, and catalog request is scoped to a **tenant** chosen by the
`X-Bluedb-Tenant` header (absent ⇒ the default tenant `_`):

```
X-Bluedb-Tenant: acme
```

Tenants are fully isolated (separate keyspace and Iceberg namespace). A token may
be bound to specific tenants with a `tenant:<name>` scope; it can then act only
on those tenants (a `superuser` reaches any, an unbound token only the default
tenant). See [Multi-tenancy](../deployment/configuration.md#multi-tenancy).

## `POST /sql`: transactional reads & writes (read-your-writes)

`/sql` is the **transactional, read-your-writes** surface. It runs a single,
parameterized, non-DDL statement — one `SELECT`/`INSERT`/`UPDATE`/`DELETE`,
optionally with `RETURNING`. Reads are served from the live row store at
**lookup latency**, and writes are acknowledged when durable. Bind values with
`$N` placeholders and a `params` array (injection-proof by construction); never
interpolate values into the SQL string.

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT name FROM users WHERE id = $1", "params": [42]}'
```

!!! important "Reads are index-only"
    A `/sql` `SELECT` must be served by the primary key or a secondary index
    (point, range, prefix, keyset, or an indexed equality). Anything that would
    require a scan — a non-indexed `WHERE`/`ORDER BY`, a join, an aggregate, a
    window function, a JSON path — is **rejected with `400 NO_INDEX`**, and the
    error names the exact index to create. For those reads use
    [`POST /query`](#post-query-analytical-reads-htap). See
    [Reads and indexes](../sql/query-guardrail.md).

A read served on the writer reflects every write it has acknowledged, so a
`SELECT … WHERE pk = …` immediately after the write is read-your-writes at
lookup latency — no seal cycle, no mirror lag. A write response carries the
sequence it reached in `X-Bluedb-Watermark`; echo it on a subsequent read as
`X-Bluedb-Min-Watermark` to enforce freshness (see
[Read-your-writes & freshness](#read-your-writes-freshness)).

### `RETURNING`: get the affected rows back

Add `RETURNING <cols>` (or `RETURNING *`) to an `INSERT`/`UPDATE`/`DELETE` to
receive the affected rows instead of a count — one round trip:

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "INSERT INTO users (id, name) VALUES (1, '\''ada'\'') RETURNING id, name"}'
# [{"id":1,"name":"ada"}]

curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "DELETE FROM users WHERE id = 1 RETURNING name"}'
# [{"name":"ada"}]   — the row, captured before deletion
```

`DELETE … RETURNING` reads the matching rows **before** they are removed.
`UPDATE … RETURNING` re-selects by the statement's `WHERE`, so if the `UPDATE`
changed a column the filter tests, the returned set reflects the post-update
matches. A write that matched no rows returns `[]`.

!!! note "Full-text search"
    `/sql` also accepts the PostgreSQL full-text surface (`to_tsvector(cfg, col)
    @@ plainto_tsquery($1)`, `ts_rank(...)`, and trigram-accelerated
    `col LIKE '%lit%'`), which is rewritten transparently against the live index
    (to a `pk IN (...)` predicate, which the read path serves as a point lookup).
    See [Full-text search](../sql/full-text-search.md).

!!! warning "DDL goes elsewhere"
    `CREATE`/`DROP`/`ALTER`, transactions, and multi-statement scripts are **not**
    accepted here. Use the structured [`/schema/*`](#schema-ddl-endpoints)
    endpoints, or `/admin/sql` for the raw escape hatch.

## `POST /query`: analytical reads (HTAP)

`/query` is the **analytical** surface and the counterpart to `/sql`. It runs a
single `SELECT` through the DataFusion front door over the tenant's Iceberg
mirror ∪ the unsealed CDC tail: joins, `GROUP BY`/aggregates, window functions,
CTEs (including `WITH RECURSIVE`), set operations, JSON paths, and arbitrary
non-indexed filters/sorts all belong here. It never rejects a read on indexing
grounds.

```bash
curl -s localhost:8081/query -H 'content-type: application/json' \
  -d '{"sql": "SELECT category, COUNT(*) FROM products GROUP BY category"}'
```

It accepts the same `$N` parameters and JSON body as `/sql`, and the same
[`X-Bluedb-Min-Watermark`](#read-your-writes-freshness) freshness gate applies.
On the active writer it is read-your-writes (the unsealed tail union); on a
replica it is bounded-stale at the sealed snapshot. Reads here run at **scan
latency** (columnar Parquet + CDC-tail merge), not the lookup latency of `/sql` —
that is the trade: full analytical flexibility, no index required.

| Read shape | `/sql` | `/query` |
|------------|:------:|:--------:|
| `WHERE pk = …` / `WHERE pk > … ORDER BY pk` | ✅ lookup | ✅ scan |
| `WHERE <indexed-col> = …` | ✅ lookup | ✅ scan |
| `WHERE <non-indexed> = …` | ❌ `400 NO_INDEX` | ✅ scan |
| Join / aggregate / window / CTE | ❌ | ✅ |
| JSON path (`data->>'status'`) | ❌ | ✅ |
| Full-text `@@` (single-table) | ✅ lookup | ✅ scan |

`GLUE_*` catalog introspection views are transactional synthetics only `/sql`
can resolve; a `/query` that references one returns `400 UNSUPPORTED_STATEMENT`.
Writes and DDL are not accepted on `/query` (use `/sql` and `/schema`).



## `POST /admin/sql`: arbitrary SQL (off by default)

The raw escape hatch: arbitrary SQL including DDL, transactions, and
multi-statement scripts. It is **disabled by default** and only enabled when
`BLUEDB_ENABLE_ADMIN_SQL=1` (or `true`). Every call is audited, and the route
requires the `superuser` scope.

```bash
curl -s localhost:8081/admin/sql -H 'content-type: application/json' \
  -H 'authorization: Bearer <superuser-token>' \
  -d '{"sql": "BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT;"}'
```

`/admin/sql` does **not** rewrite the `@@` full-text surface; it is left as the
literal raw passthrough.

## `GET /tables/{table}`: select

Filters and modifiers go in the query string, PostgREST-style:

```bash
# SELECT name, age FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10
curl -s 'localhost:8081/tables/users?select=name,age&age=gte.18&order=age.desc&limit=10'
```

Common modifiers:

| Query param | Meaning |
|-------------|---------|
| `select=col,col` | Columns to return |
| `col=eq.X` / `gt.` / `gte.` / `lt.` / `lte.` / `neq.` | Filter operators |
| `order=col.asc` / `col.desc` | Ordering |
| `limit=N` / `offset=N` | Pagination |

A point or range filter on the primary key or a secondary-indexed column is
served from the transactional store (fresh, read-your-writes). A filter or sort
on any other column (or a JSON path, below) is **routed to the analytical
engine** automatically; you never add an index just to make a read run. See
[Reads and indexes](../sql/query-guardrail.md).

### JSON-path filters on `/tables`

A [JSON](../sql/json.md) column can be filtered and projected by path with the
PostgREST `col->>key` syntax (extract as text) or `col->key` (extract as JSON):

```bash
# SELECT * FROM events WHERE (attrs ->> 'status') = 'active'
curl -s 'localhost:8081/tables/events?attrs->>status=eq.active'

# project a nested field
curl -s 'localhost:8081/tables/events?select=id,attrs->>status&order=id.asc'
```

A JSON-path read is served by the analytical engine. Containment (`@>`) and
`jsonb_path_query` are available over [`POST /query`](#post-query-analytical-reads-htap).

### Pagination total: `Prefer: count=exact`

Send `Prefer: count=exact` to get the total row count (ignoring `limit`/`offset`)
in a PostgREST `Content-Range` response header, so a grid can show "page 1 of N":

```bash
curl -si 'localhost:8081/tables/users?limit=25' -H 'Prefer: count=exact'
# Content-Range: 0-24/137
```

The format is `first-last/total` (`*/0` when no rows match). The count runs on
the same engine that served the data.

### Value encoding

`UUID` values are returned as their canonical hyphenated string; `BYTEA` as
**base64**. `JSON` / `JSONB` columns are returned as real JSON (objects/arrays),
not as a quoted string.

## `POST /tables/{table}`: insert

A JSON object, or an array of objects for a batch:

```bash
curl -s -X POST localhost:8081/tables/users -H 'content-type: application/json' \
  -d '{"id": 1, "name": "ada", "age": 36}'

curl -s -X POST localhost:8081/tables/users -H 'content-type: application/json' \
  -d '[{"id": 2, "name": "lin"}, {"id": 3, "name": "sam"}]'
```

## `PATCH /tables/{table}`: update

Assignments in the body, rows selected by the query-string filter:

```bash
# UPDATE users SET age = 37 WHERE id = 1
curl -s -X PATCH 'localhost:8081/tables/users?id=eq.1' \
  -H 'content-type: application/json' -d '{"age": 37}'
```

## `DELETE /tables/{table}`: delete

```bash
# DELETE FROM users WHERE age < 18
curl -s -X DELETE 'localhost:8081/tables/users?age=lt.18'
```

!!! warning "A filter is required"
    A `PATCH`/`DELETE` with **no query-string filter is rejected** with
    `400 "refusing to render UPDATE/DELETE with no filters (would affect every
    row)"`. Always include a filter. To affect every row, use an always-true
    predicate against an indexed column (e.g. `id=gte.0` on an integer primary
    key), or run the bulk mutation through `POST /sql`
    (`UPDATE`/`DELETE` without a `WHERE`).

### Returning the affected rows: `Prefer: return=representation`

By default a write returns a count: `{"inserted": 1}`, `{"updated": 3}`,
`{"deleted": 2}`. Send `Prefer: return=representation` to get the **affected rows
themselves** back instead (the same JSON shape a `GET` returns, with JSON columns
re-inflated):

```bash
curl -s -X POST 'localhost:8081/tables/users' \
  -H 'content-type: application/json' -H 'Prefer: return=representation' \
  -d '{"id": 1, "name": "ada", "age": 36}'
# [{"id":1,"name":"ada","age":36}]
```

- `POST` reads the inserted rows back by primary key.
- `PATCH` returns the rows after the update (re-selected by the same filter).
- `DELETE` captures the matching rows **before** removing them, so you get the
  deleted rows in the response.

!!! note
    For an `INSERT` whose rows can't be identified by primary key after the fact
    (a multi-row insert into a table with a composite key), the response falls
    back to the count form.

## Schema DDL endpoints

DDL is expressed as **typed JSON**, never raw SQL: every identifier is checked
against `^[A-Za-z_][A-Za-z0-9_]*$` and every column type against an explicit
allow-list before the server builds the DDL. All `/schema/*` endpoints require
the `schema:admin` scope and the active writer.

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/schema/tables` | Create a table from a typed column spec |
| `GET` | `/schema/tables/{table}` | Describe a table: columns and indexes (`data:read`) |
| `DELETE` | `/schema/tables/{table}` | Drop a table |
| `POST` | `/schema/tables/{table}/indexes` | Create a secondary index |
| `DELETE` | `/schema/tables/{table}/indexes/{name}` | Drop an index |
| `POST` | `/schema/tables/{table}/fulltext-indexes` | Declare a full-text index on a text column |
| `POST` | `/schema/tables/{table}/trigram-indexes` | Declare a trigram index on a text column |

### Create a table

```bash
curl -s -X POST localhost:8081/schema/tables -H 'content-type: application/json' \
  -d '{
        "name": "docs",
        "columns": [
          {"name": "id",     "type": "INTEGER", "primaryKey": true},
          {"name": "title",  "type": "TEXT",    "nullable": false},
          {"name": "body",   "type": "TEXT"},
          {"name": "status", "type": "TEXT"},
          {"name": "attrs",  "type": "JSON"}
        ],
        "indexes": [
          {"name": "docs_status", "columns": ["status"]}
        ]
      }'
```

Each column takes `name`, `type`, and optionally `primaryKey` (camelCase),
`nullable` (defaults to `true`), and `unique`. Allowed types: `TEXT`,
`INTEGER`/`INT`, `BOOLEAN`/`BOOL`, `FLOAT`, `DECIMAL`, `DATE`, `TIME`,
`TIMESTAMP`, `UUID`, `JSON`/`JSONB` (see [JSON](../sql/json.md)).

The optional `indexes` array declares secondary indexes in the **same schema
apply**: each entry is `{"name": …, "columns": […]}`, equivalent to a follow-up
`POST …/indexes` but atomic with the create.

### Describe a table

`GET /schema/tables/{table}` returns the table's columns and indexes as
structured JSON (the composite-primary-key surrogate column is hidden):

```bash
curl -s localhost:8081/schema/tables/docs
```

```json
{
  "table": "docs",
  "columns": [
    {"name": "id",     "type": "INT",  "primary_key": true,  "indexed": true,  "nullable": false},
    {"name": "title",  "type": "TEXT", "primary_key": false, "indexed": false, "nullable": false},
    {"name": "status", "type": "TEXT", "primary_key": false, "indexed": true,  "nullable": true},
    {"name": "attrs",  "type": "JSON", "primary_key": false, "indexed": false, "nullable": true}
  ],
  "indexes": [
    {"name": "docs_status", "column": "status"}
  ]
}
```

`indexed` is `true` for a primary-key column or one backed by a secondary index,
i.e. the columns a point/range filter is served from without an analytical scan.

### Create / drop an index

```bash
# CREATE INDEX docs_status ON docs (status)
curl -s -X POST localhost:8081/schema/tables/docs/indexes \
  -H 'content-type: application/json' \
  -d '{"name": "docs_status", "columns": ["status"]}'

# DROP INDEX docs.docs_status
curl -s -X DELETE localhost:8081/schema/tables/docs/indexes/docs_status
```

### Declare a full-text or trigram index

A full-text index takes the text `column` and an optional `analyzer` (defaults to
`"english"`); a trigram index takes only the `column` (it always tokenizes
through the `whitespace` analyzer). In both cases the table's integer primary key
is resolved automatically.

```bash
# Full-text (BM25) index on docs.body
curl -s -X POST localhost:8081/schema/tables/docs/fulltext-indexes \
  -H 'content-type: application/json' \
  -d '{"column": "body", "analyzer": "english"}'

# Trigram index on docs.body (accelerates LIKE '%lit%')
curl -s -X POST localhost:8081/schema/tables/docs/trigram-indexes \
  -H 'content-type: application/json' \
  -d '{"column": "body"}'
```

Once declared, query both through [`/sql`](../sql/full-text-search.md).

## Read-your-writes & freshness

Every **mutating** response carries the sequence the write reached:

```
X-Bluedb-Watermark: acme:42
```

(`<tenant>:<seq>`.) The sequence is the writer's per-tenant commit counter. It
advances on every committed `INSERT`, `UPDATE`, and `DELETE`, so a write
response always carries a non-zero watermark whether or not the
[lakehouse mirror](../lakehouse/iceberg-mirror.md) is on. A read response
carries the same header for the watermark it reflects. To require a read to
reflect at least a given write (e.g. to read your own write through the
analytical engine), echo it back on the read:

```
X-Bluedb-Min-Watermark: acme:42
```

If the node can't satisfy that freshness (its sealed analytical snapshot is
behind the requested sequence and it is not the active writer), it returns
**`503`** rather than serve stale data. Retry against the writer or after the
next seal. How much staleness a read tolerates before that gate trips is set per
tenant, in **seal cycles**, with a PRAGMA over [`/sql`](#post-sql-transactional-reads-writes-read-your-writes)
(default `1`):

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "PRAGMA bluedb_read_wait_seal_n = 2"}'
```

The active writer serves reads from its own live state, so a read routed to it
reflects every write it has acknowledged. See
[Consistency model](../guarantees/consistency.md#read-consistency).

## Errors

Error responses are JSON with a human `error` message and, when the failure is
classified, a stable machine-readable `code`. Branch on the `code`, not the
prose:

```json
{ "error": "table 'usrs' not found", "code": "NOT_FOUND" }
```

| `code` | HTTP | Meaning |
|--------|------|---------|
| `NOT_FOUND` | 404 | No such table |
| `UNIQUE_VIOLATION` | 409 | Duplicate primary key / unique value |
| `PARSE_ERROR` | 400 | SQL did not parse / translate |
| `TYPE_MISMATCH` | 400 | A value didn't match the column type |
| `NO_INDEX` | 400 | A `/sql` read needs a scan; add an index or run it on `/query` |
| `UNSUPPORTED_STATEMENT` | 400 | A statement the endpoint doesn't accept (e.g. a non-`SELECT` on `/query`) |

A write sent to a passive (non-writer) node returns `503`; re-resolve the active
writer (see [Administration](../operations/admin.md)).

## Collections: document API

bluedb exposes a MongoDB-style document API at `/collections/{collection}/{verb}`. It speaks HTTP/JSON (not the MongoDB wire protocol), so any HTTP client works. Supported verbs:

`insert` · `find` · `update` · `delete` · `aggregate` · `count` · `createIndex`

Tenant selection and bearer scopes are the same as the rest of the API: pass `X-Bluedb-Tenant` to target a tenant (default `_`), and use `data:read` / `data:write` / `schema:admin` scopes as appropriate. See the full reference at [Collections](../collections/README.md).

### Collections: search endpoints

An Elasticsearch-shaped search surface is available on the same path prefix:

| Method | Path | Scope | Description |
|--------|------|-------|-------------|
| `POST` | `/collections/{c}/searchIndex` | `schema:admin` | Declare or replace a search mapping; backfills existing documents |
| `GET` | `/collections/{c}/searchIndex` | `data:read` | Describe the current search mapping for a collection |
| `POST` | `/collections/{c}/search` | `data:read` | Run a BM25 search with the Elasticsearch query DSL; returns the ES hits envelope |

See the full reference at [Collections search](../collections/search.md).

## Ledger

`/ledger/accounts` and `/ledger/transfers` (batched create + lookup) expose the
[double-entry ledger](ledger.md).

## Iceberg REST catalog

`GET /catalog/v1/*` is a read-only [Iceberg REST Catalog](../lakehouse/iceberg-mirror.md)
warehouses use to discover and load the mirrored tables:

- `GET /catalog/v1/config`
- `GET /catalog/v1/namespaces` · `GET /catalog/v1/namespaces/{ns}`
- `GET /catalog/v1/namespaces/{ns}/tables`: list mirrored tables
- `GET /catalog/v1/namespaces/{ns}/tables/{table}`: `loadTable` (metadata location + schema)

Requires `data:read` when authorization is enabled. Each **tenant** publishes its
own namespace (`namespace == tenant`; the default tenant maps to `default`), and a
token only sees the namespaces for tenants it is bound to. Control which tables are
mirrored with `PRAGMA lakehouse_mirror` over [`POST /sql`](#post-sql-transactional-reads-writes-read-your-writes)
(per tenant, selected by the `X-Bluedb-Tenant` header).

## Health & admin

`GET /health`, `GET /admin/status`, `POST /admin/promote`, `POST /admin/demote`:
see [Administration](../operations/admin.md).
