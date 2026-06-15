# REST API

`bluedb-server` exposes a **PostgREST-style** HTTP API over four capability
surfaces: a data plane (`/tables/{table}`), one parameterized SQL statement
(`/sql`), structured DDL (`/schema/*`), and an off-by-default arbitrary-SQL
admin escape hatch (`/admin/sql`). A [double-entry ledger](ledger.md) is
mounted at `/ledger/*`. The listener speaks HTTP/1.1 and HTTP/2 (h2c,
prior-knowledge), so an h2 client can multiplex many concurrent in-flight
writes over one connection.

All write methods (`POST`/`PATCH`/`DELETE`, `POST /sql`, every `/schema/*`
endpoint, and the ledger create endpoints) are accepted only by the
[active writer](../operations/admin.md); reads (`GET`) are served by any node.

## Authorization

Each request carries a bearer token:

```
Authorization: Bearer <token>
```

Tokens map to **scopes** via the `BLUEDB_AUTHZ_TOKENS` env var
(format `tok1=scope,scope;tok2=scope` — see
[Configuration](../deployment/configuration.md)). When that variable is **unset**
the server runs in **open mode** — every request is allowed without a token.
Production deployments should always set it. The `superuser` scope satisfies any
required scope.

Per-route scopes:

| Route | Required scope |
|-------|----------------|
| `GET /tables/{table}` | `data:read` |
| `POST` / `PATCH` / `DELETE /tables/{table}` | `data:write` |
| `POST /sql` | `data:query` |
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

## `POST /sql` — run one parameterized statement

`/sql` runs a **single, parameterized, non-DDL** statement — one
`SELECT`/`INSERT`/`UPDATE`/`DELETE`. Bind values with `$N` placeholders and a
`params` array (injection-proof by construction); never interpolate values into
the SQL string.

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT name FROM users WHERE age > $1 ORDER BY name", "params": [21]}'
```

It returns a JSON object with the result rows. See the
[SQL reference](../sql/README.md).

!!! note "Full-text search"
    `/sql` also accepts the PostgreSQL full-text surface — `to_tsvector(cfg, col)
    @@ plainto_tsquery($1)`, `ts_rank(...)`, and trigram-accelerated
    `col LIKE '%lit%'` — which is rewritten transparently against the live index.
    See [Full-text search](../sql/full-text-search.md).

!!! warning "DDL goes elsewhere"
    `CREATE`/`DROP`/`ALTER`, transactions, and multi-statement scripts are **not**
    accepted here. Use the structured [`/schema/*`](#schema-ddl-endpoints)
    endpoints, or `/admin/sql` for the raw escape hatch.

## `POST /admin/sql` — arbitrary SQL (off by default)

The raw escape hatch: arbitrary SQL including DDL, transactions, and
multi-statement scripts. It is **disabled by default** and only enabled when
`BLUEDB_ENABLE_ADMIN_SQL=1` (or `true`). Every call is audited, and the route
requires the `superuser` scope.

```bash
curl -s localhost:8081/admin/sql -H 'content-type: application/json' \
  -H 'authorization: Bearer <superuser-token>' \
  -d '{"sql": "BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT;"}'
```

`/admin/sql` does **not** rewrite the `@@` full-text surface — it is left as the
literal raw passthrough.

## `GET /tables/{table}` — select

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

## `POST /tables/{table}` — insert

A JSON object, or an array of objects for a batch:

```bash
curl -s -X POST localhost:8081/tables/users -H 'content-type: application/json' \
  -d '{"id": 1, "name": "ada", "age": 36}'

curl -s -X POST localhost:8081/tables/users -H 'content-type: application/json' \
  -d '[{"id": 2, "name": "lin"}, {"id": 3, "name": "sam"}]'
```

## `PATCH /tables/{table}` — update

Assignments in the body, rows selected by the query-string filter:

```bash
# UPDATE users SET age = 37 WHERE id = 1
curl -s -X PATCH 'localhost:8081/tables/users?id=eq.1' \
  -H 'content-type: application/json' -d '{"age": 37}'
```

## `DELETE /tables/{table}` — delete

```bash
# DELETE FROM users WHERE age < 18
curl -s -X DELETE 'localhost:8081/tables/users?age=lt.18'
```

!!! warning
    A `PATCH`/`DELETE` with no filter affects every row. Always include a
    query-string filter unless you mean it.

## Schema DDL endpoints

DDL is expressed as **typed JSON**, never raw SQL: every identifier is checked
against `^[A-Za-z_][A-Za-z0-9_]*$` and every column type against an explicit
allow-list before the server builds the DDL. All `/schema/*` endpoints require
the `schema:admin` scope and the active writer.

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/schema/tables` | Create a table from a typed column spec |
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
          {"name": "status", "type": "TEXT"}
        ]
      }'
```

Each column takes `name`, `type`, and optionally `primaryKey` (camelCase),
`nullable` (defaults to `true`), and `unique`. Allowed types: `TEXT`,
`INTEGER`/`INT`, `BOOLEAN`/`BOOL`, `FLOAT`, `DECIMAL`, `DATE`, `TIME`,
`TIMESTAMP`, `UUID`.

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

## Ledger

`/ledger/accounts` and `/ledger/transfers` (batched create + lookup) expose the
[double-entry ledger](ledger.md).

## Iceberg REST catalog

`GET /catalog/v1/*` is a read-only [Iceberg REST Catalog](../lakehouse/iceberg-mirror.md)
warehouses use to discover and load the mirrored tables:

- `GET /catalog/v1/config`
- `GET /catalog/v1/namespaces` · `GET /catalog/v1/namespaces/{ns}`
- `GET /catalog/v1/namespaces/{ns}/tables` — list mirrored tables
- `GET /catalog/v1/namespaces/{ns}/tables/{table}` — `loadTable` (metadata location + schema)

Requires `data:read` when authorization is enabled. Each **tenant** publishes its
own namespace (`namespace == tenant`; the default tenant maps to `default`), and a
token only sees the namespaces for tenants it is bound to. Control which tables are
mirrored with `PRAGMA lakehouse_mirror` over [`POST /sql`](#post-sql-run-one-parameterized-statement)
(per tenant, selected by the `X-Bluedb-Tenant` header).

## Health & admin

`GET /health`, `GET /admin/status`, `POST /admin/promote`, `POST /admin/demote` —
see [Administration](../operations/admin.md).
