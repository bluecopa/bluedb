# REST API

`bluedb-server` exposes a **PostgREST-style** HTTP API: tables are addressed at
`/tables/{table}`, filters/ordering/pagination go in the query string, and
bodies are JSON. A raw `/sql` endpoint runs arbitrary SQL for setup and admin.

All write methods (`POST`/`PATCH`/`DELETE`, and `POST /sql`) are accepted only by
the [active writer](../operations/admin.md); reads (`GET`) are served by any
node.

## `POST /sql` — run SQL

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT name FROM users WHERE age > 21 ORDER BY name"}'
```

Runs DDL and queries. A single statement returns a JSON object/rows; a
multi-statement script returns a JSON array of per-statement results. See the
[SQL reference](../sql/README.md).

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

## Health & admin

`GET /health`, `GET /admin/status`, `POST /admin/promote`, `POST /admin/demote` —
see [Administration](../operations/admin.md).
