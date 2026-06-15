# Query guardrail

[← SQL index](README.md)

bluedb is an **indexed point/range OLTP store**. Every read is served by the
primary key or a secondary index, so a query can never quietly turn into a
full-table scan or an in-memory sort that stalls the single writer. Whole-table
analytics belongs in the warehouse, reached through the Iceberg mirror — not on
the hot path.

A plan-time **guardrail** enforces this on every user connection — `/sql`,
`GET /tables/{table}`, **and** `/admin/sql`. It is **not overridable**: there is
no flag, scope, or `PRAGMA` to switch it off, so no client can trip an unbounded
scan by accident or by injection.

## The rules

| Query shape | Result |
|-------------|--------|
| `WHERE` on the primary key or an indexed column | ✅ allowed, **any size** |
| No `WHERE` (a bare `SELECT * FROM t`) | ✅ auto-bounded to the first **100 rows** in primary-key order |
| `WHERE` on only non-indexed columns | ❌ rejected — a `LIMIT` can't bound a post-scan filter |
| `ORDER BY` a non-indexed column | ❌ rejected — that is an in-memory sort |

Index/PK-served range scans and PK/index-bounded aggregation stay allowed at any
size — you bound them yourself with the predicate.

## A bare scan is bounded, not blocked

A `SELECT` with no `WHERE` is **not** rejected — it is capped to the first **100
rows in primary-key order**. An explicit `LIMIT` above 100 is clamped down; a
smaller one is kept:

```sql
SELECT * FROM users;                          -- first 100 rows by id
SELECT * FROM users LIMIT 500;                -- still 100 (clamped)
SELECT * FROM users LIMIT 10;                 -- 10 rows
```

To read past the first page, walk the primary key — a PK predicate is
index-served and **uncapped**:

```sql
SELECT * FROM users WHERE id > 100 LIMIT 100; -- the next page
```

## Filters and sorts need an index

A predicate or `ORDER BY` on a column that is neither the primary key nor indexed
is rejected — and the error hands you the exact index to create (Firestore-style):

```sql
SELECT * FROM users WHERE email = 'ada@x.io';
```

> rejected: query on `users` filters only non-indexed column(s) `email` — this
> would scan the whole table (a LIMIT can't bound a post-scan filter). Create an
> index and retry, e.g.
> &nbsp;&nbsp;&nbsp;&nbsp;`CREATE INDEX users_email ON users (email);`
> then filter on that column (one index is enough). Or filter on the primary key,
> or run the scan in the warehouse via the Iceberg mirror.

Create the index and the same query is served from it:

```sql
CREATE INDEX users_email ON users (email);
SELECT * FROM users WHERE email = 'ada@x.io';   -- index-served
```

`ORDER BY` works the same way — sort on the primary key or an indexed column:

```sql
SELECT * FROM users WHERE id > 0 ORDER BY name; -- rejected: name not indexed
CREATE INDEX users_name ON users (name);
SELECT * FROM users WHERE id > 0 ORDER BY name; -- now an ordered index scan
```

!!! note
    A `WHERE … AND …` only needs **one** indexed, sargable conjunct — the index
    narrows the scan and the engine filters the rest. `OR` across a non-indexed
    column is rejected (it can't be narrowed by a single index).

!!! warning
    `col LIKE '%infix%'` is **not** served by a plain index — accelerate it with
    a **trigram index** (see [Full-text search](full-text-search.md)). An
    unindexed `LIKE` is a full scan and is rejected.

## Why there is no override

The guardrail protects the single writer's latency: one accidental full scan over
a large table — a forgotten `WHERE`, an unindexed `ORDER BY`, a `LIKE '%x%'`, or a
SQL-injection probe — would stall every other request. Making it bypassable would
defeat the protection, so it binds everyone, `/admin/sql` included. When you
genuinely need to read or aggregate a whole table, do it in the warehouse via the
Iceberg mirror, where columnar scans belong.

## Engine internals are exempt

bluedb's own maintenance work — dropping a table, full-text index seal/merge, the
ledger's SQL projection, `GLUE_OBJECTS` introspection — runs **below** the SQL
surface (directly on the storage traits, not `Glue::execute`), so it scans as
needed without tripping the guardrail. The guardrail governs *user queries*, not
the engine.

## See also

- [Statements](statements.md) — every table needs a schema and a `PRIMARY KEY`
  (there are no schemaless tables), and how online `ALTER TABLE` evolves one.
- [Full-text search](full-text-search.md) — `@@` and trigram-accelerated `LIKE`.
- [Limitations & differences](limitations.md) — the full compatibility contract.
