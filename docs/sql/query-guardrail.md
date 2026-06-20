# Reads and indexes

[← SQL index](README.md)

bluedb has **two read tiers**, exposed as two SQL surfaces with different
performance contracts. Knowing which one a read lands on is the key to
predictable latency.

## The two read tiers

| Surface | What it serves | Latency | Read-your-writes? |
|---------|----------------|---------|-------------------|
| **`POST /sql`** (transactional / RYW) | Point and range lookups on the primary key, and equality/range on a secondary index. A full-text `@@` read (rewritten to `pk IN (...)`). | **Lookup** — O(log n + k) on the row store, fresh from SlateDB | **Yes, immediate** (the writer's live memtable) |
| **`POST /query`** (analytical / HTAP) | The full analytical surface: arbitrary `WHERE` filters, `ORDER BY` on any column, joins, `GROUP BY` / aggregates, window functions, CTEs, set operations, JSON paths. | **Scan** — columnar read of the Iceberg mirror ∪ the unsealed CDC tail | Yes on the writer (unsealed-tail union); bounded-stale on a replica |

The distinction is **performance**, not capability. `/sql` rejects anything that
would require a scan with `400 NO_INDEX` and an error that names the exact index
to create. `/query` never rejects a read on indexing grounds — it scans.

### Which endpoint when?

| You want to… | Use |
|--------------|-----|
| Fetch a row (or page) by primary key | `/sql` |
| Fetch by a secondary-indexed column (`WHERE email = …`) | `/sql` |
| Read your own write back at lookup latency | `/sql` (on the writer) |
| Run a one-statement `INSERT`/`UPDATE`/`DELETE` (with optional `RETURNING`) | `/sql` |
| Filter or sort on a non-indexed column | `/query` (or `GET /tables`, which auto-routes) |
| Join, aggregate, window, recursive CTE | `/query` |
| Filter/project a JSON path (`data->>'status'`) | `/query` |
| Full-text `@@` search | `/sql` (single-table) |

`GET /tables/{table}` (the PostgREST data plane) is a third surface that mirrors
the split: a point/range filter on the primary key or an indexed column is served
from the transactional fast path; anything else (or a JSON-path filter) is
**auto-routed to the analytical engine**. See [REST API](../api/rest.md).

## Point and range lookups (`/sql`)

A predicate on the primary key (or a composite-key prefix, range, or row-value
keyset) is a direct key lookup, the fastest path and always consistent with your
latest write:

```bash
# POST /sql
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT * FROM users WHERE id = 42"}'
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT * FROM users WHERE id > 100 ORDER BY id LIMIT 100"}'   # next page
```

A secondary index accelerates equality and range lookups on a non-key column:

```sql
CREATE INDEX users_email ON users (email);
-- POST /sql
SELECT * FROM users WHERE email = 'ada@x.io';   -- index-served
```

## Analytical reads (`/query`)

Anything that is **not** a PK/index lookup belongs on `/query`: a filter or sort
on a non-indexed column, a join, an aggregate, a window function, a JSON path.
No index is required to make it *run*:

```bash
# POST /query
curl -s localhost:8081/query -H 'content-type: application/json' \
  -d '{"sql": "SELECT category, COUNT(*) FROM products GROUP BY category"}'
```

```sql
-- any of these run on /query, no index needed
SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
    WHERE c.region = 'EU' ORDER BY o.total DESC;
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS rn FROM events;
SELECT * FROM users WHERE name LIKE '%ada%' ORDER BY name;
SELECT * FROM events WHERE (attrs ->> 'status') = 'active';   -- JSON, see json.md
```

!!! note "The same statement, two outcomes"
    `SELECT * FROM t WHERE score > 50` on `/sql` returns **`400 NO_INDEX`** if
    `score` is not indexed; the identical statement on `/query` runs as a scan
    and returns rows. That is the tier split working as designed: `/sql` is your
    low-latency OLTP contract, `/query` is your analytical escape hatch.

### What `/sql` accepts and rejects

`/sql` runs every read through a plan-time **scan/sort guardrail** (there is no
client, PRAGMA, or scope bypass — nobody triggers an unbounded scan by accident):

- **`WHERE` on the PK or an indexed column** → allowed, any size (you bound it).
- **No `WHERE` at all** → auto-bounded to the first 100 rows in PK order (a safe
  browse), not rejected.
- **`WHERE` on only non-indexed columns** → **rejected** (`400 NO_INDEX`). A
  `LIMIT` would not bound it (the filter runs *after* the scan), so it is
  rejected with an error naming the column and the exact `CREATE INDEX` DDL.
- **`ORDER BY` on a non-indexed column** → **rejected** (an in-memory sort a
  `LIMIT` would not make cheap). Order by the primary key, an indexed column, or
  run the sort on `/query`.

The reject error is actionable — it hands back the index you would need:

```json
{
  "code": "NO_INDEX",
  "error": "… filters only non-indexed column(s) `score` — Create an index and retry, e.g.\n    CREATE INDEX t_score ON t (score);\nthen filter on that column (one index is enough). Or run the scan in the warehouse via the Iceberg mirror."
}
```

So: **add an index to bring a hot predicate back onto `/sql`** (lookup latency),
or **send the read to `/query`** (scan latency, no index needed). Both are
correct; they trade latency for zero-index flexibility.

## Substring search

`col LIKE '%infix%'` is a scan, so it belongs on `/query`. A **trigram index**
accelerates it without changing the result, and once declared the qualifying
`LIKE` is served on `/sql` (lookup-fast). See [Full-text search](full-text-search.md).

## See also

- [REST API](../api/rest.md): `/sql`, `/query`, and the `/tables` data plane.
- [Statements](statements.md): every table needs a schema and a `PRIMARY KEY`.
- [Full-text search](full-text-search.md): `@@` and trigram-accelerated `LIKE`.
- [Limitations & differences](limitations.md): the compatibility contract.
