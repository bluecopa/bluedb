# Full-text search

bluedb's full-text search is **SQL-integrated**: you declare a full-text index on
a text column, then search it through ordinary SQL: a relevance predicate in
`WHERE`, combinable with normal structured filters, `ORDER BY`, and pagination.
There is **no separate search API and no search cluster**. The index (a
BM25/tantivy index) lives in the same object-storage substrate as the table, so
there is no ETL or sync between a database and a search engine.

The surface mirrors **PostgreSQL FTS**: `to_tsvector(cfg, col) @@
*_tsquery(q)`, with `ts_rank(...)` for ranking, so it is familiar and portable.

## 1. Declare a full-text index

Create the index over a text column with the
[`/schema/*` DDL endpoint](../api/rest.md#declare-a-full-text-or-trigram-index).
The table's integer primary key is resolved automatically; you name only the
column and (optionally) the analyzer.

```bash
curl -s -X POST localhost:8081/schema/tables/docs/fulltext-indexes \
  -H 'content-type: application/json' \
  -d '{"column": "body", "analyzer": "english"}'
```

The `analyzer` defaults to `"english"` (stemming). Other analyzers supported by
the engine are `keyword` and `whitespace`.

## 2. Search through `/sql`

A full-text query is a `to_tsvector(cfg, col) @@ *_tsquery(q)` predicate over
[`POST /sql`](../api/rest.md#post-sql-transactional-reads-writes-read-your-writes)
(the transactional surface — the `@@` is rewritten to a `pk IN (...)` point
lookup, served at lookup latency and read-your-writes):

```sql
SELECT id, title
FROM docs
WHERE to_tsvector('english', body) @@ plainto_tsquery($1)
```

```bash
curl -s localhost:8081/sql -H 'content-type: application/json' \
  -d '{"sql": "SELECT id, title FROM docs WHERE to_tsvector('"'"'english'"'"', body) @@ plainto_tsquery($1)", "params": ["invoice overdue"]}'
```

The `cfg` argument to `to_tsvector` (`'english'`) selects the analyzer and **must
match** the analyzer the index was declared with. The left-hand `col` must be the
indexed column. Behind the scenes the engine searches the index, rewrites the
`@@` predicate to a `pk IN (…)` over the matching rows, and lets the SQL engine
run the rest of the query, so everything composes with normal SQL.

!!! note "Single table, no joins"
    An `@@` predicate is supported on a single-table `SELECT` only; a query that
    joins tables and uses `@@` is rejected. Run the search on the base table, then
    join its results if needed.

## 3. tsquery variants

The right-hand side selects how the query string is parsed, exactly as in
PostgreSQL:

| Function | Query parsing |
|----------|---------------|
| `plainto_tsquery(q)` | Plain text; terms AND-ed together |
| `to_tsquery(q)` | Boolean operators (`&`, `|`, `!`) in the query string |
| `websearch_to_tsquery(q)` | Web-search syntax (quoted phrases, `or`, `-term`) |

```sql
-- all of "invoice" AND "overdue"
WHERE to_tsvector('english', body) @@ plainto_tsquery($1)        -- 'invoice overdue'

-- boolean: "invoice" AND NOT "paid"
WHERE to_tsvector('english', body) @@ to_tsquery($1)             -- 'invoice & !paid'

-- web search: a phrase and an exclusion
WHERE to_tsvector('english', body) @@ websearch_to_tsquery($1)   -- '"past due" -draft'
```

## 4. Combine with filters, ranking, and pagination

Because the `@@` predicate lowers to an ordinary `pk IN (…)`, you combine it
freely with structured filters (which can ride a B-tree
[secondary index](statements.md)), `ORDER BY ts_rank(...)`, and `LIMIT`/`OFFSET`:

```sql
SELECT id, title
FROM docs
WHERE to_tsvector('english', body) @@ plainto_tsquery($1)
  AND status = 'open'
ORDER BY ts_rank(to_tsvector('english', body), plainto_tsquery($1)) DESC
LIMIT 20;
```

`ts_rank(to_tsvector(cfg, col), *_tsquery(q))` in an `ORDER BY` resolves to the
BM25 relevance order from the index (`DESC` gives best-match-first). The `AND
status = 'open'` filter and `LIMIT`/`OFFSET` are applied by the SQL engine over
the candidate rows.

## 5. Read-your-writes

The index is maintained on **every committed write** through an in-process commit
tap that feeds an in-memory **live segment**. So after an `INSERT`/`UPDATE`/
`DELETE` commits, a subsequent `@@` query **sees the change immediately**, with
no explicit flush or reindex step. Updates and deletes are reflected through
tombstones, so a row that no longer matches drops out of results right away.

A background **seal** (on the `BLUEDB_FTS_SEAL_INTERVAL_MS` interval, default
30000 ms) folds the in-memory live segment into a durable tantivy split in object
storage and resets the live segment, bounding both memory and failover-replay
time. Sealed splits survive a restart.

!!! note "Active node only"
    Full-text reads run only on the **active writer**, which serves all client
    traffic in active-passive HA, so read-your-writes is cluster-wide and
    transparent. A demoted node serves no FTS. On promotion the new active reopens
    its durable FTS engine and replays any un-sealed writes from the SQL watermark
    before serving fresh `@@` queries, so no matches are lost across failover
    (SQL is the source of truth). Cross-region replicas are bounded-staleness and
    out of the read-your-writes scope.

## 6. Trigram-accelerated `LIKE`

A **trigram index** accelerates substring matching (`col LIKE '%lit%'`) without
a full table scan. Declare it the same way as a full-text index (it takes only
the column):

```bash
curl -s -X POST localhost:8081/schema/tables/docs/trigram-indexes \
  -H 'content-type: application/json' \
  -d '{"column": "body"}'
```

With the index in place, a qualifying `LIKE` is rewritten to a `pk IN (…)`
prefilter while the original `LIKE` is kept as the exact verify:

```sql
SELECT id FROM docs WHERE body LIKE '%overdue%';
```

A `LIKE` is accelerated only when its literal is a **clean infix**: at least 3
characters, with no remaining `%`/`_` wildcards inside the core after stripping
one optional leading and one optional trailing `%`. So `'%overdue%'`,
`'%overdue'`, `'overdue%'`, and the bare `'overdue'` all qualify; `'%ab%'`
(too short), `'%ov_rdue%'` (internal wildcard), and `NOT LIKE` do not; those
fall back to a correct (but unindexed) scan.

!!! note "Regex not yet supported"
    Regex matching (the PostgreSQL `~` / `~*` / `!~` operators) is designed but
    **not yet implemented**. Only `@@` full-text and trigram-`LIKE` acceleration
    are available today.
