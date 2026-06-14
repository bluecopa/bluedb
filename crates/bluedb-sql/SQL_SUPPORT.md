# bluedb-sql — SQL support

`bluedb-sql` runs the **GlueSQL** engine over SlateDB, with a thin
**SQL-compatibility rewrite layer** in front that extends what GlueSQL accepts
(comma-joins, set operations, non-recursive CTEs, parameterized types) by
rewriting SQL into forms GlueSQL can execute. This document states exactly what
is supported, what is not, and where bluedb's SQL **semantics differ** from
PostgreSQL/DuckDB.

> **Evidence (snapshot).** Measured by the conformance harness in
> [`crates/bluedb-sqltest`](../bluedb-sqltest):
> - SQLite `sqllogictest` subset: **77%** of statements/queries execute without error.
> - DuckDB test subset: **27%** execute (lower — the corpus is DuckDB-specific); **of what executes, ~76% returns the correct result.**
>
> Reproduce: run `fetch_corpus.sh`, then
> `cargo run -p bluedb-sqltest --bin conformance -- <corpus-dir>`.

## How a query is processed

```
SQL ──▶ rewrite shim ──▶ GlueSQL translate ──▶ Planner hook ──▶ execute
```

The rewrite shim (in `bluedb-sql`) applies, in order:

1. **CTE inlining** — non-recursive `WITH c AS (q) …` → `… FROM (q) AS c`.
2. **Set-op rewrite** — `UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT` → joins / `IN`-subqueries.
3. **Comma-join fold + type normalization** — `FROM a, b` → `a JOIN b ON TRUE`; `VARCHAR(n)`→`TEXT`, `DECIMAL(p,s)`→`DECIMAL`, `INT(n)`/`BIGINT`/…→`INTEGER`.

Then GlueSQL's **`Planner` hook** pushes equi-join predicates from `WHERE` into
the join `ON` (so GlueSQL builds **hash joins**, not nested-loop cross products),
and a **backstop** rejects any multi-table query left without a join key (an
unbounded cartesian product) at plan time.

## ✅ Supported

**Statements**
- `CREATE TABLE` (typed *and* schemaless), `DROP TABLE`
- `INSERT` (single-row), `UPDATE`, `DELETE`
- `SELECT` — projection, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `LIMIT`/`OFFSET`, `DISTINCT`
- `CREATE INDEX` / `DROP INDEX` (single-column)
- Transactions — `BEGIN` / `COMMIT` / `ROLLBACK` (snapshot isolation; write transactions serialized — single writer)

**Queries**
- Aggregates — `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`
- Joins — `INNER JOIN` / `LEFT JOIN … ON` (equi-joins run as hash joins)
- **Comma-joins** `FROM a, b WHERE a.x = b.y` — **only with an equi-join key** (rewritten to a hash join). See gotchas.
- **Set operations** `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` — **single-column branches only** (rewritten to joins/subqueries).
- **Non-recursive CTEs** (`WITH c AS (…) SELECT … FROM c`) — inlined as derived tables.
- Subqueries — `IN` / `NOT IN`, `EXISTS`, scalar subqueries, derived tables (`FROM (…) AS x`).

**Data types**
- Native: `BOOLEAN`, `INTEGER`, `FLOAT`, `DECIMAL`, `TEXT`, `BYTEA`, `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`, `UUID`
- Accepted via normalization: `VARCHAR(n)`/`CHAR(n)` → `TEXT`; `DECIMAL(p,s)`/`NUMERIC` → `DECIMAL`; `INT(n)`/`BIGINT`/`SMALLINT`/`TINYINT`/`INT2/4/8` → `INTEGER`

## ❌ Not supported (the query **errors** — safe to detect)

- **Window functions (`OVER`)** — `SUM(x) OVER (…)`, `ROW_NUMBER() OVER (…)`,
  `RANK()`, etc. GlueSQL has no windowing and would *silently mis-execute* them,
  so they are **detected before execution and rejected** with a clear
  "unsupported" error (a pre-execution check walks projection, `WHERE`,
  `HAVING`, and `FROM`-derived subqueries for an `OVER` clause).
- **Cartesian products** — `CROSS JOIN`, or comma-joins without a join key. Rejected at plan time (would materialize the full product).
- **`WITH RECURSIVE`** and DML CTEs (`INSERT … WITH …`, `CREATE … AS WITH …`).
- **Multi-column set operations** (`SELECT a, b … UNION SELECT c, d …`).
- **Views** — `CREATE VIEW` / `DROP VIEW`.
- **Composite (multi-column) indexes.**
- **`EXPLAIN`, `SET`, `PRAGMA`, `SELECT DISTINCT ON`.**
- **Engine-specific functions** not in GlueSQL — e.g. `TRY_CAST`, and many DuckDB/Postgres builtins.
- **Large multi-row `INSERT … VALUES (…),(…),…`** (parser limit). Use single-row inserts, or wrap a batch in one `BEGIN … COMMIT`.
- **Arbitrary-precision types** (`HUGEINT`, DuckDB `BIGNUM`).

## ⚠️ Semantic differences & gotchas (runs, but may be **wrong**)

These execute **without error** but behave differently from PostgreSQL/DuckDB —
they will not be flagged at runtime, so know them:

- **No implicit type coercion in comparisons.** `true = 1`, `'1' = 1`,
  `'1' = true` all evaluate to **FALSE** (no auto-cast), unlike DuckDB/MySQL.
  Use an explicit `CAST`.
- **Aggregate result types differ.** `SUM`/`AVG` over integers may return an
  integer where other engines return a decimal/float.
- **Default row order.** Without `ORDER BY`, rows come back in **storage-key
  (primary-key) order**, not insertion order.
- **Set-op / comma-join rewrites are not order-preserving.** The correct *rows*
  are returned, but not necessarily in source order — add `ORDER BY` if order
  matters.
- **Single-row insert throughput.** Each autocommit `INSERT` is one durable
  object-store write; bulk loads should batch into a transaction. (Concurrent
  writers are coalesced via group commit.)

## Maintaining this document

This reflects GlueSQL `0.19.0` + the bluedb rewrite shim. When the shim or the
pinned GlueSQL version changes, re-run the conformance harness and update the
supported/unsupported lists and the headline numbers above.
