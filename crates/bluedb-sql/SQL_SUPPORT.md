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
> - DuckDB test subset: **28%** execute (lower — the corpus is DuckDB-specific); **of what executes, ~80% returns the correct result.**
>
> The correctness comparator is layout- and numeric-tolerant (the DuckDB corpus
> mixes tab-separated-row and one-value-per-line result blocks, and `R`-columns
> print `11.000000` for the integer `11`); it still requires exact numeric
> equality, so genuine value differences are not masked.
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

Then GlueSQL's **`Planner` hook** runs three schema-aware passes on the planned
statement (it has the column types): it **pushes equi-join predicates** from
`WHERE` into the join `ON` (so GlueSQL builds **hash joins**, not nested-loop
cross products); it **rejects** any multi-table query left without a join key (an
unbounded cartesian product); and it **inserts implicit `CAST`s** so a comparison
between a numeric operand and a numeric string literal compares *numerically*
(see [Supported](#-supported)).

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
- **`SET default_null_order`** (`'nulls_first'` / `'nulls_last'`) — a session
  setting GlueSQL lacks; honored by rewriting each `ORDER BY e` to
  `ORDER BY (e IS NULL) [DESC], e`, which GlueSQL sorts natively. (GlueSQL's own
  default is NULLs-largest: last when ascending, first when descending.)
- Subqueries — `IN` / `NOT IN`, `EXISTS`, scalar subqueries, derived tables (`FROM (…) AS x`).
- **Implicit text↔number coercion in comparisons** — `num_col = '5'`, `price < '9.99'`,
  `'1' = 1`. A comparison between a numeric operand and a *numeric string literal*
  is cast to compare numerically (DuckDB/affinity-engine semantics) instead of
  GlueSQL's silent type-segregated mismatch. Conservative: only string **literals**
  are cast (never a stored text column), and only when the literal parses into the
  target type, so an inserted `CAST` never fails. `bool`↔`int` is **not** coerced
  (see gotchas).

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

- **`bool`↔`int` comparisons are not coerced.** `true = 1` / `flag = 1` evaluate
  to **FALSE** (no auto-cast). This is intentionally left alone — engines disagree
  (DuckDB/MySQL say true, PostgreSQL errors), so there is no safe universal answer.
  Use an explicit `CAST`. *(Text↔number comparisons such as `num_col = '5'` and
  `'1' = 1` **are** coerced — see Supported.)*
- **A number compared to a non-numeric text column is not coerced.** `name = 5`
  where `name` is `TEXT` stays a no-match rather than casting the column to a
  number — we never reinterpret stored text. Comparing a number to a text string
  literal that *isn't* numeric (`id = 'abc'`) is likewise left alone.
- **Aggregate result types differ.** `SUM`/`AVG` over integers may return an
  integer where other engines return a decimal/float.
- **Aggregate over an empty match returns no row.** A no-`GROUP BY` aggregate
  whose `WHERE` matches nothing yields **0 rows**, where SQL wants **1** row
  (`COUNT → 0`, others `→ NULL`). This is GlueSQL executor behavior; a plan-time
  rewrite can synthesize the row for most aggregates but not `COUNT(*)`, so it's
  left to a future executor fix.
- **`AVG`/`STDDEV`/`VARIANCE` over `INTERVAL`** is incorrect — GlueSQL has no
  interval division, so the float-cast/divide path produces a wrong value.
- **Default row order.** Without `ORDER BY`, rows come back in **storage-key
  (primary-key) order**, not insertion order.
- **Default NULL ordering.** With `ORDER BY` but no `SET default_null_order`,
  NULLs sort as the **largest** value (last ascending, first descending). Set
  `default_null_order` to choose explicitly.
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
