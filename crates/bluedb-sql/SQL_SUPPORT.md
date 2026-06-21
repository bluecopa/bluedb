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
> - DuckDB test subset: **34%** execute (lower — the corpus is DuckDB-specific); **of what executes, ~78% returns the correct result.** (Both rose as more
>   statements run — broader type coverage, no-op `SET`/`PRAGMA`, views, `USING`,
>   `TRY_CAST`, `NULLS FIRST/LAST`; the *correct* fraction dips slightly only
>   because the newly-running queries are harder.)
>
> The correctness comparator is layout- and numeric-tolerant (the DuckDB corpus
> mixes tab-separated-row and one-value-per-line result blocks, and `R`-columns
> print `11.000000` for the integer `11`); it still requires exact numeric
> equality, so genuine value differences are not masked.
>
> Reproduce: run `fetch_corpus.sh`, then
> `cargo run -p bluedb-sqltest --bin conformance -- <corpus-dir>`.
>
> **Storage-contract conformance.** Separately, GlueSQL's own custom-storage
> test suite runs against `SlateDbStorage` (`crates/bluedb-sql/tests/gluesql_suite.rs`):
> **205/205** pass across store / alter-table / index / transaction / metadata.
> This validates the trait impls *we* wrote (CRUD, snapshot-isolation rollback,
> secondary-index scans, `GLUE_OBJECTS`) — the axis the SQL corpus doesn't cover.
> Run: `cargo test -p bluedb-sql --test gluesql_suite`.

## How a query is processed

```
SQL ──▶ rewrite shim ──▶ GlueSQL translate ──▶ Planner hook ──▶ execute
```

The rewrite shim (in `bluedb-sql`, with session-state pieces — `SET`/view capture
— in the harness) applies, in order:

1. **Session intercepts** — `SET default_null_order` and other `SET`/`PRAGMA` (no-op), `CREATE`/`DROP VIEW` (capture/inline).
2. **CTE inlining** — non-recursive `WITH c AS (q) …` → `… FROM (q) AS c` (incl. under `CREATE TABLE AS` / `INSERT`); view-reference inlining.
3. **Set-op rewrite** — `UNION`/`UNION ALL` (any width) / single-column `INTERSECT`/`EXCEPT` → joins / `IN`-subqueries.
4. **Comma-join fold + `USING`→`ON` + type normalization + `TRY_CAST`→`CAST`** — `FROM a, b` → `a JOIN b ON TRUE`; broad type mapping (text/decimal/integer/float/temporal families).
5. **NULL-order normalization** — explicit `NULLS FIRST/LAST` and the session default → leading `(e IS NULL)` sort keys.

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
- `CREATE INDEX` / `DROP INDEX` (single-column) — and secondary indexes are actually *used* by the planner (`plan_index`), not just built.
- Metadata/introspection tables — `GLUE_OBJECTS` (tables + indexes, with a real per-table `CREATED`), `GLUE_TABLES`, `GLUE_TABLE_COLUMNS`, `GLUE_INDEXES`.
- `CREATE VIEW` / `DROP VIEW` — no engine view support; the definition is captured and inlined as a derived table on reference (one level; non-recursive).
- `SET` / `PRAGMA` — `default_null_order` is honored; other engine-config knobs are accepted as no-ops rather than rejected.
- Transactions — `BEGIN` / `COMMIT` / `ROLLBACK` (snapshot isolation; write transactions serialized — single writer)

**Queries**
- Aggregates — `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`
- Joins — `INNER JOIN` / `LEFT JOIN … ON` (equi-joins run as hash joins); `JOIN … USING (cols)` is rewritten to the equivalent `ON`.
- **Comma-joins** `FROM a, b WHERE a.x = b.y` — **only with an equi-join key** (rewritten to a hash join). See gotchas.
- **Set operations** — `UNION` / `UNION ALL` over **any number of columns**; `INTERSECT` / `EXCEPT` **single-column only** (multi-column needs NULL-aware row matching). Rewritten to joins/subqueries.
- **Non-recursive CTEs** (`WITH c AS (…) SELECT … FROM c`) — inlined as derived tables (also under `CREATE TABLE AS` / `INSERT`).
- **`NULLS FIRST` / `NULLS LAST`** in `ORDER BY` — rewritten to a leading `(e IS NULL)` sort key (GlueSQL lacks the syntax).
- **`TRY_CAST` / `SAFE_CAST`** — rewritten to `CAST` (differs only when the cast would fail: `TRY_CAST` yields NULL, `CAST` errors).
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
- Accepted via normalization:
  - text — `VARCHAR(n)`/`CHAR(n)`/`NVARCHAR`/`CLOB`/`STRING` → `TEXT`
  - decimal — `DECIMAL(p,s)`/`NUMERIC`/`BIGNUM` → `DECIMAL`
  - integer — `BIGINT`/`SMALLINT`/`TINYINT`/`INT(n)`/`INT2/4/8`/`HUGEINT` and the unsigned/wide family (`UHUGEINT`/`UBIGINT`/`UINT*`/`INT128`…) → `INTEGER` (i64; lossy out of range)
  - float — `DOUBLE`/`DOUBLE PRECISION`/`REAL`/`FLOAT(n)`/`FLOAT4/8` → `FLOAT`
  - temporal — `TIMESTAMP(n)`/`TIMESTAMPTZ`/`DATETIME` → `TIMESTAMP`, `TIME(n)` → `TIME` (sub-second precision / timezone dropped)

## ❌ Not supported (the query **errors** — safe to detect)

- **Window functions (`OVER`)** — `SUM(x) OVER (…)`, `ROW_NUMBER() OVER (…)`,
  `RANK()`, etc. GlueSQL has no windowing and would *silently mis-execute* them,
  so they are **detected before execution and rejected** with a clear
  "unsupported" error (a pre-execution check walks projection, `WHERE`,
  `HAVING`, and `FROM`-derived subqueries for an `OVER` clause).
- **Cartesian products** — `CROSS JOIN`, or comma-joins without a join key. Rejected at plan time (would materialize the full product).
- **`WITH RECURSIVE`** — needs iterative evaluation and can't be inlined into the transactional `/sql` path. It **is** supported on the analytical `/query` surface (DataFusion's `enable_recursive_ctes` flag is on there) — see `docs/sql/query-syntax.md`.
- **Multi-column `INTERSECT` / `EXCEPT`** (multi-column `UNION`/`UNION ALL` *is* supported).
- **Composite (multi-column) indexes.**
- **`EXPLAIN`, `SELECT DISTINCT ON`.**
- **Engine-specific functions** not in GlueSQL — many DuckDB/Postgres builtins (`arg_min`, `list`, `any_value`, `regexp_*`, …).
- **Large multi-row `INSERT … VALUES (…),(…),…`** (parser limit). Use single-row inserts, or wrap a batch in one `BEGIN … COMMIT`.
- **Exotic types** — `BIT`, `STRUCT`, enums (`CREATE TYPE`).

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
