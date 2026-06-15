# Limitations & differences

[← SQL index](README.md)

This page is the compatibility contract: what bluedb **does not** support, and
where it **runs but behaves differently** from PostgreSQL/DuckDB. If a feature
isn't listed as unsupported here or omitted from the other pages, assume it
works as in standard SQL.

## Not supported — these error (safe to detect)

| Feature | Notes |
|---------|-------|
| **Window functions** (`… OVER (…)`) | `ROW_NUMBER`, `RANK`, running `SUM`, etc. Rejected with a clear error (the engine has no windowing). |
| **`WITH RECURSIVE`** | Recursive CTEs need iterative evaluation. (Non-recursive `WITH` is supported.) |
| **Cartesian products** | `CROSS JOIN`, or a comma join with no equi-join key — rejected to avoid materializing the full product. |
| **Unindexed scans & sorts** | A `WHERE` or `ORDER BY` on a column that is neither the primary key nor indexed is rejected by the [query guardrail](query-guardrail.md) (it would be a full scan / in-memory sort). The error names the exact `CREATE INDEX` to add. A bare `SELECT` (no `WHERE`) is allowed but capped to the first 100 rows in primary-key order. |
| **Schemaless / PK-less tables** | Every table needs a typed column list **and** a `PRIMARY KEY`; a column-less or key-less `CREATE TABLE` is rejected. |
| **Multi-column `INTERSECT` / `EXCEPT`** | Single-column only. (Multi-column `UNION` / `UNION ALL` *is* supported.) |
| **Composite indexes** | Indexes are single-column. |
| **`SELECT DISTINCT ON (…)`** | Not supported. |
| **`EXPLAIN` / `EXPLAIN ANALYZE`** | No query-plan output. |
| **User-defined functions** | `CREATE FUNCTION` and custom aggregates. |
| **Engine-specific builtins** | Many DuckDB/PostgreSQL-only functions (`arg_min`, `list_*`, `regexp_*`, `any_value`, …). |
| **Exotic types** | `BIT`, `STRUCT`, enums (`CREATE TYPE`). See [Data types](data-types.md). |
| **Large multi-row `INSERT … VALUES (…),(…),…`** | Parser limit. Use single-row inserts or one `BEGIN … COMMIT` batch. |

## Semantic differences — these run, but differ

!!! warning
    These execute without error but behave differently from
    PostgreSQL/DuckDB. Know them.

- **`bool`/`int` comparisons are not coerced.** `TRUE = 1` is **FALSE**. Engines
  disagree (DuckDB/MySQL: true; PostgreSQL: error), so bluedb leaves it — use an
  explicit `CAST`. *(Text↔number comparisons such as `qty = '5'` and `'1' = 1`
  **are** coerced numerically — see [Expressions](expressions.md).)*

- **Aggregate result types differ.** `SUM`/`AVG` over integers may return an
  integer where PostgreSQL/DuckDB return a decimal/float.

- **Aggregate over an empty match returns no row.** A no-`GROUP BY` aggregate
  whose `WHERE` matches nothing yields **0 rows**, where SQL expects one row
  (`COUNT → 0`, others `→ NULL`).

- **`AVG`/`STDEV`/`VARIANCE` over `INTERVAL`** is incorrect (no interval
  division).

- **Default row order.** Without `ORDER BY`, rows come back in storage
  (primary-key) order, not insertion order.

- **Default NULL ordering.** Without `SET default_null_order`, `NULL` sorts as
  the largest value (last ascending, first descending). Set it, or use
  `NULLS FIRST/LAST`, to choose. See [Query syntax](query-syntax.md#order-by).

- **Set-op / comma-join order.** The correct rows come back, but not necessarily
  in source order — add `ORDER BY` if order matters.

- **Single-row insert throughput.** Each autocommit `INSERT` is one durable
  object-storage write; batch large loads in a transaction. See
  [Transactions](transactions.md).

## How this is verified

bluedb's SQL is checked along two axes, so this contract stays honest:

1. **SQL-surface conformance** — a [sqllogictest] corpus (SQLite + a DuckDB
   `test/sql` subset) runs every statement/query through the engine and compares
   results.
2. **Storage-contract conformance** — GlueSQL's own custom-storage test suite
   (CRUD, transactions, indexes, alter-table, metadata) runs against bluedb's
   storage backend.

[sqllogictest]: https://github.com/risinglightdb/sqllogictest-rs

When the engine or the compatibility layer changes, both suites re-run and this
page is updated.
