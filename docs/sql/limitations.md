# Limitations & differences

[← SQL index](README.md)

This page is the compatibility contract: what bluedb **does not** support, and
where it **runs but behaves differently** from PostgreSQL/DuckDB. If a feature
isn't listed as unsupported here or omitted from the other pages, assume it
works as in standard SQL.

`SELECT` runs the full analytical surface — joins, `GROUP BY` / aggregates,
window functions, subqueries, CTEs, set operations, and arbitrary `WHERE` /
`ORDER BY` on any column. See [Reads and indexes](query-guardrail.md).

## Not supported — these error (safe to detect)

| Feature | Notes |
|---------|-------|
| **Schemaless / PK-less tables** | Every table needs a typed column list **and** a `PRIMARY KEY`; a column-less or key-less `CREATE TABLE` is rejected. |
| **`WITH RECURSIVE`** | Recursive CTEs need iterative evaluation. (Non-recursive `WITH` is supported.) |
| **Composite (multi-column) indexes** | Secondary indexes are single-column. (A composite **`PRIMARY KEY`** is supported.) |
| **`UPDATE` of a primary-key column** | Changes a row's identity — delete and re-insert instead. |
| **User-defined functions** | `CREATE FUNCTION` and custom aggregates. |
| **Exotic types** | `BIT`, `STRUCT`, enums (`CREATE TYPE`). See [Data types](data-types.md). |
| **Large multi-row `INSERT … VALUES (…),(…),…`** | Parser limit. Use single-row inserts or one `BEGIN … COMMIT` batch. |
| **JSON key-existence `?` / `?|` / `?&`** | The dialect reserves `?` for parameters, so these can't parse. Use `@>` or `(col ->> 'k') IS NOT NULL`. See [JSON](json.md). |

The function library is its own set — broad coverage of the common string /
numeric / date-time / aggregate functions, but not every PostgreSQL- or
DuckDB-specific builtin. See [Functions](functions.md).

## Semantic differences — these run, but differ

!!! warning
    These execute without error but may behave differently from
    PostgreSQL/DuckDB. Know them.

- **Result order without `ORDER BY` is unspecified.** Add an explicit `ORDER BY`
  whenever order matters; don't rely on insertion or primary-key order.

- **`NULL` ordering and aggregate numeric types follow the engine's defaults.**
  Use `ORDER BY … NULLS FIRST/LAST` for explicit null placement, and `CAST` for
  an exact aggregate result type, when you need portable behavior.

- **Single-row insert throughput.** Each autocommit `INSERT` is one durable
  object-storage write; batch large loads in a transaction. See
  [Transactions](transactions.md).

- **`jsonb_path_query` is scalar over a navigation subset.** It returns the
  **first** match (PostgreSQL returns one row per match — use
  `jsonb_path_query_array` for all of them), supports only navigation paths
  (`$`, `.key`, `[n]`, `.*`, `[*]`), and **errors** (rather than returning `NULL`)
  on a path it can't evaluate — filters, methods, ranges, or variables. See
  [JSON](json.md).

## How this is verified

bluedb's SQL is checked along two axes, so this contract stays honest:

1. **SQL-surface conformance** — a query corpus runs statements and queries
   through the engine and compares results.
2. **Storage-contract conformance** — a CRUD / transaction / index / alter-table
   / metadata suite runs against bluedb's storage backend.

When the engine or the compatibility layer changes, both suites re-run and this
page is updated.
