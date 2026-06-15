# bluedb SQL

bluedb speaks SQL over object storage. This section documents the SQL dialect
bluedb accepts — which statements, clauses, types, and functions are supported,
where the behavior matches PostgreSQL/DuckDB, and where it differs.

## What powers it

bluedb's SQL is an embedded engine ([GlueSQL]) extended by a bluedb
**compatibility layer** — a set of query rewrites and planner passes that widen
what you can write (parameterized types, `JOIN … USING`, `TRY_CAST`,
`NULLS FIRST/LAST`, set operations, CTEs, views, …) and tighten correctness
(predicate pushdown to hash joins, secondary-index selection, numeric/text
comparison coercion). Everything runs directly on the same object-storage
substrate as the rest of bluedb — no separate database server.

[GlueSQL]: https://gluesql.org

## Dialect at a glance

- Core **SQL-92**: `CREATE TABLE` / `INSERT` / `UPDATE` / `DELETE` / `SELECT`
  with `WHERE`, `GROUP BY` / `HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`,
  `DISTINCT`, joins, subqueries, aggregates.
- **Schema'd tables** — every table has a typed column list and a `PRIMARY KEY`;
  schema evolution (ADD/DROP/RENAME column, RENAME TABLE) is **online** (O(1)
  metadata, no row rewrite). There are no schemaless tables.
- **Bounded reads** — a plan-time [query guardrail](query-guardrail.md) keeps
  every read served by the primary key or an index (an unfiltered `SELECT` is
  capped to the first 100 rows in PK order; a non-indexed filter/sort is
  rejected with the exact `CREATE INDEX` to run).
- **Transactions** with snapshot isolation (`BEGIN` / `COMMIT` / `ROLLBACK`).
- **Secondary indexes**, **views**, **non-recursive CTEs**, **set operations**.
- Closest in feel to **PostgreSQL**; this guide calls out every place the
  behavior diverges from PostgreSQL or DuckDB.

## Pages

| Page | Covers |
|------|--------|
| [Statements](statements.md) | `CREATE`/`DROP`/`ALTER TABLE`·`INDEX`·`VIEW`, `INSERT`/`UPDATE`/`DELETE`, `SET` |
| [Query guardrail](query-guardrail.md) | Why reads must be index-served, the bare-scan cap, and the index a rejected query asks for |
| [Query syntax](query-syntax.md) | `SELECT`, `FROM`/joins, `WHERE`, `GROUP BY`, `ORDER BY`, set ops, CTEs, subqueries |
| [Data types](data-types.md) | Native types and accepted aliases (`VARCHAR(n)`, `DOUBLE`, …) |
| [Expressions](expressions.md) | Operators, comparisons & coercion, `CAST`/`TRY_CAST`, `CASE`, `IN`, `BETWEEN` |
| [Functions](functions.md) | Scalar (math/string/date/…) and aggregate functions |
| [Transactions](transactions.md) | `BEGIN`/`COMMIT`/`ROLLBACK`, isolation, concurrency |
| [Metadata](metadata.md) | Introspection tables: `GLUE_OBJECTS`, `GLUE_TABLES`, … |
| [Limitations & differences](limitations.md) | What's **not** supported and where semantics differ |

## Conventions in these docs

SQL appears in code blocks; results are shown as tables.

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO t VALUES (1, 'ada'), (2, 'lin');
SELECT name FROM t ORDER BY id;
```

| name |
|------|
| ada  |
| lin  |

!!! note
    Callouts like this clarify behavior.

!!! warning
    Callouts like this flag a sharp edge or a difference from PostgreSQL/DuckDB.
    The [Limitations & differences](limitations.md) page collects them all.
