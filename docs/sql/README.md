# bluedb SQL

bluedb speaks SQL over object storage. This section documents the SQL dialect
bluedb accepts — which statements, clauses, types, and functions are supported,
where the behavior matches PostgreSQL/DuckDB, and where it differs.

## What powers it

bluedb speaks SQL directly over its object-storage substrate — no separate
database server. Point and range access by primary key or secondary index is
served from the transactional store; analytical reads — joins, `GROUP BY` /
aggregates, window functions, subqueries, CTEs, set operations, and arbitrary
filters and sorts — are served by a columnar engine over the same data. You
write one SQL surface, and bluedb routes each query to the right path.

## Dialect at a glance

- Core **SQL-92** plus analytics: `SELECT` with `WHERE`, `GROUP BY` / `HAVING`,
  `ORDER BY`, `LIMIT` / `OFFSET`, `DISTINCT`, joins, subqueries, aggregates, and
  **window functions**; `CREATE TABLE` / `INSERT` / `UPDATE` / `DELETE`.
- **Schema'd tables** — every table has a typed column list and a `PRIMARY KEY`;
  schema evolution (ADD/DROP/RENAME column, RENAME TABLE) is **online** (O(1)
  metadata, no row rewrite). There are no schemaless tables.
- **Full read surface** — `SELECT` runs any filter, sort, join, aggregate, or
  window function; the primary key and [secondary indexes](query-guardrail.md)
  accelerate point and range lookups.
- **Transactions** with snapshot isolation (`BEGIN` / `COMMIT` / `ROLLBACK`).
- **Secondary indexes**, **views**, **non-recursive CTEs**, **set operations**.
- Closest in feel to **PostgreSQL**; this guide calls out every place the
  behavior diverges from PostgreSQL or DuckDB.

## Pages

| Page | Covers |
|------|--------|
| [Statements](statements.md) | `CREATE`/`DROP`/`ALTER TABLE`·`INDEX`·`VIEW`, `INSERT`/`UPDATE`/`DELETE`, `SET` |
| [Reads and indexes](query-guardrail.md) | How `SELECT` is served, and how the primary key and secondary indexes accelerate point/range lookups |
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
