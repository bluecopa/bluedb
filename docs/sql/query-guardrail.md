# Reads and indexes

[← SQL index](README.md)

`SELECT` runs the full analytical surface — arbitrary `WHERE` filters, `ORDER BY`
on any column, joins, `GROUP BY` / aggregates, window functions, subqueries,
CTEs, and set operations — over any table. You never add an index just to make a
query *run*; indexes and the primary key are a **performance** feature.

| Query shape | How it's served |
|-------------|-----------------|
| `WHERE` on the primary key (point, range, composite prefix/keyset) | direct key lookup — the fastest path |
| `WHERE` / `ORDER BY` on a secondary-indexed column | index-served |
| Filter / sort / aggregate on any other column, joins, windows | analytical scan |

## Point and range lookups

A predicate on the primary key — or a composite-key prefix, range, or row-value
keyset — is a direct key lookup, the fastest path and always consistent with your
latest write:

```sql
SELECT * FROM users WHERE id = 42;
SELECT * FROM users WHERE id > 100 ORDER BY id LIMIT 100;   -- next page
```

A secondary index accelerates equality and range lookups on a non-key column:

```sql
CREATE INDEX users_email ON users (email);
SELECT * FROM users WHERE email = 'ada@x.io';   -- index-served
```

## Analytical reads

Anything else — a filter or sort on a non-indexed column, a join, an aggregate, a
window function — is served by an analytical scan, no index required:

```sql
SELECT category, COUNT(*) FROM products GROUP BY category;
SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
    WHERE c.region = 'EU' ORDER BY o.total DESC;
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS rn FROM events;
SELECT * FROM users WHERE name LIKE '%ada%' ORDER BY name;
```

## Substring search

`col LIKE '%infix%'` is served as a scan. A **trigram index** accelerates it
without changing the result — see [Full-text search](full-text-search.md).

## See also

- [Statements](statements.md) — every table needs a schema and a `PRIMARY KEY`.
- [Full-text search](full-text-search.md) — `@@` and trigram-accelerated `LIKE`.
- [Limitations & differences](limitations.md) — the compatibility contract.
