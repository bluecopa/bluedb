# Query syntax

[← SQL index](README.md)

A `SELECT` reads rows. Clauses run in the usual logical order: `FROM` →
`WHERE` → `GROUP BY` → `HAVING` → `SELECT` (projection) → `DISTINCT` →
`ORDER BY` → `LIMIT`/`OFFSET`.

```sql
SELECT name, age
FROM users
WHERE age >= 18
ORDER BY age DESC
LIMIT 10;
```

## `FROM` and joins

```sql
-- INNER JOIN
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id;

-- LEFT JOIN
SELECT u.name, o.total
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- JOIN ... USING (rewritten to the equivalent ON)
SELECT * FROM users JOIN orders USING (user_id);

-- Comma join WITH an equi-join key (runs as a hash join)
SELECT u.name, o.total
FROM users u, orders o
WHERE u.id = o.user_id;
```

Equi-joins (`a.x = b.y`) run as **hash joins**: the planner pushes the equality
from `WHERE` into the join automatically, so a comma join with a key is as fast
as an explicit `JOIN … ON`.

!!! note
    A multi-table query without a join key (a `CROSS JOIN`, or a comma join with
    no `WHERE` equality) produces the full cartesian product — fine for small
    inputs, expensive for large ones. Give every join a key when you mean an
    equi-join.

## `WHERE`

```sql
SELECT * FROM users WHERE age > 21 AND email IS NOT NULL;
SELECT * FROM users WHERE name IN ('ada', 'lin');
SELECT * FROM users WHERE age BETWEEN 18 AND 65;
```

See [Expressions](expressions.md) for the full operator set and comparison
semantics (including numeric/text coercion).

## `GROUP BY` / `HAVING`

```sql
SELECT age, COUNT(*) AS n
FROM users
GROUP BY age
HAVING COUNT(*) > 1
ORDER BY age;
```

Available aggregates: `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `VARIANCE`, `STDEV`
(see [Functions](functions.md#aggregate-functions)).

## `ORDER BY`

```sql
SELECT name FROM users ORDER BY name;            -- ascending
SELECT name FROM users ORDER BY age DESC, name;  -- multi-key
SELECT name FROM users ORDER BY name NULLS FIRST;
```

**NULL ordering.** By default `NULL` sorts as the largest value (last when
ascending, first when descending). You can be explicit per key with
`NULLS FIRST` / `NULLS LAST`, or set a session default:

```sql
SET default_null_order = 'nulls_first';
SELECT name FROM users ORDER BY name;   -- NULLs now sort first
```

## `LIMIT` / `OFFSET`

```sql
SELECT * FROM users ORDER BY id LIMIT 20 OFFSET 40;   -- page 3 of 20
```

## `DISTINCT`

```sql
SELECT DISTINCT age FROM users;
SELECT DISTINCT age, name FROM users;
```

## Set operations

Combine the results of two queries.

```sql
SELECT name FROM users      UNION      SELECT name FROM admins;   -- de-duplicated
SELECT name FROM users      UNION ALL  SELECT name FROM admins;   -- keep duplicates
SELECT id   FROM users      INTERSECT  SELECT id FROM admins;     -- rows in both
SELECT id   FROM users      EXCEPT     SELECT id FROM admins;     -- in left, not right
```

- `UNION` / `UNION ALL` support **any number of columns**.
- `INTERSECT` / `EXCEPT` are **single-column** only.

!!! note
    Set operations return the correct rows but not necessarily in
    source order; add `ORDER BY` if order matters.

## Common table expressions (CTEs)

Non-recursive `WITH`:

```sql
WITH adults AS (
    SELECT id, name FROM users WHERE age >= 18
)
SELECT name FROM adults ORDER BY name;
```

Multiple and chained CTEs work, and `WITH` is allowed in front of
`CREATE TABLE … AS` and `INSERT … SELECT`.

!!! warning
    `WITH RECURSIVE` is **not** supported (it needs iterative
    evaluation). See [Limitations](limitations.md).

## Subqueries

```sql
-- IN / NOT IN
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders);

-- EXISTS
SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- Scalar subquery
SELECT name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS orders
FROM users u;

-- Derived table
SELECT t.age, t.n FROM (SELECT age, COUNT(*) AS n FROM users GROUP BY age) t;
```
