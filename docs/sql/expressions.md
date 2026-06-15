# Expressions

[← SQL index](README.md)

## Operators

| Category | Operators |
|----------|-----------|
| Arithmetic | `+` `-` `*` `/` `%` |
| Comparison | `=` `<>` (`!=`) `<` `<=` `>` `>=` |
| Logical | `AND` `OR` `NOT` |
| Null test | `IS NULL` `IS NOT NULL` |
| String | `||` (concatenation) |

```sql
SELECT price * quantity AS total FROM line_items;
SELECT * FROM users WHERE age >= 18 AND email IS NOT NULL;
SELECT first_name || ' ' || last_name AS full_name FROM users;
```

## Comparison & type coercion

A comparison between a **numeric** operand and a **numeric string literal** is
compared *numerically* (matching DuckDB / affinity engines), not as a failed
type match:

```sql
SELECT * FROM orders WHERE qty = '5';     -- '5' is compared as the number 5
SELECT * FROM t WHERE '1' = 1;            -- TRUE
SELECT * FROM products WHERE price < '9.99';
```

This coercion is conservative: only string **literals** are cast (never a stored
text column), and only when the literal parses into the target numeric type — so
it never turns a working query into a runtime cast error.

!!! warning
    **`bool`/`int` comparisons are not coerced.** `TRUE = 1` evaluates to
    **FALSE**. Engines disagree here (DuckDB/MySQL say true, PostgreSQL errors), so
    bluedb leaves it alone — use an explicit `CAST`. A number compared to a
    *non-numeric* text column (`name = 5`) is likewise not coerced.

## `CAST` and `TRY_CAST`

```sql
SELECT CAST('42' AS INTEGER);
SELECT CAST(qty AS FLOAT) / 2 FROM orders;
SELECT '42'::INTEGER;            -- shorthand cast
SELECT TRY_CAST(code AS INTEGER) FROM raw;   -- treated as CAST
```

!!! note
    `TRY_CAST` / `SAFE_CAST` are accepted and run as `CAST`. They differ
    only when the cast would *fail*: standard `TRY_CAST` yields `NULL`, whereas
    bluedb's `CAST` raises an error.

## `CASE`

```sql
SELECT name,
       CASE WHEN age < 18 THEN 'minor'
            WHEN age < 65 THEN 'adult'
            ELSE 'senior'
       END AS bracket
FROM users;

-- Simple form
SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM accounts;
```

## `IN` / `NOT IN`

```sql
SELECT * FROM users WHERE age IN (18, 21, 65);
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned);
```

## `BETWEEN`

```sql
SELECT * FROM events WHERE ts BETWEEN TIMESTAMP '2026-01-01 00:00:00'
                                  AND TIMESTAMP '2026-02-01 00:00:00';
```

## `IS NULL` / `COALESCE` / `NULLIF`

```sql
SELECT COALESCE(nickname, name, 'anon') AS display FROM users;
SELECT NULLIF(status, '') FROM accounts;          -- '' -> NULL
SELECT * FROM users WHERE deleted_at IS NULL;
```

## Subquery expressions

`EXISTS`, scalar subqueries, and `IN (SELECT …)` are all valid in expressions —
see [Query syntax › Subqueries](query-syntax.md#subqueries).

!!! warning
    **window functions are not supported.** `SUM(x) OVER (…)`,
    `ROW_NUMBER() OVER (…)`, `RANK()`, etc. are **rejected** with a clear error
    (the engine has no windowing; rejecting prevents silently wrong results).
