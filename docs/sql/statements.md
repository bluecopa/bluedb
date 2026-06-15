# Statements

[← SQL index](README.md)

## `CREATE TABLE`

Create a typed table:

```sql
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    name  TEXT,
    email TEXT,
    age   INTEGER
);
```

Create a **schemaless** table — no column list; each row is a free-form record:

```sql
CREATE TABLE events;
INSERT INTO events VALUES ('{"kind": "click", "x": 10}');
```

Options:

- `IF NOT EXISTS` — `CREATE TABLE IF NOT EXISTS users (…)`.
- `PRIMARY KEY` on a column makes it the clustered key; rows scan back in
  primary-key order without an explicit `ORDER BY` (see the [Note](#default-row-order) below).
- `CREATE TABLE … AS SELECT …` (CTAS) is supported, including a leading `WITH`.

```sql
CREATE TABLE adults AS SELECT * FROM users WHERE age >= 18;
```

!!! note
    Parameterized and vendor type spellings are accepted and
    normalized: `VARCHAR(100)` → `TEXT`, `DOUBLE` → `FLOAT`, `BIGINT`/`UHUGEINT`
    → `INTEGER`, `TIMESTAMP(6)` → `TIMESTAMP`, etc. See [Data types](data-types.md).

## `DROP TABLE`

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users;
```

## `INSERT`

```sql
INSERT INTO users (id, name, age) VALUES (1, 'ada', 36);
INSERT INTO users VALUES (2, 'lin', 'lin@x.io', 29);
INSERT INTO users SELECT id, name, email, age FROM staging;
```

!!! warning
    Insert **one row per statement** for large loads, or wrap a
    batch in a single transaction (`BEGIN … COMMIT`). Very large multi-row
    `VALUES (…), (…), …` lists hit a parser limit. See [Limitations](limitations.md).

## `UPDATE`

```sql
UPDATE users SET age = age + 1 WHERE id = 1;
UPDATE users SET email = 'ada@x.io';   -- no WHERE updates every row
```

## `DELETE`

```sql
DELETE FROM users WHERE age < 18;
DELETE FROM users;                     -- no WHERE deletes every row
```

!!! warning
    A `DELETE` / `UPDATE` with no `WHERE` affects **all rows**.

## `CREATE INDEX` / `DROP INDEX`

Single-column secondary indexes. The planner will use an index automatically
when a `WHERE` predicate matches it.

```sql
CREATE INDEX users_email ON users (email);
SELECT * FROM users WHERE email = 'ada@x.io';   -- uses users_email
DROP INDEX users_email ON users;
```

!!! warning
    Indexes are **single-column** only. Composite (multi-column)
    indexes are not supported.

## `CREATE VIEW` / `DROP VIEW`

Views are stored definitions that are inlined wherever the view is referenced.

```sql
CREATE VIEW active_users AS
    SELECT id, name FROM users WHERE age >= 18;

SELECT name FROM active_users ORDER BY name;
DROP VIEW active_users;
```

!!! note
    A view referencing another view is resolved one level deep.
    Materialized views are treated the same as regular views (definition inlined,
    not precomputed).

## Transactions

```sql
BEGIN;
INSERT INTO users VALUES (3, 'sam', 's@x.io', 41);
UPDATE accounts SET balance = balance - 10 WHERE id = 3;
COMMIT;          -- or ROLLBACK to discard everything since BEGIN
```

See [Transactions](transactions.md) for isolation and concurrency details.

## `SET` / `PRAGMA`

- `SET default_null_order = 'nulls_first' | 'nulls_last'` — controls where
  `NULL`s sort in `ORDER BY` (see [Query syntax](query-syntax.md#order-by)).
- Other engine-config `SET` / `PRAGMA` knobs are accepted as **no-ops** rather
  than rejected, so scripts written for other engines run unchanged.

<a name="default-row-order"></a>
!!! note
    **default row order.** Without `ORDER BY`, rows come back in
    **storage (primary-key) order**, not insertion order. Add `ORDER BY` whenever
    order matters.
