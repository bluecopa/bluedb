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

!!! warning
    **Every table needs a column list and a `PRIMARY KEY`.** bluedb has no
    schemaless tables: a column-less `CREATE TABLE`, or one without a primary
    key, is rejected. The primary key is the clustered row key (so it must be
    unique and is the default scan order) and the identity column for the
    warehouse Iceberg mirror.

Options:

- `IF NOT EXISTS`: `CREATE TABLE IF NOT EXISTS users (…)`.
- `PRIMARY KEY` on a column makes it the clustered key; rows scan back in
  primary-key order without an explicit `ORDER BY` (see the [Note](#default-row-order) below).
- `CREATE TABLE … AS SELECT …` (CTAS) is supported, including a leading `WITH`;
  the new table still requires a `PRIMARY KEY`.

```sql
CREATE TABLE adults AS SELECT * FROM users WHERE age >= 18;
```

### Composite primary keys

A multi-column `PRIMARY KEY (a, b, …)` is supported:

```sql
CREATE TABLE memberships (
    org_id  INTEGER,
    user_id INTEGER,
    role    TEXT,
    PRIMARY KEY (org_id, user_id)
);
```

The component columns are forced `NOT NULL`. Point, **leading-prefix**, range,
and **row-value keyset** lookups all use the key (the same shapes a Postgres
multicolumn index serves) as fast key-served access paths:

```sql
SELECT role FROM memberships WHERE org_id = 1 AND user_id = 7;   -- point
SELECT *    FROM memberships WHERE org_id = 1 ORDER BY org_id, user_id;  -- prefix
SELECT *    FROM memberships WHERE org_id = 1 AND user_id > 100;         -- range
SELECT *    FROM memberships WHERE (org_id, user_id) > (1, 100)
            ORDER BY org_id, user_id LIMIT 50;                          -- keyset page
```

Component values may be **inline literals or bound `$N` parameters**, so the
PostgREST-style `/tables` data plane works too. A value is coerced to its
column's type, so the key encodes identically however it was supplied (`org_id =
1` and a `"1"` data-plane param resolve to the same key).

Internally the composite key is backed by a single hidden surrogate column; it
is never returned (`SELECT *` shows only your columns) and is the identity column
for the [Iceberg mirror](../lakehouse/iceberg-mirror.md).

!!! note "v1 limitation"
    **`UPDATE` of a key column** is rejected (it changes the row's identity;
    delete and re-insert instead).

!!! note
    Parameterized and vendor type spellings are accepted and
    normalized: `VARCHAR(100)` → `TEXT`, `DOUBLE` → `FLOAT`, `BIGINT`/`UHUGEINT`
    → `INTEGER`, `TIMESTAMP(6)` → `TIMESTAMP`, etc. See [Data types](data-types.md).

## `DROP TABLE`

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users;
```

## `ALTER TABLE`

Schema evolution is **online**: these are O(1) metadata changes (stable
field-ids and table-ids under the hood), never a row rewrite, so they don't block
reads or writes:

```sql
ALTER TABLE users ADD COLUMN nickname TEXT;     -- old rows read back the default/NULL
ALTER TABLE users RENAME COLUMN nickname TO handle;
ALTER TABLE users DROP COLUMN handle;
ALTER TABLE users RENAME TO members;            -- O(1): no row/index re-key
```

!!! note
    `ADD COLUMN` with `NOT NULL` needs a `DEFAULT` (existing rows must have a
    value to read back).

!!! warning
    **Changing the primary key or a column's type is not an in-place operation.**
    The primary key is the physical row key and the type fixes the on-disk
    encoding, so both require a rebuild: `CREATE` the new table, `INSERT … SELECT`
    into it, `DROP` the old one, and `RENAME` the new one into place (the rename
    is O(1)). Dropping the primary-key column is rejected; a table can't be left
    without one.

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

Single-column secondary indexes. The planner uses an index automatically when a
`WHERE` predicate (or `ORDER BY`) matches it.

```sql
CREATE INDEX users_email ON users (email);
SELECT * FROM users WHERE email = 'ada@x.io';   -- uses users_email
DROP INDEX users_email ON users;
```

!!! note
    An index is a **performance** feature: it turns an equality or range filter
    (or an `ORDER BY`) on that column into a fast index-served lookup. Queries on
    non-indexed columns still run; they're served as analytical scans (see
    [Reads and indexes](query-guardrail.md)).

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

- `SET default_null_order = 'nulls_first' | 'nulls_last'`: controls where
  `NULL`s sort in `ORDER BY` (see [Query syntax](query-syntax.md#order-by)).
- `PRAGMA lakehouse_mirror[...]` and `PRAGMA lakehouse_target_file_bytes = <n>`
  control the [Iceberg mirror](../lakehouse/iceberg-mirror.md) (mirroring on/off
  per table or globally, and the compaction bin-pack target size). These are
  **applied**, not no-ops.
- Other engine-config `SET` / `PRAGMA` knobs are accepted as **no-ops** rather
  than rejected, so scripts written for other engines run unchanged.

<a name="default-row-order"></a>
!!! note
    **default row order.** Without `ORDER BY`, rows come back in
    **storage (primary-key) order**, not insertion order. Add `ORDER BY` whenever
    order matters.
