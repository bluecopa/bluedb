# Data types

[← SQL index](README.md)

## Native types

| Type | Notes |
|------|-------|
| `BOOLEAN` | `TRUE` / `FALSE` |
| `INTEGER` | 64-bit signed |
| `FLOAT` | 64-bit (double precision) |
| `DECIMAL` | Exact numeric |
| `TEXT` | UTF-8 string |
| `BYTEA` | Binary string |
| `DATE` | Calendar date |
| `TIME` | Time of day |
| `TIMESTAMP` | Date + time |
| `INTERVAL` | Duration |
| `UUID` | 128-bit UUID |

!!! note
    `LIST`, `MAP`, and `POINT` values are also supported (for the list/map/geo
    functions); they are niche and not covered in depth here.

## Accepted aliases (normalized)

bluedb accepts the common vendor/parameterized spellings and normalizes them to
a native type, so DDL written for PostgreSQL, MySQL, or DuckDB loads as-is:

| You write | Stored as |
|-----------|-----------|
| `VARCHAR(n)`, `CHAR(n)`, `NVARCHAR`, `CLOB`, `STRING` | `TEXT` |
| `DECIMAL(p,s)`, `NUMERIC`, `BIGNUM` | `DECIMAL` |
| `INT(n)`, `BIGINT`, `SMALLINT`, `TINYINT`, `INT2/4/8`, `HUGEINT` | `INTEGER` |
| `UHUGEINT`, `UBIGINT`, `UINT…`, `INT128` (unsigned/wide) | `INTEGER` |
| `DOUBLE`, `DOUBLE PRECISION`, `REAL`, `FLOAT(n)`, `FLOAT4/8` | `FLOAT` |
| `TIMESTAMP(n)`, `TIMESTAMPTZ`, `DATETIME` | `TIMESTAMP` |
| `TIME(n)` | `TIME` |

```sql
-- All of these are accepted:
CREATE TABLE t (
    a VARCHAR(255),     -- -> TEXT
    b DOUBLE,           -- -> FLOAT
    c BIGINT,           -- -> INTEGER
    d TIMESTAMP(6),     -- -> TIMESTAMP (sub-second precision dropped)
    e DECIMAL(10, 2)    -- -> DECIMAL
);
```

!!! warning
    **lossy normalizations.**
    - Wide/unsigned integers (`UHUGEINT`, `INT128`, …) become 64-bit `INTEGER`;
      values outside the signed-64 range will not round-trip.
    - `TIMESTAMP(n)` / `TIME(n)` drop their sub-second **precision** specifier and
      any timezone; values are kept, the precision annotation is not.

## Type literals

```sql
SELECT DATE '2026-01-15';
SELECT TIMESTAMP '2026-01-15 09:30:00';
SELECT CAST('9.99' AS DECIMAL);
```

## Not supported

`BIT`, `STRUCT`, and user-defined enum types (`CREATE TYPE`) are not supported.
See [Limitations](limitations.md).
