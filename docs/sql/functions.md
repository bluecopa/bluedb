# Functions

[← SQL index](README.md)

These are the built-in functions bluedb's engine provides. Exact argument
signatures follow [GlueSQL](https://gluesql.org); the groupings below cover the
common ones.

!!! note
    **Window functions** (`… OVER (…)` — `ROW_NUMBER`, `RANK`, running `SUM`, …)
    are supported. **User-defined functions** (`CREATE FUNCTION`) are not, and
    some PostgreSQL/DuckDB-specific builtins (`arg_min`, `list_*`, `any_value`, …)
    are absent. See [Limitations](limitations.md).

## Aggregate functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` / `COUNT(expr)` | Row count / non-null count |
| `SUM(expr)` | Sum |
| `MIN(expr)` / `MAX(expr)` | Minimum / maximum |
| `AVG(expr)` | Average (returns a float) |
| `VARIANCE(expr)` | Variance |
| `STDEV(expr)` | Standard deviation |

```sql
SELECT age, COUNT(*) AS n, AVG(age) FROM users GROUP BY age;
```

!!! warning
    An aggregate with **no `GROUP BY`** over a `WHERE` that matches
    **zero rows** returns **no row** (rather than one `NULL`/`0` row). Guard with a
    separate `COUNT` if you need the empty case.

## Math

| | | | |
|---|---|---|---|
| `ABS` | `SIGN` | `CEIL` | `FLOOR` |
| `ROUND` | `TRUNC` | `SQRT` | `POWER` |
| `EXP` | `LN` | `LOG` | `LOG2` / `LOG10` |
| `MOD` | `DIV` | `GCD` | `LCM` |
| `PI` | `RADIANS` | `DEGREES` | `GREATEST` |
| `SIN` `COS` `TAN` | `ASIN` `ACOS` `ATAN` | `RAND` | |

```sql
SELECT ROUND(price, 2), SQRT(area), ABS(balance) FROM t;
```

## String

| | | | |
|---|---|---|---|
| `LOWER` | `UPPER` | `INITCAP` | `LENGTH` |
| `LEFT` | `RIGHT` | `SUBSTR` | `POSITION` |
| `LPAD` | `RPAD` | `LTRIM` | `RTRIM` / `TRIM` |
| `CONCAT` | `CONCAT_WS` | `REPLACE` | `REPEAT` |
| `REVERSE` | `ASCII` | `CHR` | `FIND_IDX` |
| `FORMAT` | `HEX` | `MD5` | `IS_EMPTY` |

```sql
SELECT UPPER(name), SUBSTR(email, 1, POSITION('@' IN email) - 1) FROM users;
SELECT CONCAT_WS('-', area_code, number) FROM phones;
```

## Date & time

| Function | Description |
|----------|-------------|
| `NOW()` | Current timestamp |
| `CURRENT_DATE` / `CURRENT_TIME` / `CURRENT_TIMESTAMP` | Current date / time / timestamp |
| `EXTRACT(field FROM ts)` | Pull a field (`YEAR`, `MONTH`, `DAY`, `HOUR`, …) |
| `ADD_MONTH(date, n)` | Add `n` months |
| `LAST_DAY(date)` | Last day of the month |
| `TO_DATE` / `TO_TIME` / `TO_TIMESTAMP` | Parse a string with a format |

```sql
SELECT EXTRACT(YEAR FROM created_at), COUNT(*) FROM users
GROUP BY EXTRACT(YEAR FROM created_at);
```

## Null & conditional

| Function | Description |
|----------|-------------|
| `COALESCE(a, b, …)` | First non-null argument |
| `IFNULL(a, b)` | `b` if `a` is null |
| `NULLIF(a, b)` | `NULL` if `a = b`, else `a` |
| `GREATEST(a, b, …)` | Largest argument |

## Other

| Function | Description |
|----------|-------------|
| `GENERATE_UUID()` | Random UUID |
| `POINT(x, y)`, `GET_X`, `GET_Y`, `CALC_DISTANCE` | Geometry point + helpers |
| `APPEND`, `PREPEND`, `SLICE`, `SORT`, `DEDUP`, `KEYS`, `VALUES`, `ENTRIES`, `TAKE`, `SKIP` | `LIST` / `MAP` helpers |

```sql
SELECT GENERATE_UUID() AS id;
```
