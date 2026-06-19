# Functions

[← SQL index](README.md)

These are the built-in functions bluedb provides. The groupings below cover the
common ones; signatures are PostgreSQL-style.

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
| `VARIANCE(expr)` | Sample variance (use `var_pop` for population) |
| `STDEV(expr)` | Sample standard deviation (use `stddev_pop` for population) |

```sql
SELECT age, COUNT(*) AS n, AVG(age) FROM users GROUP BY age;
```

!!! warning
    An aggregate with **no `GROUP BY`** over a `WHERE` that matches
    **zero rows** returns **no row** (rather than one `NULL`/`0` row). Guard with a
    separate `COUNT` if you need the empty case.

### Approximate aggregates

For high-cardinality dashboards these trade exactness for speed and bounded
memory:

| Function | Description |
|----------|-------------|
| `approx_count_distinct(expr)` | Approximate distinct count (HyperLogLog) |
| `approx_percentile(expr, p)` / `approx_quantile(expr, p)` | Approximate percentile, `p` in `[0,1]` (t-digest) |
| `approx_median(expr)` | Approximate median |

```sql
SELECT region,
       approx_count_distinct(user_id)      AS uniques,
       approx_percentile(latency_ms, 0.99) AS p99
FROM events GROUP BY region;
```

#### Mergeable distinct-count sketches

`approx_count_distinct` is one-shot. To **store** a distinct-count sketch and
union it later — incremental rollups, or combining per-shard/per-day counts —
use the HyperLogLog sketch functions. A sketch is a `BYTEA` value you can persist:

| Function | Description |
|----------|-------------|
| `hll_build(expr)` | Aggregate: build a sketch (`BYTEA`) from a column |
| `hll_merge(sketch)` | Aggregate: union sketches into one |
| `hll_count(sketch)` | Scalar: estimated distinct count from a sketch |

```sql
-- per-day sketches, persisted once
SELECT day, hll_build(user_id) AS sketch FROM events GROUP BY day;

-- later: distinct users across an arbitrary day range, no re-scan of events
SELECT hll_count(hll_merge(sketch)) FROM daily_sketches WHERE day >= '2026-01-01';
```

!!! note
    Sketches use bluedb's own format (`p = 14`, ~0.8% standard error) and are
    portable across bluedb instances, not across other HyperLogLog libraries.

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
| `FORMAT` | `HEX` | `MD5` | `SPLIT_PART` |

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

## Formatting

PostgreSQL-style text/number formatting.

| Function | Description |
|----------|-------------|
| `to_char(numeric, fmt)` | Format a number to text — `9`/`0` digits, `.`/`D` decimals, `,`/`G` thousands grouping (e.g. `'FM9,999.00'`) |
| `to_char(date/timestamp, fmt)` | Format a temporal value to text (the same `to_char`, dispatched on argument type) |
| `to_number(text, fmt)` | Parse a formatted number string to a float; the mask is advisory (US-style `.`/`,`), group separators and currency are stripped |
| `format(fmt, …)` | Substitute `%s` / `%I` / `%L` with successive arguments (as text); `%%` is a literal `%` |

```sql
SELECT to_char(1234.5, 'FM9,999.00');     -- '1,234.50'
SELECT to_number('$1,234.50', '9G999D99'); -- 1234.5
SELECT format('%s/%s', area_code, number) AS phone FROM phones;
```

## JSON

Field access (`->`, `->>`), containment (`@>`, `<@`), and `jsonb_path_query` /
`jsonb_path_query_first` / `jsonb_path_query_array` are documented on the
[JSON](json.md) page.

## Other

| Function | Description |
|----------|-------------|
| `GENERATE_UUID()` | Random UUID |

```sql
SELECT GENERATE_UUID() AS id;
```

!!! note "Arrays & maps"
    For list/map work use the `array_*` / `map_*` family — e.g. `array_append`,
    `array_prepend`, `array_slice`, `array_sort`, `array_distinct`, `cardinality`,
    `map_keys`, `map_values`, `map_entries`. Mind the signatures: `array_prepend`
    is element-first, and `array_distinct` removes **all** duplicates (not just
    consecutive ones). Geometry types and functions are not available.
