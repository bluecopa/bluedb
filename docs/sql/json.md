# JSON

[← SQL index](README.md)

bluedb has a **`JSON` / `JSONB` column type** and the common PostgreSQL JSON
operators and functions — field access (`->`, `->>`), containment (`@>`, `<@`),
and the `jsonb_path_query` family. JSON values round-trip as **real JSON** over
the [`/tables`](../api/rest.md) data plane, not as quoted strings.

!!! note "Where JSON runs"
    The JSON **operators and functions** are evaluated by bluedb's analytical
    engine. You reach them two ways: through [`POST /sql`](../api/rest.md#post-sql-run-one-parameterized-statement),
    and through a [`/tables`](../api/rest.md#get-tablestable-select) read whose
    filter uses a JSON path (`col->>key`) — that read is routed to the analytical
    engine automatically. Storing and retrieving whole JSON values works on every
    path; only the field-level operators are analytical-engine-only.

## The `JSON` / `JSONB` column type

Declare a column `JSON` or `JSONB` (the two are interchangeable — both are stored
as text and returned as JSON):

```sql
CREATE TABLE events (
    id    INTEGER PRIMARY KEY,
    actor TEXT,
    attrs JSON
);
```

Or with the structured DDL endpoint:

```bash
curl -s -X POST localhost:8081/schema/tables -H 'content-type: application/json' \
  -d '{"name":"events","columns":[
        {"name":"id","type":"INTEGER","primaryKey":true},
        {"name":"actor","type":"TEXT"},
        {"name":"attrs","type":"JSON"}
      ]}'
```

Insert a JSON object, array, or scalar — send it as JSON, not a string:

```bash
curl -s -X POST localhost:8081/tables/events -H 'content-type: application/json' \
  -d '{"id":1,"actor":"ada","attrs":{"status":"active","tags":["a","b"],"n":3}}'
```

A `GET` returns the `attrs` value as **real JSON**, ready to consume without a
second parse:

```bash
curl -s 'localhost:8081/tables/events?id=eq.1'
# [{"id":1,"actor":"ada","attrs":{"status":"active","tags":["a","b"],"n":3}}]
```

## Field access — `->` and `->>`

| Operator | Returns | Function form |
|----------|---------|---------------|
| `json -> key` | the sub-value as **JSON** (a string stays quoted) | `json_get(json, key)` |
| `json ->> key` | the sub-value as **text** (a string is unquoted) | `json_get_str(json, key)` |

`key` is an object field name (text) **or** an array index (integer). A parse
failure, a missing key, or a non-navigable value yields `NULL` — never an error —
so a column holding ragged JSON never fails a query.

```sql
SELECT attrs ->> 'status'          AS status,   -- 'active'  (text, unquoted)
       attrs -> 'tags'             AS tags,      -- ["a","b"] (JSON)
       attrs -> 'tags' ->> 0       AS first_tag, -- 'a'       (array index, chained)
       attrs ->> 'missing'         AS gone       -- NULL
FROM events;
```

Operators chain left-to-right; each `->` step returns JSON the next step parses.
Use the function form (`json_get_str(attrs,'status')`) when you want to avoid
operator-precedence surprises — see the caveat below.

!!! warning "Parenthesize `->>` in a comparison"
    The analytical engine gives `->>` **lower** precedence than `=`, so a bare
    `attrs ->> 'status' = 'active'` parses as `attrs ->> ('status' = 'active')`.
    Wrap the access in parentheses, or use the function form:

    ```sql
    SELECT * FROM events WHERE (attrs ->> 'status') = 'active';
    SELECT * FROM events WHERE json_get_str(attrs, 'status') = 'active';
    ```

    Over the `/tables` query string this is handled for you — `attrs->>status=eq.active`
    renders to the function form.

## Containment — `@>` and `<@`

PostgreSQL `jsonb` containment, evaluated recursively: an object contains an
object when every key/value is contained; an array contains an array when every
element is contained in some element; an array contains a scalar equal to one of
its elements.

| Operator | Meaning | Function form |
|----------|---------|---------------|
| `a @> b` | does `a` contain `b`? | `json_contains(a, b)` |
| `a <@ b` | is `a` contained by `b`? | `json_contains(b, a)` |

```sql
SELECT * FROM events WHERE attrs @> '{"status":"active"}';
SELECT * FROM events WHERE attrs -> 'tags' @> '["a"]';
```

!!! note "Key-existence `?` / `?|` / `?&` are not available"
    The analytical engine's SQL dialect reserves `?` as a parameter placeholder,
    so `attrs ? 'status'` fails at parse time before it can be interpreted as a
    JSON operator. Test for a key with containment or a path query instead:
    `attrs @> '{"status":null}'` is **not** equivalent, so use
    `(attrs ->> 'status') IS NOT NULL`.

## Path queries — `jsonb_path_query`

| Function | Returns |
|----------|---------|
| `jsonb_path_query(target, path)` | the first matching value (JSON text) |
| `jsonb_path_query_first(target, path)` | the first matching value (JSON text) |
| `jsonb_path_query_array(target, path)` | all matches as a JSON array |

```sql
SELECT jsonb_path_query(attrs, '$.status')        FROM events;  -- "active"
SELECT jsonb_path_query(attrs, '$.tags[0]')       FROM events;  -- "a"
SELECT jsonb_path_query_array(attrs, '$.tags[*]') FROM events;  -- ["a","b"]
```

Supported path steps are a **navigation subset**: `$` (root), `.key` and
`["key"]` (object member), `.*` (all object values), `[n]` (array index), `[*]`
(all array elements); a leading `strict`/`lax` word is accepted and ignored.

!!! warning "Set-returning vs. scalar, and unsupported paths"
    - PostgreSQL `jsonb_path_query` returns **one row per match**; bluedb's is a
      scalar, so it returns the **first** match (exact for the common single-match
      path). Use `jsonb_path_query_array` to get every match as one JSON array.
    - A path bluedb can't evaluate — filters `? (...)`, methods like `.type()`,
      ranges `[1 to 3]`, `starts with`, arithmetic, variables — **raises an error**
      rather than returning `NULL`, so an unevaluable path is never mistaken for a
      genuine no-match. A *supported* path that matches nothing returns `NULL`.

## See also

- [Expressions](expressions.md#operators) — JSON operators in the full operator set.
- [Functions](functions.md#json) — the JSON function reference.
- [REST API](../api/rest.md#json-path-filters-on-tables) — JSON-path filters on the `/tables` data plane.
- [Limitations & differences](limitations.md) — the compatibility contract.
