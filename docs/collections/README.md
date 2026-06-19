# Collections

bluedb exposes a **MongoDB-style document API** alongside its SQL and REST surfaces.
You store schema-free JSON documents in named collections, filter and mutate them
with familiar MQL operators, and run aggregation pipelines — all over plain HTTP,
without a native MongoDB driver or the MongoDB wire protocol. A collection is
backed by a table `coll(_id TEXT PRIMARY KEY, doc JSON)` managed transparently by
the server. The **tenant** (analogous to a MongoDB database) is selected per-request
via `X-Bluedb-Tenant`; omitting the header uses the default tenant `_`.

!!! note "Not the MongoDB wire protocol"
    This surface speaks **HTTP/JSON**, not the MongoDB binary wire protocol (OP_MSG /
    OP_QUERY). Native `mongosh` or a `MongoClient` driver cannot connect directly;
    use any HTTP client. The request/response shapes are intentionally MongoDB-shaped
    so migration is straightforward.

## Authorization

Collections use the same bearer-token authorization as the rest of bluedb (see
[REST API — Authorization](../api/rest.md#authorization)).

| Operation | Required scope |
|-----------|----------------|
| `insert`, `update`, `delete` | `data:write` |
| `find`, `count`, `aggregate` | `data:read` |
| `createIndex` | `schema:admin` |

Pass the token and (optionally) a tenant on every request:

```
Authorization: Bearer <token>
X-Bluedb-Tenant: acme
```

## Endpoints

All endpoints follow the pattern `POST /collections/{coll}/{operation}`.

| Endpoint | Request body | Response |
|----------|-------------|----------|
| `POST /collections/{coll}/insert` | `{"documents": [{…}, …]}` | `{"insertedIds": […], "insertedCount": N}` |
| `POST /collections/{coll}/find` | `{"filter": {}, "projection": {}, "sort": {}, "limit": N, "skip": N}` | `{"documents": […]}` |
| `POST /collections/{coll}/update` | `{"filter": {}, "update": {}, "multi": false, "upsert": false}` | `{"matchedCount": N, "modifiedCount": N, "upsertedId": …}` |
| `POST /collections/{coll}/delete` | `{"filter": {}, "multi": false}` | `{"deletedCount": N}` |
| `POST /collections/{coll}/aggregate` | `{"pipeline": [{…}, …]}` | `{"documents": […]}` |
| `POST /collections/{coll}/count` | `{"filter": {}}` | `{"count": N}` |
| `POST /collections/{coll}/createIndex` | `{"keys": {"field": 1}, "options": {"unique": false}}` | `{"name": "<index_name>"}` |

All fields in request bodies are optional unless noted. An error returns the
MongoDB-shaped body `{"ok": 0, "code": <N>, "codeName": "<name>", "errmsg": "<msg>"}`.

### Examples

**Insert two documents** (the server assigns `_id` if absent):

```bash
curl -s -X POST localhost:8081/collections/orders/insert \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"documents": [
        {"customer": "ada", "amount": 120, "status": "pending"},
        {"customer": "lin", "amount": 85,  "status": "shipped"}
      ]}'
# {"insertedIds":["a1b2c3d4e5f6...","..."],"insertedCount":2}
```

**Find by filter with sort and limit:**

```bash
curl -s -X POST localhost:8081/collections/orders/find \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"filter": {"status": "pending"}, "sort": {"amount": -1}, "limit": 10}'
# {"documents":[{...}, ...]}
```

**Increment a field:**

```bash
curl -s -X POST localhost:8081/collections/orders/update \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"filter": {"_id": "a1b2c3d4e5f6..."}, "update": {"$inc": {"amount": 10}}}'
# {"matchedCount":1,"modifiedCount":1,"upsertedId":null}
```

**Create a secondary index** (backfills existing rows):

```bash
curl -s -X POST localhost:8081/collections/orders/createIndex \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"keys": {"status": 1}, "options": {"unique": false}}'
# {"name":"cidx_orders_status"}
```

**Create a compound index** (accelerates full-key equality lookups):

```bash
curl -s -X POST localhost:8081/collections/orders/createIndex \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"keys": {"customer": 1, "status": 1}}'
# {"name":"cidx_orders_customer_status"}
```

**Aggregate with `$unwind` and `$group`:**

```bash
curl -s -X POST localhost:8081/collections/orders/aggregate \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"pipeline": [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
      ]}'
```

**`$lookup` join** (one output row per match):

```bash
curl -s -X POST localhost:8081/collections/orders/aggregate \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer mytoken' \
  -H 'x-bluedb-tenant: acme' \
  -d '{"pipeline": [
        {"$lookup": {
          "from": "customers",
          "localField": "customer",
          "foreignField": "name",
          "as": "customerDoc"
        }}
      ]}'
# Note: "customerDoc" is a flat JSON value per match, not a nested array.
```

---

## Compatibility matrix

### `find` / `count` filter operators

| Operator | Supported | Notes |
|----------|-----------|-------|
| `{field: value}` implicit `$eq` | yes | |
| `$eq` | yes | |
| `$ne` | yes | Matches documents where the field is **missing or null** as well as those where it holds a different value (MongoDB semantics) |
| `$gt`, `$gte`, `$lt`, `$lte` | yes | |
| `$in` | yes | |
| `$nin` | yes | Matches documents where the field is **missing or null** in addition to those where the field value is not in the list (MongoDB semantics) |
| `$exists` | yes | |
| `$regex` | yes (SQL fast path) / no (aggregate) | The `~` SQL operator is used on the transactional fast path. `$regex` is **rejected** by the DataFusion analytical path (`aggregate`'s `$match` stage and any `find` routed to the analytical engine). Treat as best-effort. |
| `$and` | yes | |
| `$or` | yes | |
| `$not` | yes | |
| `$elemMatch` | **no** | Returns `UnsupportedOperator` |
| `$type` | **no** | Returns `UnsupportedOperator` |
| `$where` | **no** | Returns `UnsupportedOperator` |
| `$text` | **no** | Returns `UnsupportedOperator` |
| `$expr` | **no** | Returns `UnsupportedOperator` |

Any top-level key that starts with `$` and is not `$and`, `$or`, or `$not` is
rejected immediately. Any field-level operator that is not one of the ten above
is rejected immediately.

### Update operators

| Operator | Supported | Notes |
|----------|-----------|-------|
| Full-document replacement | yes | Body has no `$`-keys; `_id` is preserved |
| `$set` | yes | |
| `$unset` | yes | |
| `$inc` | yes | Initializes to 0 if field absent |
| `$push` | yes | Initializes to `[]` if field absent |
| `$pull` | yes | Value equality only; no query selectors |
| `$addToSet` | **no** | |
| `$pop` | **no** | |
| `$rename` | **no** | |
| `$mul` | **no** | |
| `$min` / `$max` | **no** | |
| `$bit` | **no** | |
| `$currentDate` | **no** | |
| Array update operators (`$`, `$[]`, `$[<id>]`) | **no** | |

Any unrecognized operator (key starting with `$`) is rejected with an error.

### Aggregation stages

| Stage | Supported | Notes |
|-------|-----------|-------|
| `$match` | yes | Full filter expression (same operators as `find`); `$regex` rejected on this path |
| `$sort` | yes | `{field: 1}` ascending, `{field: -1}` descending |
| `$limit` | yes | |
| `$skip` | yes | |
| `$count` | yes | Output field name as a string argument |
| `$group` | yes | `_id` must be `"$field"` or `null`; accumulators: `$sum`, `$avg`, `$min`, `$max`, `$count` |
| `$project` | yes (best-effort) | Inclusion `{field: 1}` and `{out: "$field"}` renaming work; field exclusion `{field: 0}` is silently skipped |
| `$addFields` / `$set` | yes (best-effort) | Appends computed fields; same limitations as `$project` |
| `$lookup` | yes | Left join on **document fields** (not limited to `_id`). Join keys are materialized from the document store; one output row is produced per match. **Limitation:** the `as` value is a flat per-match JSON value — not Mongo's nested single-element array (Arrow list columns are not yet rendered as JSON arrays in responses). Joining on a JSON sub-field path (e.g. `"a.b"`) is **not supported** — the join key must be a plain field name. |
| `$unwind` | yes | Expands a JSON array field stored in the document (`"$arrayField"`) into one row per element. The expanded element is referenceable by subsequent stages (e.g. `$group {_id: "$arrayField"}`). A missing or empty array yields no rows (MongoDB default behavior). |
| `$facet` | **no** | `UnsupportedStage` |
| `$graphLookup` | **no** | `UnsupportedStage` |
| `$bucket` / `$bucketAuto` | **no** | `UnsupportedStage` |
| `$setWindowFields` | **no** | `UnsupportedStage` |
| `$merge` / `$out` | **no** | `UnsupportedStage` |
| `$replaceRoot` / `$replaceWith` | **no** | `UnsupportedStage` |
| `$unionWith` | **no** | `UnsupportedStage` |

All pipelines run against the **analytical engine** (DataFusion over the Iceberg
mirror). Data is visible to `aggregate` only after the Iceberg seal cycle —
see [Freshness](#freshness) below.

### Indexing

| Feature | Supported | Notes |
|---------|-----------|-------|
| Single-field index | yes | `{"keys": {"field": 1}}` |
| Unique index | yes | `{"options": {"unique": true}}` |
| Compound index | yes | `{"keys": {"a": 1, "b": 1}}` — accelerates **full-key equality** lookups (`{a: x, b: y}`) on the fresh fast path. Prefix-only queries (`{a: x}`) or range queries on the last component are not accelerated by the compound index — they fall to single-field indexes or the analytical engine. |
| Multikey index (array field) | **no** | |
| Geospatial index | **no** | |
| TTL index | **no** | |
| Text index | **no** | Use [SQL full-text search](../sql/full-text-search.md) instead |
| Partial index | **no** | |
| Sparse index | **no** | |
| Hashed index | **no** | |

Indexes are **gateway-maintained**: the server adds a derived column
`__cidx_<path>` to the backing table and keeps it in sync on every `insert`,
`update`, and `delete`. The path must be a valid dot-separated identifier (e.g.
`address.city`); slashes and other special characters are rejected.

!!! note "Index type inference: string, integer/bool, and float fields"
    `createIndex` infers the column type by sampling existing documents in the
    collection. You can also override it explicitly via the `options` field:
    `{"options": {"type": "number"}}`, `{"options": {"type": "bool"}}`, or
    `{"options": {"type": "string"}}`.

    - **String fields** — the derived column is TEXT; equality and range filters
      on string values (`{status: "active"}`) use the fast path.
    - **Integer/boolean fields** — the derived column is an INTEGER or BOOLEAN
      column. Filters on integer values (e.g. `{age: 36}`) or boolean values
      (e.g. `{enabled: true}`) are served on the **fresh fast path** (read-your-writes),
      just like string fields. `5` and `5.0` are treated as the same value.
    - **Fractional/float fields** — a field whose sampled values include a
      fractional part (e.g. `3.14`) cannot use the fast INT index. Queries on
      that field are served by the **analytical engine** (seconds-fresh, correct
      results). This is automatic — type inference picks the right column type.

    `$exists` always uses the derived column regardless of type (it is a NULL
    check and is type-agnostic).

!!! note "Sort on nested/dotted paths not supported"
    The `sort` field in a `find` request supports **top-level** field names
    only (e.g. `{"sort": {"age": -1}}`). Nested paths such as
    `{"sort": {"address.city": 1}}` are not supported in v1 and will produce
    unexpected results.

---

## Freshness

bluedb has two read paths for collections. The one that serves a given request
depends on whether the filter field is indexed:

**Transactional fast path (read-your-writes)**
: Queries on `_id`, or any field that has a gateway-maintained index
  (`createIndex` was called for that path) **and** whose filter value is a
  string, integer, or boolean, are served by the transactional engine directly.
  Writes you just issued are immediately visible. `insert`, `update`, and
  `delete` always write through this path.

**Analytical path (seconds-fresh)**
: Queries on a field that is **not** indexed — all `aggregate` requests — and
  queries on an indexed field whose values are fractional/float — are served by
  the analytical engine over the Iceberg mirror. The mirror reflects data as of
  the last **seal cycle** (typically sub-second to a few seconds, depending on
  write cadence and deployment topology). Data written in the current seal window
  may not yet be visible. Clients that need read-your-writes guarantees on a
  non-indexed field should either:
  - add an index for that field with `createIndex`, or
  - wait for the seal by polling `count` or an indexed field as a watermark.

`find` and `count` may be silently rerouted to the analytical path when the
query guardrail rejects a non-indexed JSON field scan on the transactional
engine; the response shape is identical regardless of which path served it.

!!! note "update / delete on non-indexed fields sees sealed data"
    When the `filter` in an `update` or `delete` targets a **non-indexed**
    field, the pre-read that identifies matching documents is served by the
    analytical engine (same seconds-fresh caveat as `find` on non-indexed
    fields). Documents written in the current seal window may not yet be
    visible to that pre-read. Filtering on `_id` or an indexed field always
    uses the transactional engine (read-your-writes).

---

## Not supported in v1

The following MongoDB features are not implemented in this release:

- **MongoDB wire protocol** — native `mongosh` / `MongoClient` drivers cannot connect.
- **Server-side cursors** — all results are returned in a single response body.
- **Multikey, geospatial, TTL, text, partial, sparse, and hashed indexes.**
- **Compound index prefix/range acceleration** — a compound index `{a,b}` accelerates full-key equality (`{a:x, b:y}`) but not prefix-only (`{a:x}`) or range queries on the last component; those fall to single-field indexes or the analytical engine.
- **Fractional/float field fast-path** — queries on a field indexed as float are served by the analytical engine (seconds-fresh, not read-your-writes).
- **Aggregation stages:** `$facet`, `$graphLookup`, `$bucket`/`$bucketAuto`, `$setWindowFields`, `$merge`, `$out`, `$replaceRoot`, `$replaceWith`, `$unionWith`.
- **`$lookup` nested-array fidelity** — the `as` field contains a flat JSON value per match, not Mongo's nested single-element array. This is a current serialization limitation (Arrow list columns are not yet rendered as JSON arrays in responses).
- **`$lookup` on JSON sub-field paths** — the join key must be a plain top-level field name, not a dotted path like `"address.city"`.
- **`$unwind` on missing/null fields** — a document where the unwound field is absent or null yields no output rows (consistent with MongoDB's default behavior; `preserveNullAndEmptyArrays` is not supported).
- **Distributed multi-document transactions** — updates are applied document-by-document; there is no multi-document ACID boundary across the collections API.
- **Heterogeneous `_id` types** — `_id` is always `TEXT`. Integer, ObjectId, and composite `_id` values are stored as their JSON string representation.
- **Array update operators** — `$`, `$[]`, `$[<identifier>]`, `$addToSet`, `$pop`.
- **`$regex` in aggregation pipelines** — `$regex` is rejected in the `$match` stage that runs on the analytical engine.

## See also

- [REST API](../api/rest.md) — the SQL and PostgREST data planes.
- [Full-text search](../sql/full-text-search.md) — BM25 and trigram search over SQL.
- [Iceberg mirror](../lakehouse/iceberg-mirror.md) — the analytical engine backing `aggregate`.
- [Query guardrail](../sql/query-guardrail.md) — why non-indexed queries are rerouted.
