# Collections — MongoDB-style Document Interface (Design)

**Status:** Design · **Date:** 2026-06-19

**Sub-project A** of bluedb's document + search capability. Sub-project **B** —
an Elasticsearch-style text-search interface — is a separate spec and shares the
richer JSON indexes introduced here.

## Summary

A `/collections` HTTP surface that gives bluedb a **MongoDB-shaped document API** —
collection CRUD, `find` with the common query operators, and the aggregation
pipeline — by translating MongoDB Query Language (MQL) *shapes* onto bluedb's
existing engine. There is **no new storage engine and no MongoDB wire protocol**:
a stateless translation gateway sits over the JSON-column, secondary-index, and
DataFusion machinery bluedb already has.

## Decisions (locked during brainstorming)

- **API-shape compatibility, not wire-protocol.** MQL-shaped JSON over bluedb's
  own HTTP endpoints; native MongoDB drivers do **not** connect. Rationale:
  captures the ergonomic/adoption value (developers think in MQL) without
  emulating a binary protocol or chasing MongoDB's version churn.
- **Familiar 80% subset, not a high-fidelity port.** A pragmatic, documented
  subset; a published compatibility matrix states what is supported. Not
  bug-for-bug compatible.
- **Namespace `/collections`** — vendor-neutral, and does not imply the
  wire-compat we are explicitly not building.
- **The aggregation pipeline routes to DataFusion** by building a `LogicalPlan`
  **directly** from the pipeline stages (no SQL-string round-trip).
- **Reuse, don't rebuild.** The gateway is a thin translator over the existing
  engine; no engine changes.

## Goals / Non-goals

**Goals (v1):** collection CRUD; `find` with the common operators; the
aggregation pipeline (common stages); single-field JSON-path indexes (incl.
unique); Mongo-shaped result and error documents; per-tenant ("database")
scoping; inheriting bluedb's single-writer consistency and read-your-writes.

**Non-goals (v1):** the MongoDB wire protocol; native drivers; server-side
cursors; compound / multikey / geospatial / TTL indexes; aggregation
`$facet` / `$graphLookup` / `$bucket` / `$setWindowFields`; change streams;
distributed multi-document transactions; heterogeneous `_id` types.

## Architecture

```
client ──MQL JSON──▶ /collections/{coll}/{verb}  (bluedb-server handlers)
                              │
                              ▼
                     bluedb-collections          ← new crate: pure MQL↔bluedb translation
                       ├─ filter  → predicate / DataFusion Expr
                       ├─ update  → in-process document mutation (RMW)
                       └─ pipeline→ DataFusion LogicalPlan (built directly)
                              │
              ┌───────────────┴───────────────┐
              ▼                                ▼
   GlueSQL fast path                  DataFusion analytical
   (_id / indexed JSON path,          (unindexed filter/sort,
    fresh, read-your-writes)           aggregation pipeline)
              │                                │
              ▼                                ▼
        bluedb-sql / SlateDB         bluedb-query over Iceberg mirror (+ writer tail)
```

`bluedb-collections` is a **stateless translator** (one responsibility: MQL ⇄
bluedb), unit-testable in isolation. The `/collections/*` handlers in
`bluedb-server` are thin: authorize, resolve tenant, dispatch to the translator,
and run the result on the path it selects. Reused as-is: `bluedb-query`
(analytical context + DataFusion catalog/`SchemaProvider`), `bluedb-sql` (JSON
catalog, secondary indexes, composite-PK surrogate), and the durable write path.

The **read split is the existing one**: a point/indexed read (`_id`, or an
indexed JSON path) goes to the GlueSQL fast path (fresh, read-your-writes);
everything else (unindexed filter/sort, the aggregation pipeline) goes to
DataFusion over the Iceberg mirror plus the writer-local unsealed tail.

## 1. Collection & `_id` model

A collection `C` is a bluedb table `C(_id TEXT PRIMARY KEY, doc JSON)`. It is
created implicitly on first insert (MongoDB-style) or via an explicit
`createCollection`. The full document lives in the `doc` column; the `_id`
primary-key column is **maintained from `doc->>'_id'`** using the same
generated-surrogate pattern bluedb already uses for composite primary keys.

On insert, if a document has no `_id`, the gateway injects a generated
ObjectId-like 24-hex-character id before writing; an explicit `_id` is used as-is
and must be unique (enforced by the PK).

*Simplification (v1):* `_id` is stored and compared as text — covering string,
ObjectId, and number-rendered-as-text ids. Heterogeneous-typed `_id` values
(e.g. mixing int and string ids in one collection) are out of scope.

## 2. API surface (`/collections`)

The tenant header (`X-Bluedb-Tenant`) selects the **database** (consistent with
bluedb's cells model); the path segment is the **collection**.

| Method · path | MQL body | Result |
|---|---|---|
| `POST /collections/{c}/insert` | `{documents:[…]}` | `{insertedIds:[…], insertedCount}` |
| `POST /collections/{c}/find` | `{filter, projection, sort, limit, skip}` | `{documents:[…]}` |
| `POST /collections/{c}/update` | `{filter, update, multi, upsert}` | `{matchedCount, modifiedCount, upsertedId}` |
| `POST /collections/{c}/delete` | `{filter, multi}` | `{deletedCount}` |
| `POST /collections/{c}/aggregate` | `{pipeline:[…]}` | `{documents:[…]}` |
| `POST /collections/{c}/count` | `{filter}` | `{count}` |
| `POST /collections/{c}/createIndex` | `{keys, options}` | `{name}` |

Authorization reuses the existing bearer scopes: `data:read` for
`find`/`aggregate`/`count`, `data:write` for `insert`/`update`/`delete`,
`schema:admin` for `createIndex`. Writes require the active writer (single-writer
model). *v1 returns batch results — there are **no server-side cursors**; page
with `limit`/`skip`.*

## 3. `find` translation + routing

An MQL filter document translates to a bluedb predicate. A dotted field path
`a.b.c` becomes the JSON accessor chain `doc->'a'->'b'->>'c'`.

**Operators (v1):** `$eq $ne $gt $gte $lt $lte $in $nin $and $or $not $exists
$regex $elemMatch $type`. `projection` → a select of JSON paths; `sort` →
`ORDER BY`; `limit`/`skip` → `LIMIT`/`OFFSET`.

**Routing (the existing rule):** `find({_id: x})` is a primary-key lookup (the
fastest, freshest path); a filter on an **indexed** JSON path takes the GlueSQL
fast path (read-your-writes); anything else routes to the DataFusion analytical
scan. A read never fails for lack of an index — it is routed, not rejected.

## 4. Aggregation pipeline → DataFusion `LogicalPlan` (built directly)

An aggregation pipeline is an ordered list of stages, which maps almost 1:1 onto
DataFusion's **`DataFrame` API** (the idiomatic builder over `LogicalPlanBuilder`).
The translator resolves the collection as a DataFusion table through the existing
`BluedbSchemaProvider`/catalog, then folds the stages onto the resulting
`DataFrame` — `.filter()` · `.select()`/`.with_column()` · `.aggregate()` ·
`.sort()` · `.limit()` · `.join()` · unnest — reusing `analytical_context()` so the
JSON UDFs and the JSON/JSONB type planner are present, and `.collect()`s the
result. This is the one piece of **net-new execution infrastructure**:
`bluedb-query` runs only SQL strings today (`query_via_catalog` → `ctx.sql(...)`),
so the pipeline path adds an entry point that hands back a prepared
`SessionContext`/`DataFrame` with the tenant's collections registered, against
which the translator builds the plan directly.

| Stage | Plan op |
|---|---|
| `$match` | `.filter()` — same MQL→Expr translation as §3 |
| `$project` / `$addFields` / `$set` | `.project()` (incl. JSON-path extraction, common `$`-expressions) |
| `$group` | `.aggregate()` — `$sum $avg $min $max $count $first $last` |
| `$sort` | `.sort()` |
| `$limit` / `$skip` | `.limit()` |
| `$lookup` | `.join()` — equi-join `localField = foreignField` against another collection's scan |
| `$unwind` | the DataFusion unnest API on the array column |
| `$count` | `.aggregate()` (count) + `.project()` |

*v1 covers the stages above; `$facet` / `$graphLookup` / `$bucket` /
`$setWindowFields` are deferred.*

## 5. Writes & update operators

- **insert** → the existing durable single-writer path; returns the
  `X-Bluedb-Watermark` header. A multi-document insert batches into one
  transaction. Missing `_id`s are generated (§1).
- **update** with `$set` / `$inc` / `$unset` / `$push` / `$pull` → **read-modify-write
  in the gateway on the active writer**: fetch the matching documents, apply the
  operators to the JSON in-process, write them back. This is safe under bluedb's
  single-writer, no-lost-update guarantee. A native `jsonb_set` to do this
  in-engine is a later optimization, not v1.
- **upsert** → insert when the filter matches nothing.
- **delete** → translate the filter to a `DELETE`.
- Multi-document `update`/`delete` iterate within a bounded transaction.

## 6. Indexing (the minimal JSON-path foundation)

`createIndex({field: 1})` creates a JSON-path **expression index**: a hidden
generated column derived from `doc->>'field'` (the composite-PK-surrogate
pattern) plus an ordinary secondary index on it — accelerating equality and
range. `{unique: true}` makes it a unique secondary index. `_id` is always the
primary key and therefore always indexed.

*v1 is single-field.* Compound indexes (`{a:1, b:1}`) are deferred — they later
reuse the composite-surrogate trick. The richer **containment and full-text
indexes are the shared slice specced with sub-project B** (Elasticsearch-style
search); they are not built here.

## 7. Errors

Errors return a Mongo-shaped document `{ok: 0, code, codeName, errmsg}`, mapped
from bluedb's existing structured error codes (built for the `/tables` surface):
`UNIQUE_VIOLATION` → `DuplicateKey` (11000), `NOT_FOUND` → namespace-not-found,
`PARSE_ERROR`/`TYPE_MISMATCH` → a `FailedToParse`/`TypeMismatch` equivalent. An
unsupported operator or stage returns a clear `errmsg` naming what is not
supported (consistent with the "documented subset" decision) rather than
silently ignoring it.

## 8. Components & file layout

- **`crates/bluedb-collections`** (new): the pure translator — MQL filter → predicate /
  DataFusion `Expr`; update operators → in-process JSON mutation; pipeline →
  `LogicalPlan`. No I/O; unit-testable in isolation. Modules: `filter`,
  `update`, `pipeline`, `id` (ObjectId-like generation), `error` (Mongo error
  mapping), `model` (request/response shapes).
- **`crates/bluedb-server`**: thin `/collections/*` handlers + route registration;
  they authorize, resolve tenant, call `bluedb-collections`, and run the result
  on the GlueSQL fast path or via `bluedb-query`.
- **Reused unchanged:** `bluedb-query` (analytical context, catalog/SchemaProvider,
  plan execution), `bluedb-sql` (JSON catalog, secondary indexes, composite-PK
  surrogate), the durable write path.

## 9. Testing & the compatibility matrix

- **Unit (`bluedb-collections`):** filter → predicate/`Expr`; update-operator
  application to sample documents; pipeline → `LogicalPlan` (assert plan shape).
- **Integration (embedded server):** insert/find/update/delete/aggregate
  round-trips; `_id` generation and round-trip; routing (PK vs indexed-path vs
  DataFusion); upsert; unique-violation and not-found error codes.
- **Compatibility matrix:** a published doc page listing every supported operator
  and pipeline stage (and the notable unsupported ones) — the contract implied by
  the "familiar 80% subset" decision.

## Open questions / future work

- Compound and multikey indexes (via the composite-surrogate trick / array fan-out).
- Native `jsonb_set` for in-engine partial updates (replacing gateway RMW).
- Server-side cursors for large result sets.
- Sub-project **B**: the Elasticsearch-style text-search interface, sharing the
  containment + full-text indexes (tantivy) hinted at in §6.
