# bluedb composite primary keys — design

**Status:** draft (spec)
**Date:** 2026-06-15
**Branch:** `feat/composite-primary-keys`
**Crates touched:** `bluedb-sql` (rewrite seam, planner, storage, keyspace), `bluedb-lakehouse` (mirror identifier + sort order), `bluedb-server` (none expected), docs.

## 1. Problem

bluedb tables can have only a **single-column** primary key, because the
embedded SQL engine (gluesql 0.19.0) cannot represent composite keys:

- `gluesql_core::data::Key` is a **scalar enum** — `Key::try_from(Value::List)`
  returns `ListTypeKeyNotSupported`.
- The insert executor keys each row on exactly one `is_primary` column:
  `column_defs.position(|c| c.unique == is_primary)` → `Key::try_from(one value)`
  (`executor/insert.rs`). No PK column ⇒ auto-increment `append_data`.
- A table-level `PRIMARY KEY (a, b)` constraint hits `translate_foreign_key`'s
  fallthrough → `TranslateError::UnsupportedConstraint`. It **errors outright**;
  only a single-column `PRIMARY KEY` *column option* is understood.

So `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))` fails today, and the
[lakehouse mirror](../../lakehouse/iceberg-mirror.md) — which keys merge-on-read
equality deletes on the PK — is limited to single-column keys.

A second, related defect surfaced while investigating: **bluedb does not push
PK *range* predicates into bounded scans.** gluesql executes
`IndexItem::PrimaryKey` as a point `fetch_data` (equality only); any PK range
(`id > 5`, `id BETWEEN`, keyset pagination `id > cursor`) falls to the
`_ => scan_data` arm (`executor/fetch.rs`), and bluedb's `scan_data` is
`collect_rows` — a **full table scan + in-memory filter**. The guardrail
currently *permits* PK ranges uncapped ("index-served reads are not capped"),
but that is optimistic: today they are O(table). Fixing this is a prerequisite
for composite-key range queries to be efficient, and independently valuable for
single-column PKs.

## 2. Goals / non-goals

**Goals**

- `CREATE TABLE … PRIMARY KEY (a, b, …)` works end to end (DDL, INSERT,
  SELECT/UPDATE/DELETE by full or **prefix** of the key, ORDER BY the key).
- Composite-key point lookups, prefix lookups, and ranges (`a = ?`,
  `a = ? AND b > ?`, `(a,b) > (?,?)`) execute as **bounded** scans — the same
  table Postgres's multicolumn B-tree gives you.
- Single-column PK ranges become bounded too (the fix above).
- The composite key is mirrored to Iceberg with the component columns as
  first-class, prunable columns.
- Zero behaviour change and zero overhead for existing single-column-PK tables.

**Non-goals (v1)**

- `UPDATE` that changes a PK **component** value (changes the row's identity →
  delete+reinsert). **Rejected** in v1 with a clear error (Postgres allows it;
  many engines don't; revisit later).
- `INSERT … SELECT` into a composite-PK table. **Rejected** in v1 (the surrogate
  key must be computed per row; the VALUES path covers the common case).
- Trailing-column-only predicates (`WHERE b = ?` with no constraint on `a`)
  being index-served — same limitation as Postgres; declare a secondary index
  on `b`. The guardrail behaviour is unchanged for these.
- A gluesql fork. Everything rides the existing bluedb-sql rewrite/planner seams.

## 3. Approach

Two independent pieces that compose:

1. **PK range pushdown** (foundation, also fixes single-column PKs) — make PK
   range/prefix predicates execute as bounded scans over the data keyspace.
2. **Composite PK = surrogate-key shim** — present `PRIMARY KEY (a,b)` to users,
   map it to a single hidden `__bluedb_pk BYTEA PRIMARY KEY` column gluesql
   treats as an ordinary scalar PK, derived from the components by an
   order-preserving, self-terminating concatenation so byte order == tuple
   order. Composite-key prefix/range predicates rewrite to `__bluedb_pk` ranges,
   which ride piece (1).

No gluesql fork; no new storage primitives (the byte encoding already exists for
secondary indexes).

## 4. PK range pushdown (foundation)

### 4.1 Mechanism

Row keys are `data_prefix(table_id) ‖ pk.to_cmp_be_bytes()` — byte-ordered by
the primary key. So a PK range *is* a bounded `slatedb` range scan; bluedb
already has `scan_range`. The only missing piece is getting gluesql to invoke a
bounded scan, which its `Store::scan_data(table)` (no predicate) can't express.

We reuse gluesql's **secondary-index** path, which *is* bounded
(`Index::scan_indexed_data` already computes Eq/Lt/LtEq/Gt/GtEq byte ranges):

- The executor's `NonClustered` arm calls `storage.scan_indexed_data(name, …)`
  and **never re-validates that the index exists** (`executor/fetch.rs`) — it
  trusts the planner's `IndexItem`.
- `plan_index` emits `NonClustered { name, cmp_expr }` for any
  `Gt/Lt/GtEq/LtEq/Eq` predicate on a column that has an index in
  `schema_map[table].indexes` (`plan/index.rs`).

So, in `SlateDbStorage::plan` (`storage.rs`), after `fetch_schema_map` and
*after* the guardrail pass, inject a **synthetic pseudo-index** on the PK column
into the in-memory schema map:

```text
SchemaIndex { name: PK_PSEUDO_INDEX ("__bluedb_pk"), expr: <pk column ident>,
              order: Both, created: <epoch> }
```

- It is **never persisted** and never appears in `Schema::indexes` on disk.
- `plan_primary_key` runs first and still converts PK **equality** to a point
  `IndexItem::PrimaryKey` (optimal — direct `fetch_data`). `plan_index` only
  sets an index when none is set, so it picks up PK **ranges** only, emitting
  `NonClustered { name: PK_PSEUDO_INDEX, … }`.

Then in `SlateDbStorage::scan_indexed_data`, special-case
`index_name == PK_PSEUDO_INDEX`: scan the **data prefix** directly with the
value-bounded range derived from `cmp_value` (the data *is* the clustered PK
index — no `TAG_INDEX` entries, no pk→row resolution). Reuse the existing
Eq/Lt/LtEq/Gt/GtEq bound construction, but build bounds on the **data** key
(`data_prefix ‖ key.to_cmp_be_bytes()`) instead of an index-entry key, and emit
`(Key, DataRow)` straight from the scan. Honour `asc` (reverse for DESC).

### 4.2 Properties

- Single-column PK ranges (`id > 5`, `id BETWEEN x AND y`, `id >= cursor`
  keyset pagination) are now bounded scans; the guardrail's "PK ranges are
  bounded" becomes true.
- Equality is unchanged (still the point `fetch_data` fast path).
- No behaviour change for non-PK-range queries: the pseudo-index is only
  consulted for range predicates on the PK column that `plan_primary_key`
  didn't already consume.
- `PK_PSEUDO_INDEX` is reserved: `CREATE INDEX` rejects that name.

## 5. Composite key encoding

`__bluedb_pk = concat_i( push_order_preserving( component_i.to_cmp_be_bytes() ) )`

- `Key::to_cmp_be_bytes` (gluesql) — per-value big-endian, order-preserving.
- `push_order_preserving` (already in `keyspace.rs` for index-entry values) —
  escapes every `0x00` to `0x00 0xFF`, terminates the segment with `0x00 0x00`.
  Order-preserving **and** prefix-free.
- Concatenating these yields: byte-lex order of `__bluedb_pk` == lexicographic
  tuple order of `(a, b, …)`, and unambiguous component boundaries (a prefix on
  the leading components is exact — `a = 5` can't bleed into `a = 50`, even for
  variable-length `a`). This is the FoundationDB-tuple / CockroachDB
  key-encoding pattern.

Every component is escaped+terminated — including the last (unlike a row's
trailing single PK today, which needs no terminator because nothing follows it).

`NULL` in any component is rejected at insert (a PK component must be NOT NULL,
which the DDL rewrite enforces).

Lives in a new `bluedb-sql` module `pkcodec.rs`:
`encode_composite_key(components: &[Key]) -> Vec<u8>`.

## 6. Per-table PK catalog

A small record per composite-PK table, so the shim can compute `__bluedb_pk` on
insert, rewrite predicates, hide `__bluedb_pk` from results, and let the mirror
expose components:

```rust
struct PkCatalog { columns: Vec<String> }   // the user PK columns, in order
```

Persisted under a new keyspace tag `TAG_PKCAT` (mirrors the `colcat` pattern:
exact-key lookup per table, never prefix-scanned). **Absent** for single-column
PK tables — their presence/absence is the switch that makes the whole shim a
no-op for existing tables.

## 7. DDL rewrite (CREATE TABLE)

At the sqlparser-AST level (like `rewrite.rs`), before handing SQL to gluesql:

1. Detect a table-level `PRIMARY KEY (a, b, …)` constraint with ≥2 columns.
2. Validate: every component column exists, is scalar, and is forced `NOT NULL`.
   Reject if any component is named with the reserved prefix `__bluedb_`.
3. Remove the constraint; prepend a synthetic column
   `__bluedb_pk BYTEA NOT NULL PRIMARY KEY`.
4. Persist `PkCatalog { [a, b, …] }`.
5. Hand the rewritten (single-column-PK) `CREATE TABLE` to gluesql.

A single-column `PRIMARY KEY` (column option or one-column table constraint) is
left untouched.

## 8. DML rewrites

Driven by the presence of a `PkCatalog` for the target table.

- **INSERT … VALUES**: for each row, evaluate the component columns, compute
  `__bluedb_pk`, and inject it as a leading column+value. Reject NULL/missing
  components. `INSERT … SELECT` is rejected (non-goal v1).
- **SELECT**:
  - *Projection hiding*: `SELECT *` is expanded to the explicit user-column
    list (reusing `projection.rs`) so `__bluedb_pk` never surfaces. Explicit
    column lists simply don't name it.
  - *Predicate rewrite* — translate predicates on a **prefix** of the key
    columns into a `__bluedb_pk` predicate that rides §4 pushdown:
    - full key `a = ? AND b = ?` → `__bluedb_pk = enc(a,b)` (point lookup),
    - leading prefix `a = ?` (optionally `AND b <op> ?`) → `__bluedb_pk`
      range `[lo, hi)`,
    - row-value `(a,b) > (?,?)` (keyset pagination) → `__bluedb_pk > enc(?,?)`.
  - Predicates not expressible as a key prefix are left as-is (the component
    columns still exist in the row; gluesql filters on them).
  - `ORDER BY a, b` (or a leading prefix) → `ORDER BY __bluedb_pk`.
- **UPDATE / DELETE**: rewrite the `WHERE` as for SELECT. `UPDATE` that assigns
  a PK component is rejected (non-goal v1); `UPDATE` of non-PK columns is fine.

## 9. Lakehouse / Iceberg integration

The mirror sees a normal single-column PK (`__bluedb_pk`). Two variants:

- **Variant A (default, v1):** identifier field = `__bluedb_pk`; equality deletes
  are single-column (the already-proven path — no new iceberg-rust risk). The
  data files carry `__bluedb_pk` **plus** the component columns `a, b, …` as
  ordinary columns. Warehouses join/filter on `a, b` (real columns with min/max
  stats → file pruning); `__bluedb_pk` is an internal binary column they ignore.
- **Variant B (optional, gated):** identifier = `(a, b)` (multi-column equality
  deletes); `__bluedb_pk` not mirrored. Cleaner Iceberg schema, but depends on
  iceberg-rust 0.9.1's reader applying **multi-column** equality deletes — the
  gating spike. Adopt only if that spike is green.

Either way, emit an Iceberg **sort order on the component columns** so seal-time
data files cluster by `(a, b)` → strong warehouse file-pruning for
`a = ? AND b > ?`. We control the sort at seal time (sort collapsed rows before
writing). Warehouse range-pruning on `a, b` works in **both** variants because
`a, b` are real columns regardless of the identifier choice.

The lakehouse derives the PK column set from the `PkCatalog` via a thin
`bluedb-sql` connection accessor.

## 10. Phases

1. **PK range pushdown** (§4) — `pkcodec` not needed; pure planner+storage.
   Independently shippable; fixes single-column PK ranges. *(Spike target.)*
2. **Encoding + catalog + DDL + INSERT** (§5–7, §8 INSERT) — composite create &
   write; rows land and round-trip; point lookup by full key works.
3. **SELECT/UPDATE/DELETE predicate rewrite + projection hiding** (§8).
4. **Lakehouse variant A + sort order** (§9).
5. **Gating spike + variant B** (§9) — multi-column equality deletes; adopt if
   green.
6. **Docs + e2e** (REST, SQL reference, lakehouse page; remove the
   "single-column primary key" limitation).

## 11. Risks / spike

The load-bearing risks, retired by the spike on this branch:

- **R1 — encoding order.** A multi-component order-preserving, self-terminating
  concatenation must sort tuples lexicographically, including mixed
  fixed/variable-length components. → unit test on `encode_composite_key`.
- **R2 — PK range pushdown.** The synthetic-pseudo-index → bounded
  `scan_indexed_data` mechanism must yield correct, ordered results for a PK
  range, with equality still using the point path and other queries unaffected.
  → integration test: `WHERE id > N`, `BETWEEN`, keyset `id > cursor`, `ORDER BY
  id` return correct ordered rows; equality unaffected.

R2 is the architecturally novel risk; R1 is mostly mechanical (the byte scheme
already exists). The gating spike for variant B (§9) is deferred to Phase 5.

## 12. Testing strategy

- `pkcodec` unit tests: ordering across int/text/mixed tuples; prefix-free
  boundaries; round-trip is **not** required (the key is opaque; components are
  stored as their own columns).
- Planner/storage tests: PK range → bounded scan (correct rows + order);
  equality still point; non-PK-range queries unchanged; DESC.
- Rewrite tests (per phase): DDL strips constraint + adds `__bluedb_pk`; INSERT
  injects key; predicate rewrites for full/prefix/range/keyset; `SELECT *` hides
  `__bluedb_pk`; rejected non-goals error cleanly.
- Lakehouse: composite-PK table mirrors; component columns present + prunable;
  merge-on-read correctness via iceberg-rust's own reader.
- No `cargo fmt` (repo convention); match style by hand; `cargo clippy` is fine.
