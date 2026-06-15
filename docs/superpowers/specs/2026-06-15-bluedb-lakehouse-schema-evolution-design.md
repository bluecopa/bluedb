# bluedb lakehouse — schema-evolution reconciliation into Iceberg (design)

**Date:** 2026-06-15
**Status:** approved, ready to implement
**Branch:** `feat/lakehouse-schema-evolution` (off `dev`, commit-only)
**Spike item:** #3 of the lakehouse v1 limitations (after #1 namespaces and #2
composite PK, both merged).

## Problem

When `ALTER TABLE` runs on a mirror-enabled table, the change is **not**
reconciled into the table's Iceberg mirror. Two facts conspire:

1. **Field-ids are positional.** `schema.rs::table_to_iceberg` assigns
   `field_id = col_idx + 1` from the *current* gluesql schema's column order
   (`crates/bluedb-lakehouse/src/schema.rs:66`).
2. **The Iceberg schema is frozen at first seal.** `LakehouseWriter::open`
   discards the freshly-computed schema for an existing table and reuses
   `metadata.current_schema()` (`crates/bluedb-lakehouse/src/writer.rs:126`).
   CDC carries only row-level changes — there is no DDL entry
   (`crates/bluedb-sql/src/cdc.rs`) — so the seal never learns a column changed.

The seal builds each Parquet `RecordBatch` by zipping the persisted Iceberg
fields to logical row positions (`writer.rs:241`, `row.get(col_idx)`). After an
`ALTER`, the logical row shape and the frozen schema diverge:

| ALTER | Today's mirror behavior |
|---|---|
| `ADD COLUMN` | gluesql appends the column; the frozen Iceberg schema never gains it — **the new column's data is silently dropped from the mirror.** |
| `DROP COLUMN` | the logical row loses a middle column and shifts left; the frozen schema still has it → **column/type misalignment, corrupt data written.** |
| `RENAME COLUMN` | the warehouse keeps the old name — a cosmetic but real mismatch. |

The docs currently tell users to avoid `ALTER` on a mirrored table. This spike
removes that limitation.

## Root cause and the key insight

Iceberg schema evolution is **field-id based**: a column keeps its field-id for
the life of the table; ADD allocates a new id, DROP retires an id, RENAME keeps
the id and changes the name. To reconcile we need a **stable per-column id** —
and bluedb already has one.

`ColumnCatalog` (`crates/bluedb-sql/src/colcat.rs`) maps each current logical
column to a **physical slot** that is stable and never reused:

- ADD COLUMN appends a slot at index `width`, bumps `width`.
- DROP COLUMN removes the slot from `slots` (the physical slot stays orphaned in
  stored rows, never reused).
- RENAME COLUMN touches neither slots nor catalog (name lives only in the schema).
- A never-altered table has **no catalog persisted** — the identity mapping
  (`slots[i] == i`) is implied.

So the slot is exactly the stable identity Iceberg needs.

## Design

### 1. Stable field-ids from colcat slots

Define the mapping `field_id(column) = slot + 1`.

- For a **never-altered table** the catalog is absent ⇒ identity ⇒ field-ids
  `1..N`. This is **byte-identical to today's positional scheme**, so every
  already-mirrored table keeps the same field-ids — no rewrite, no migration.
- After an `ALTER`, the catalog gives each surviving column its original slot, so
  its field-id is preserved; a newly-added column gets `slot + 1` where `slot`
  is freshly allocated above all existing slots.

Expose a thin accessor on the storage connection so the lakehouse never imports
the `ColumnCatalog` type:

```rust
// crates/bluedb-sql/src/storage.rs — alongside pk_columns
/// The stable physical slot of each current logical column (schema order), or
/// `None` for a never-altered table (identity: slot == position). The lakehouse
/// derives Iceberg field-ids as `slot + 1`, so a column keeps its field-id
/// across ADD/DROP/RENAME.
pub async fn column_slots(&self, table_name: &str) -> Result<Option<Vec<u32>>, SqlError> {
    Ok(self.read_catalog(table_name).await?.map(|c| c.slots))
}
```

`table_to_iceberg` takes an optional `&[u32]` slot vector and uses
`slot + 1` for each top-level field-id (and the PK field-id). `None` ⇒ identity
(unchanged behavior). Nested list/map ids start at `(max top-level field-id) + 1`
— `max(slots) + 1 + 1` for the slot case, `N + 1` for identity — so a slot-based
id can never collide with a nested id (and the identity case is byte-identical to
today's `next_nested_id = N + 1`).

PK and composite-key component columns cannot be dropped (we already reject
`UPDATE` of a key column; DROP of a key column is likewise rejected — see §4), so
`pk_field_id` and the sort-order field-ids stay stable across any allowed
`ALTER`. Equality deletes and the declared sort order keep working untouched.

### 2. Reconcile at seal time, self-authored

The reconciliation point is the writer, where the persisted Iceberg schema is
known. Add a desired-schema computation and a schema-update emission:

- **Compute the desired schema** from the current gluesql schema + slots.
- **Reconcile against the persisted schema** (`metadata.current_schema()`),
  reuse-by-id: for each desired top-level field whose id already exists in the
  persisted schema, **carry the persisted field forward** (preserving any nested
  list/map child ids) and only apply a rename if the name differs; allocate a
  fresh field (drawing nested ids above `metadata.last_column_id`) only for a
  genuinely new column id. Dropped columns simply do not appear in the desired
  schema. This makes the result an *evolution* of the persisted schema, not a
  recomputation — so unchanged columns (including their nested ids) are stable.
- **Emit the schema update** in `commit_internal`: if the desired schema differs
  from `metadata.current_schema()`, apply it through the metadata builder before
  adding the snapshot:

  ```rust
  builder = builder.add_current_schema(desired_schema)?;
  // ...then add the snapshot, stamped with the new current schema-id.
  ```

**De-risked.** iceberg-rust 0.9.1's
`TableMetadataBuilder::add_current_schema` (and `add_schema` /
`set_current_schema`) adds a new schema version preserving the field-ids we
provide, bumps `last_column_id` to the schema's highest field-id, and
**intentionally skips compatibility validation** — it documents "the builder does
not check if the added schema is compatible with the current schema"
(`table_metadata_builder.rs:640,729`). This is the same self-authored metadata
path we already use for `add_snapshot` / `set_ref`. No fork, no `unsafe`, no
`Catalog::update_table`.

The writer must re-derive its in-memory `self.schema` (and the Arrow schema used
for the next data file) from the evolved schema, so the data written in the same
snapshot matches.

### 3. Build the record batch by field-id, not position

`rows_to_record_batch` currently maps Arrow field index → `row.get(col_idx)`.
That breaks after a mid-table DROP. Change it to map **each Iceberg/Arrow field
to the logical column that currently carries its field-id**:

- The seal already hands the writer rows in **current logical order** (storage's
  `collect_rows` applies `catalog.to_logical`), and the logical column at
  position `i` has field-id `slots[i] + 1`.
- Build a `field_id → logical_position` lookup once per seal; for each Arrow
  field, read its `PARQUET:field_id` metadata and pull `row[lookup[field_id]]`.
- This keys the row→column placement on the same stable id as the schema, so any
  allowed `ALTER` stays aligned. The equality-delete batch (`keys_to_delete_batch`)
  already keys off `pk_field_id` and is unaffected.

### 4. Guardrails

- **DROP of a primary-key (or composite-key component) column is rejected** with
  a clear error — it would orphan the equality-delete identifier and the sort
  order. (Mirror of the existing UPDATE-of-key-column non-goal.) This is enforced
  in the bluedb-sql ALTER path so it holds regardless of mirroring, consistent
  with the key-column-immutability rule.

## Scope / non-goals (v1)

**In scope:** ADD COLUMN, DROP COLUMN, RENAME COLUMN of **scalar** columns,
reconciled into Iceberg with stable field-ids; RENAME TABLE already works
(table-id indirection, no field-id impact).

**Out of scope (documented, same spirit as the UPDATE-of-key-column non-goal):**

- **Type change** of an existing column (gluesql has no `ALTER … TYPE`; Iceberg
  type promotion is a separate concern).
- **ADD COLUMN of a complex (`LIST`/`MAP`) column** on an already-materialized
  table — nested-id allocation for a brand-new complex column mid-life is a
  follow-up; adding scalar columns covers the common case.
- **DROP of a key column** — rejected (see §4).

## Files

- `crates/bluedb-sql/src/storage.rs` — add `column_slots` accessor; reject DROP
  of a key column in the `AlterTable` DROP path.
- `crates/bluedb-lakehouse/src/schema.rs` — `table_to_iceberg` takes
  `slots: Option<&[u32]>`; slot-based top-level/PK field-ids; nested ids above
  `width`.
- `crates/bluedb-lakehouse/src/writer.rs` — desired-schema reconcile + reuse-by-id
  helper; `add_current_schema` emission in `commit_internal`; re-derive
  `self.schema`; `rows_to_record_batch` by field-id.
- `crates/bluedb-lakehouse/src/engine.rs` — fetch `column_slots` in `writer_for`
  and thread to `table_to_iceberg`; pass the slots so the writer can reconcile.
- Tests: a new `crates/bluedb-lakehouse/tests/schema_evolution.rs` (ADD/DROP/
  RENAME round-trips read back through the iceberg reader), schema.rs unit tests
  for slot-based ids, and an engine/HTTP e2e proving `ALTER` then a query in the
  warehouse view sees the evolved schema.
- Docs: `docs/lakehouse/iceberg-mirror.md` (remove the "avoid ALTER" limitation;
  add a "Schema evolution" subsection), `docs/concepts/architecture.md` (note the
  colcat-slot ↔ Iceberg-field-id link under online schema evolution).

## Testing strategy

TDD throughout. Key tests:

1. **schema.rs unit:** identity table ⇒ field-ids `1..N` (back-compat);
   slots `[0,2,3]` (middle dropped) ⇒ field-ids `[1,3,4]`; an added slot ⇒ its
   field-id is `slot+1` and above all others.
2. **writer reconcile unit:** persisted schema + a desired schema that adds /
   drops / renames a field ⇒ `add_current_schema` emitted, field-ids of surviving
   columns preserved, nested ids of unchanged complex columns preserved.
3. **lakehouse round-trip (`schema_evolution.rs`):** create+seal a table, then
   for each of ADD / DROP / RENAME: run the gluesql ALTER, write a row, seal,
   and read the table back through iceberg-rust's reader — assert the column set,
   names, and row values are correct (and the dropped column is gone, the added
   column present with its values, the renamed column under its new name).
4. **engine/HTTP e2e:** end-to-end through the seal loop; ALTER ADD then SELECT
   via the catalog/reader sees the new column.
5. **guardrail:** DROP of a PK / component column returns a clear error.
6. **Full workspace green**, clippy clean on touched files, no `cargo fmt`.

## Rollout / compatibility

No migration. Already-mirrored tables keep identical field-ids (identity case),
so the first seal after this change is a no-op schema-wise. The reconcile path
only fires when the desired schema genuinely differs from the persisted one.
