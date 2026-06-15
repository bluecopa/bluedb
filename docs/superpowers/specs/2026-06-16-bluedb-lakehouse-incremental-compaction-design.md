# bluedb lakehouse — incremental bin-packed compaction (design)

**Date:** 2026-06-16
**Status:** approved, ready to implement
**Branch:** `feat/lakehouse-incremental-compaction` (off `dev`, commit-only)
**Spike item:** #4 (last) of the lakehouse v1 limitations (after #1 namespaces,
#2 composite PK, #3 schema evolution — all merged).

## Problem

`LakehouseWriter::compact()` (`crates/bluedb-lakehouse/src/writer.rs`) rewrites
the **whole table** every run: it scans the fully-merged state through
iceberg-rust's reader and publishes a `Replace` snapshot containing only the
freshly-written files. It is correct and memory-bounded (streaming), but its
write amplification is **O(table size) per compaction cycle** — once a table is
large, accumulating a few small files triggers a rewrite of *everything*. We want
to rewrite only the small files (bin-packing), leaving large files untouched.

## The correctness crux: equality deletes + sequence numbers

Merge-on-read in our mirror is driven by **equality-delete files keyed on the
primary-key field-id**. An equality-delete with data-sequence-number `Sd` applies
to data files with sequence `< Sd` (strictly), which is exactly what makes upsert
work (a new row and its delete share a sequence, so the delete retires only
*prior* versions).

This makes naive bin-packing wrong: if you concatenate old, low-sequence data
files into a new file, that new file is committed at the **highest** sequence, so
older deletes (lower sequence) no longer apply to it → **deleted rows reappear**.
An incremental rewrite must therefore *materialize* the applicable deletes into
the cohort it rewrites, and must not disturb the sequence numbers of the files it
leaves in place.

## What iceberg-rust 0.9.1 does and does not allow (verified)

- ✅ `ManifestEntry::sequence_number() -> Option<i64>` (the **data** sequence
  number) is public — enough to reason about delete-applies-to ordering.
- ✅ `DataFile` exposes `file_path`, `file_size_in_bytes`, `record_count`,
  `content_type`, `equality_ids`, bounds, etc.
- ✅ Manifest enumeration is public (`load_manifest_list` → `load_manifest` →
  `entries()` with `is_alive()`, `content_type()`, `data_file()`).
- ❌ `TableScan` **cannot** be restricted to a subset of data files.
- ❌ There is **no public reader** for an equality-delete file's rows.
- ❌ `ManifestWriter::add_file(df, seq)` cannot pin an explicit sequence — a file
  re-added via `add_file` takes the new snapshot's sequence at commit. So
  survivors **cannot** be re-added by file; they must be carried forward inside
  their original manifest (which preserves their sequence).

The two ❌ scan/delete-reader walls rule out "scan a subset and hand-apply
deletes." The design routes around them.

## Design — scoped merge-on-read via a transient snapshot

Rewrite only a cohort of small files, reusing iceberg-rust's *own* merge-on-read
reader by pointing it at a throwaway snapshot that lists just the cohort:

1. **Select the cohort at per-seal data-manifest granularity.** Each seal writes
   one data manifest, so the live manifest list is a sequence of per-seal data
   manifests plus delete manifests. Choose the data manifests whose data files
   are **below the target size** (bin-pack candidates); never touch
   already-large files. Fewer than two candidates ⇒ no-op. Group candidates up to
   a target output size.
2. **Author a transient, in-memory snapshot** whose manifest list =
   `[cohort data manifests] + [all live delete manifests]`. Build a `Table` over
   that metadata and `scan().to_arrow()` it. Because the reader sees only the
   cohort's data files (plus the deletes), it applies the deletes — sequence-aware
   — to *just the cohort*, yielding the cohort's current rows. Stream them through
   the existing rolling writer → one (or few) compacted file(s) `Fnew`. **This
   reuses the tested merge-on-read path instead of hand-parsing delete files.**
3. **Commit an `Overwrite`** whose manifest list =
   `(parent manifest list − cohort data manifests) + [new Fnew data manifest]`.
   - Survivor data manifests and **all** delete manifests are carried forward
     **intact**, so their files keep their original sequence numbers.
   - `Fnew` is added fresh, taking the new (highest) sequence — correct, because
     the deletes that applied to its source rows were already materialized in
     step 2. Future deletes/upserts (higher sequence) still apply to `Fnew`
     normally.

This is correct under the merge-on-read invariant: materializing deletes into a
file and bumping its sequence to "now" is equivalent to "these rows are current
as of now," and everything not in the cohort is untouched.

### Why the leftover delete files are kept (and when they go)

Incremental compaction **keeps all delete files** — a delete may still apply to a
survivor data file, so it cannot be dropped safely in general. They are reclaimed
by the **major** compaction (below), which materializes the entire table and
emits a `Replace` with zero delete files (the existing whole-table path).

## Policy — minor (incremental) + periodic major (full)

Keep both compactions:

- **Minor = incremental bin-pack** (this design). The default, frequent
  compaction; bounds data-file count cheaply, leaves big files alone, never
  rewrites the whole table.
- **Major = whole-table rewrite** (the existing `compact`, renamed `compact_full`
  / kept as the `Replace` path). Run **occasionally** to reclaim accumulated
  delete files and fully re-bin-pack.

The compaction worker (`spawn_compaction_worker`) picks per run:
- run **major** when the live **delete-file count** exceeds a threshold (deletes
  have piled up and only major clears them), else
- run **minor** when the live data-file count exceeds `min_files`.

(Threshold knobs reuse the existing `BLUEDB_LAKEHOUSE_*` env style; the
delete-file threshold gets `BLUEDB_LAKEHOUSE_MAX_DELETE_FILES`, default chosen to
be conservative.)

## Target file size — via PRAGMA (durable, per-tenant)

The bin-pack target size is set with a **PRAGMA**, consistent with
`PRAGMA lakehouse_mirror`:

```sql
PRAGMA lakehouse_target_file_bytes = 134217728;   -- 128 MiB
```

- `LhPragma` (`crates/bluedb-sql/src/lakehouse.rs`) gains a
  `TargetFileBytes(u64)` variant; `parse_lakehouse_pragma` recognizes
  `lakehouse_target_file_bytes` (PRAGMA or SET form). A file is a bin-pack
  candidate when it is below ~½ the target; output rolls at the target.
- The server already intercepts lakehouse PRAGMAs (`lib.rs` →
  `manager.apply_pragma(tenant, pragma)`); the new variant routes the same way to
  the per-tenant engine.
- The setting is **durable per tenant** (persisted in the engine's `Registry`,
  restored on promote/failover), like the mirror flags. Default when unset comes
  from `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` (env), else a built-in constant
  (128 MiB).

## Memory bound

The minor compaction streams the cohort through iceberg-rust's reader into the
rolling writer, so peak memory ≈ one output file plus whatever delete key-set the
reader materializes for the cohort — the same shape as today's compaction, over a
**subset** of the table. The major compaction is unchanged (already
memory-bounded).

## Files

- `crates/bluedb-sql/src/lakehouse.rs` — `LhPragma::TargetFileBytes(u64)` +
  parse + unit tests.
- `crates/bluedb-lakehouse/src/writer.rs` — new `compact_incremental(target,
  watermark)`: cohort selection, transient scoped-snapshot scan, selective
  `Overwrite` commit; a `manifest_files()` helper to enumerate live manifest
  files split into data/delete; keep `compact` as the major `Replace` path
  (rename to `compact_full` for clarity, keep a thin `compact` alias if other
  callers exist).
- `crates/bluedb-lakehouse/src/engine.rs` — `compact_incremental(table)` +
  `delete_file_count(table)`; `Registry` gains `target_file_bytes: Option<u64>`;
  `set_target_file_bytes` (persist); `apply_pragma` handles the new variant;
  `spawn_compaction_worker` picks minor vs major.
- `crates/bluedb-lakehouse/src/manager.rs` — thread the per-tenant target +
  worker policy through the shared loop.
- `crates/bluedb-server/src/lib.rs` — the new PRAGMA already flows through the
  existing interception; add the delete-file threshold env knob.
- Tests: writer unit/integration for cohort selection + a transient-snapshot
  scoped scan + a full incremental round-trip read back through the iceberg
  reader (incl. an update/delete so an equality delete is materialized into the
  cohort and *stays applied* — the correctness assertion); engine policy test
  (minor vs major selection); PRAGMA parse + e2e.
- Docs: `docs/lakehouse/iceberg-mirror.md` (compaction section + the new PRAGMA +
  config table), drop the "compaction rewrites the whole table" limitation.

## De-risk spike (do first)

The load-bearing assumption is step 2: **scan a transient snapshot that
references a hand-picked manifest-list subset, with deletes applied.** Spike it
before building the rest:

> Build a small table with several data files and an equality delete, author a
> transient snapshot whose manifest list omits one data file, wrap it in a
> `Table`, scan it, and assert the reader (a) returns only the referenced data
> files' rows and (b) applies the delete. If iceberg-rust rejects a
> self-authored snapshot for read (e.g. requires the snapshot to be in
> `metadata.snapshots()` or a matching schema-id), adjust by adding the transient
> snapshot to a cloned metadata. If the approach proves unworkable, fall back to
> reading our own delete Parquet files directly (we authored them: single
> PK-field column) and filtering manually — heavier, but no new API needs.

## Scope / non-goals (v1)

- **In scope:** size-based bin-pack of small data files (minor), delete-file
  reclamation via periodic major, PRAGMA-set target size.
- **Out of scope:** partition-aware bin-packing (we don't partition); position
  deletes (we never produce them); cross-tenant or parallel compaction;
  cost-based scheduling. The major path remains the correctness backstop.

## Testing strategy

TDD throughout. Key tests:

1. **PRAGMA parse unit:** `PRAGMA lakehouse_target_file_bytes = N` (and `SET`
   form) → `LhPragma::TargetFileBytes(N)`; junk → `None`; existing mirror pragmas
   still parse.
2. **Spike — transient scoped scan:** as above; the de-risk gate.
3. **Cohort selection unit:** given manifests with mixed file sizes, the selector
   picks only sub-target data manifests and never large ones; <2 ⇒ no-op.
4. **Incremental round-trip (the correctness test):** seed a table over several
   seals (so multiple small data files + at least one update and one delete →
   equality deletes), run `compact_incremental`, read the table back through
   iceberg-rust's reader, and assert: file count dropped, large files untouched,
   and the merged contents are **unchanged** (updated row shows the new value,
   deleted row stays gone — proving deletes were materialized and not resurrected
   by the sequence bump).
5. **Major still reclaims deletes:** after incremental, a major compaction yields
   zero delete files and identical contents.
6. **Engine policy test:** worker chooses major when delete files exceed the
   threshold, else minor when data files exceed `min_files`.
7. **Server e2e + PRAGMA e2e:** target-size PRAGMA over `/sql` is durable; a
   write/seal/compact cycle keeps the REST-catalog-readable contents correct.
8. **Full workspace green; clippy clean on touched files; no `cargo fmt`.**

## Rollout / compatibility

No migration. Existing tables keep working; the major (whole-table) path is
unchanged and remains the default until enough small files accumulate to trigger
a minor run. Unset target size falls back to env/constant, so behavior is
well-defined out of the box.
