# Lakehouse mirror — Iceberg CDC projection for warehouse joins

**Date:** 2026-06-15
**Status:** Design (awaiting review)
**Depends on:**
- [Spec A — HTTP surface split & write-path](2026-06-14-bluedb-http-write-path-design.md) (the control PRAGMA + catalog routes sit on the HTTP/DDL surface; the CDC tap sits on the commit path)
- [Spec B — SQL-integrated FTS](2026-06-14-bluedb-sql-integrated-fts-design.md) (the live-tier → background-seal → active-node-only pattern is copied wholesale)
- [Drop schemaless; require schema + PK; online ALTER; query guardrail](2026-06-15-drop-schemaless-and-query-guardrail-design.md) — **assumed baseline:** every table has a typed schema + PK; columns carry stable field-ids that align 1:1 with Iceberg field-ids; schemaless is removed; engine-internal scans (incl. the mirror's seal/backfill/reconcile) are exempt from the scan/sort guardrail.

## 1. Context & directive

Users need to **`JOIN` bluedb tables with their data warehouse** (Snowflake, Databricks,
BigQuery) — bluedb holds fresh operational facts, the warehouse holds the big history, and
the analytical join should run **in the warehouse** where the heavy compute lives.

bluedb's durable state is SlateDB SSTs (a private LSM format) — **not** warehouse-readable.
GlueSQL is a single-node embedded engine — **not** a federation engine. So the only sound
design is: bluedb continuously **publishes its tables to object storage in an open table
format the warehouses already read**, and the warehouse registers them and joins natively.
No separate ETL system, no second cluster — the same "on the same object storage" stance
as the FTS work.

**Directive (from the design dialogue):**
- Open format = **Apache Iceberg** (the one format all three warehouses read).
- **Full CRUD** mirror (INSERT/UPDATE/DELETE), minutes-fresh.
- **Opt-out, not opt-in:** every table is mirrored *by default*; a runtime PRAGMA sets the
  default and per-table overrides.
- **Typed schema + PK on every table** (schemaless removed per the baseline). **Complex/nested
  columns (list, map) map to native Iceberg nested types** — queryable as real nested columns
  in the warehouse, not opaque JSON.
- **Everything is v1** — nothing deferred. Hosted catalog and exactly-once both ship in the MVP.
- **One binary, no gates:** the mirror is always compiled into `bluedb-server` and on by
  default, toggled only at runtime by PRAGMA. No cargo features, no separate process.

### 1.1 Current state (verified)

- **Commit tap exists.** `bluedb-sql` exposes `CommitObserver::on_commit(&[RowChange])`
  (`crates/bluedb-sql/src/storage.rs`), `RowChange { table, key, row: Option<DataRow> }`
  — `Some(row)` = insert/update (the new row), `None` = delete; the PK is in `key`. Fired
  **after** the durable `WriteBatch` write, in-memory only, must stay cheap. Today only the
  FTS engine consumes it (in-memory, no durability).
- **Dual-write-into-the-same-WriteBatch is a proven pattern** — the ledger SQL projection
  already does it (`bluedb_sql::ProjectedTable`). The exactly-once CDC log uses the same
  primitive.
- **Collision-free sequence allocation is a proven pattern** — `SeqAllocator` (shared
  in-memory counter, seeded from committed max, re-derived after failover) gives concurrent
  group-committed writes distinct row keys. The CDC log seq reuses this exact pattern.
- **Multi-cloud object store already landed** (commit `289a607`: S3/Azure/GCS + emulator
  round-trip tests) — Iceberg data/metadata write through the same seam.
- **No Parquet / Iceberg / Arrow / Delta anywhere in the workspace today** — this is
  design + build, new crate `bluedb-lakehouse` (10th crate).

## 2. Locked decisions

1. **Format = Apache Iceberg v2.** Read natively by Snowflake (Iceberg tables), Databricks
   (Unity Catalog / UniForm), BigQuery (BigLake Iceberg). Delta and bare Parquet rejected
   (Databricks-first / no row-level deletes respectively).
2. **Merge-on-read writes; memory-bounded compaction; change-triggered seal.** A seal (fired
   by the commit tap, debounced ~1–5s — not a fixed long interval) writes a Parquet data file
   + an equality-delete file keyed by PK; a background worker compacts in **bin-packed,
   streaming, sort-merge units** — peak memory ≈ one target-sized output file, **never the
   whole table** (§5.1).
3. **Opt-out default via PRAGMA.** `PRAGMA lakehouse_mirror = on|off` sets the global
   default (ships `on`); a per-table override PRAGMA wins over the default. State persists
   in a durable registry blob.
4. **Exactly-once via a durable CDC log + watermark in the Iceberg snapshot summary.**
5. **bluedb hosts the Iceberg REST Catalog** (read-only to warehouses) at `/catalog/v1/*`.
6. **Typed schema + PK on every table** (baseline; schemaless removed). **Complex/nested
   columns (list, map) map to native Iceberg nested types** (`list`/`map`), recursively.
   **Column field-ids align 1:1 with Iceberg field-ids** → online ALTER is metadata-only on
   both sides (§7.1).
7. **Active-node-only**, bound on promote / aborted on demote — exactly like the durable
   FTS engine.
8. **Always built into `bluedb-server`**, controlled only at runtime. No cargo feature gate.

## 3. Architecture

```
        ┌─────────────────────────── bluedb-server (active node) ───────────────────────────┐
SQL ───▶│ commit path (bluedb-sql)                                                           │
write   │   • if table mirror-enabled: append CDC entry into the SAME WriteBatch             │
        │       key = TAG_CDC || global_seq (SeqAllocator-style),  val = postcard(change)    │
        │   • durable WriteBatch commit (data + CDC entry atomic, await_durable)             │
        │                                                                                    │
        │ LakehouseEngine (bluedb-lakehouse)                                                 │
        │   seal: commit-tap-triggered + debounced (~1-5s), skips idle tables:              │
        │     1. read CDC entries with seq > watermark                                       │
        │     2. last-write-wins per (table, pk)                                             │
        │     3. write Parquet data file + equality-delete file per table                    │
        │     4. commit ONE Iceberg snapshot; store watermark in snapshot summary            │
        │     5. GC CDC entries ≤ watermark                                                  │
        │   compaction: bin-packed streaming CoW, bounded memory, drops dead files          │
        │                                                                                    │
        │ Iceberg REST Catalog  /catalog/v1/*   (read-only: config/list/loadTable)           │
        └─────────────────────────────────────┬──────────────────────────────────────────┘
                                               ▼
                       object storage:  lakehouse/{tenant}/{table}/{metadata,data}
                                               ▲
            Snowflake / Databricks / BigQuery point at /catalog/v1, load the table,
            and run  SELECT ... FROM warehouse.fact f JOIN bluedb_cat.ns.t b ON ...
```

The mirror is a **crash-consistent projection**, never source of truth (same framing as
the ledger SQL projection). Canonical state stays in bluedb; Iceberg is derived and could
be rebuilt from a full scan if ever needed.

## 4. Crate layout

New crate **`bluedb-lakehouse`** (separate dir for Cargo build isolation; **no `#[cfg(feature)]`**):
- `cdc` — read/decode the durable CDC log, LWW collapse per `(table, pk)` per batch.
- `schema` — bluedb (SchemaRegistry) → Iceberg schema; scalar + complex/nested type mapping (§7).
- `writer` — Parquet data-file + **equality-delete-file** writers; **self-authors the Iceberg
  commit** (data/delete manifests via `ManifestWriterBuilder::build_v2_data`/`build_v2_deletes`
  → `ManifestListWriter` → `Snapshot` → `TableMetadata::into_builder()` apply `AddSnapshot`/
  `SetSnapshotRef` → serialize `metadata.json`); watermark in the snapshot summary. **Published
  iceberg-rust 0.9.1 only — no `Catalog::update_table`, no fork, no `unsafe`** (see §5.2).
- `compaction` — manifest-rewrite dropping superseded data/delete files (copy-on-write),
  dead-file GC; policy thresholds (port the FTS `policy` shape).
- `catalog` — owns the Iceberg table-metadata model + the **atomic metadata-pointer publish**
  the seal commits through, and serves the REST routes.
- `engine` — `LakehouseEngine`: owns the registry, the seal + compaction schedulers,
  watermark recovery; the type `bluedb-server` binds on promote.

`bluedb-sql` gains: a `TAG_CDC` keyspace, a CDC `SeqAllocator`, and a conditional CDC append
in the commit path gated on per-table mirror-enabled state (cheap in-memory set from the
registry). `bluedb-server` gains: the PRAGMA interception, the `/catalog/v1/*` routes, and
the promote/demote wiring.

## 5. CDC → Iceberg mapping (merge-on-read)

Per seal batch, for each `(table, pk)` keep only the **last** change (LWW), then:
- last change `Some(row)` (insert or update — the tap does not distinguish, and we do not
  need it to): **data file** gets the new row, **equality-delete file** gets the PK (so an
  update is delete-old-PK + add-new-row; a first-time insert's equality-delete is a no-op).
- last change `None` (delete): **equality-delete file** gets the PK only.

Equality deletes are keyed on the table's PK column(s). Readers (warehouses) apply deletes
at query time (merge-on-read); compaction later materializes copy-on-write so steady-state
read cost stays low.

### 5.2 Commit mechanism (LOCKED)

iceberg-rust's high-level transaction API is append-only, and its `TableCommit` is **not
externally constructible**, so the seal **self-authors the Iceberg commit** from public `spec`
types and publishes it through **bluedb's own hosted catalog** — never `Catalog::update_table`:

1. write the data + equality-delete Parquet files (`ParquetWriter` + the equality-delete writer);
2. data manifest via `ManifestWriterBuilder::build_v2_data`, delete manifest via `build_v2_deletes`;
3. `ManifestListWriter` → a `Snapshot` whose summary carries `bluedb.cdc_watermark`;
4. `TableMetadata::into_builder()` + apply `TableUpdate::AddSnapshot` / `SetSnapshotRef` → `build()`;
5. serialize `metadata.json` to the bucket; the **catalog atomically swaps the metadata pointer**.

This is the clean path the spike confirmed: **published iceberg-rust 0.9.1, no fork, no git-pin,
no `unsafe`** (moonlink needed an `unsafe` `TableCommit` transmute only because it commits
through *external* catalogs — we host ours). Compaction removals use the same flow with the
superseded files omitted from the new manifest list. Residual risk = hand-assembled metadata
correctness (sequence numbers, manifest-entry statuses, snapshot lineage); `TableMetadataBuilder`
does most of the bookkeeping and the round-trip + external-reader tests (§12) are the guard.

### 5.1 Freshness (event-driven seal) & memory-bounded compaction

**Seal is change-triggered, not interval-driven.** The commit tap notifies the seal loop;
it **debounces** (coalesces a burst, seals ~1–5s after the last change) and **skips idle
tables**. This drops the wait-for-tick latency to the object-store commit floor (~hundreds of
ms). `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS` (default 2000) + a `..._MAX_INTERVAL_MS` ceiling
bound it. Note: end-to-end "seconds" *also* requires the warehouse's fast metadata-refresh
mode (a deploy/ops setting, not code); Iceberg's snapshot model caps realistic end-to-end at
**~5–15s**. True ~1–2s (SLT-grade) needs a native streaming sink (§14, future).

**Compaction is memory-bounded by construction.** Frequent sealing makes many small data +
delete files, but peak compaction memory must **not** scale with table size or file count:
- **Bin-packed, one bounded unit at a time.** Compact a *bin* of small files up to a target
  output size (`BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES`, default 128 MiB), never the whole table.
  Peak working set ≈ one bin — independent of table size and total file count.
- **Streaming Arrow record-batches** in→out (read a row-group/batch, apply, write); a file is
  never fully materialized.
- **Sort-merge delete application.** Apply equality-deletes by a PK-ordered merge over the
  bin's rows + the relevant delete slice — **O(batch) memory, no full in-memory PK hash-set**
  (the thing that would otherwise grow with the delete backlog).
- **Single throttled background worker, off the hot path**, with a `max_inflight_bytes`
  budget capping total compaction memory.
- **Bounded backlog (backpressure).** Compaction must keep up with sealing; if a table's
  unmerged small-file/delete-file backlog exceeds `BLUEDB_LAKEHOUSE_MAX_BACKLOG`, the seal
  loop **slows that table's cadence** until compaction catches up — so the backlog (hence both
  warehouse read cost and the compaction working set) stays bounded.

## 6. Exactly-once: durable CDC log + watermark

The current tap is in-memory (lost on crash). For exactly-once across crash **and**
failover, the changelog becomes **durable and atomic with the data**:

- **Write:** when a mirror-enabled table commits, a CDC entry is appended **inside the same
  `WriteBatch`** as the data mutation — so a CDC entry exists **iff** the data committed
  (await_durable=true). Key = `TAG_CDC || seq`; `seq` from a global CDC `SeqAllocator`
  (distinct across concurrent group-committed batches, exactly like row-key allocation).
  Value = postcard(`{table, pk, row|tombstone}`). Tables opted out write **no** CDC entry.
- **Seal:** read entries `seq > watermark` (gaps are fine — read what's present), build one
  atomic Iceberg snapshot, write `watermark = max sealed seq` into the **Iceberg snapshot
  summary** (so the watermark advances atomically with the snapshot, never independently).
- **Recover:** on promote/restart, read the watermark from the latest snapshot summary and
  resume; re-seed the CDC `SeqAllocator` from the max persisted CDC seq (failover-safe like
  the existing allocator). GC deletes CDC entries `≤ watermark` after a successful seal.

No loss (entry is durable-with-data), no duplication (watermark only advances on snapshot
success; a re-seal of the same range produces the same idempotent snapshot content).

## 7. Type mapping

bluedb/GlueSQL `Value` → Iceberg type:

| bluedb | Iceberg |
|---|---|
| I8/I16/I32, U8/U16 | `int` |
| I64, U32 | `long` |
| U64 | `decimal(20,0)` (exceeds `long` range) |
| **U128** (ledger amounts) | **`decimal(38,0)` + mirror-time range guard** (full u128 = 39 digits; money never reaches 10³⁸) |
| F32 / F64 | `float` / `double` |
| Bool | `boolean` |
| Str / Text | `string` |
| Bytea | `binary` |
| Date / Time / Timestamp | `date` / `time` / `timestamp` (micros) |
| Decimal(p,s) | `decimal(p,s)` |
| Uuid | `string` |
| List | `list<E>` (native, recursive) |
| Map | `map<string, V>` (native, recursive) |

**Complex/nested columns.** GlueSQL `List`/`Map` carry no element/value subtype in DDL, so
the inner type (`E`/`V`) is resolved at seal time: homogeneous scalars → that scalar's
Iceberg type (rows above); nested `List`/`Map` → recurse (e.g. list-of-map →
`list<map<string,V>>`); an empty or heterogeneous level degrades **only that level** to
`string` (JSON-encoded items), keeping the column a valid typed Iceberg `list`/`map`. Once
written, the inner type is fixed in the Iceberg schema; later compatible widening (int→long)
uses Iceberg schema evolution, an incompatible inner change degrades that field to `string`.
Warehouses then query nested fields natively (Snowflake/Databricks/BigQuery all support
`array`/`map`/`struct`).

Column nullability comes from the schema registry; absent columns in a sparse row → null.

### 7.1 Schema evolution (ALTER TABLE)

The baseline makes online ALTER **field-id-based and metadata-only** in bluedb (ADD / RENAME /
DROP COLUMN, RENAME TABLE rewrite no rows), and **bluedb field-ids align 1:1 with Iceberg
field-ids**. The mirror consumes this directly: at each seal the `schema` module
**reconciles** the table's current bluedb schema (by field-id) against the mirrored Iceberg
schema and applies the difference as an Iceberg `UpdateSchema` / rename — a **metadata-only**
commit, applied *before* the data snapshot, no data-file rewrite on either side:

| bluedb ALTER | Iceberg action |
|---|---|
| ADD COLUMN | add column (new field-id; pre-existing rows read null / default) |
| DROP COLUMN | drop column (field-id retired) |
| RENAME COLUMN | rename (same field-id, new name) — lossless because it is by id |
| RENAME TABLE | catalog rename (registry mapping; data location unchanged) |
| DROP TABLE | drop the Iceberg table from the catalog (optionally GC files) |

Reconciling **by field-id** is what distinguishes a RENAME (same id) from a DROP+ADD (new
id). PK change / column retype are **rejected** in the baseline (recreate + copy), so the
mirror never sees them. The bluedb-side ALTER mechanism itself is owned by the
schema-direction spec; the mirror only consumes the field-id schema.

## 8. Opt-out default + PRAGMA control

- `PRAGMA lakehouse_mirror = on|off` — global default for tables **without** an explicit
  override. Ships `on` (opt-out).
- `PRAGMA lakehouse_mirror_table('<t>', on|off)` — per-table override; wins over the global
  default.
- Interception: the existing pre-parse SET/PRAGMA shim (from the conformance work) routes
  these to the `LakehouseEngine` registry instead of swallowing them.
- Persistence: a durable `lakehouse/_registry` JSON blob (global default + per-table
  overrides), mirroring the FTS `fts/_registry` pattern; reloaded on promote. The commit
  path consults an in-memory mirror-enabled set derived from it (cheap).

Every table has a typed schema + PK (baseline), so every table is mirrorable; the PRAGMA
only chooses whether it is.

Turning a table **on** later triggers an initial **backfill** seal (full scan → first
snapshot) before incremental CDC takes over; turning it **off** stops CDC writes and seals
(existing Iceberg data is left in place, optionally dropped). The backfill/seal full scans
run engine-internal and are **exempt from the scan/sort guardrail** (per the baseline).

## 9. Hosted Iceberg REST Catalog

`bluedb-server` serves the read-only subset of the Iceberg REST Catalog spec at
`/catalog/v1/*` so a warehouse points at bluedb **once** and auto-discovers tables + the
latest snapshot (no manual re-register per seal):
- `GET /v1/config` — catalog config + warehouse (bucket) root.
- `GET /v1/namespaces`, `GET /v1/namespaces/{ns}` — tenant → namespace.
- `GET /v1/namespaces/{ns}/tables` — list mirrored tables.
- `GET /v1/namespaces/{ns}/tables/{table}` — **loadTable**: current metadata location +
  metadata JSON.

Table **creation and commits are internal** (the seal loop writes metadata); the REST
surface is read-only to warehouses — bounding the spec surface we must implement. Per-route
bearer-token authz, consistent with the existing capability ladder. Read endpoints can be
served from any node (metadata lives in the bucket); only the seal writer is active-only.

## 10. Configuration

- `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS` — coalesce window after a change before sealing,
  default `2000` (~seconds-fresh).
- `BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS` — ceiling so a steady write stream still seals,
  default `10000`.
- `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` — compaction output / bin target, default `134217728`
  (128 MiB) — this is the knob that bounds peak compaction memory.
- `BLUEDB_LAKEHOUSE_MAX_BACKLOG` — per-table unmerged small-file/delete-file count before seal
  backpressure engages.
- Object store — reuse the existing multi-cloud S3/Azure/GCS config; data/metadata under
  `lakehouse/{tenant}/{table}/`.

## 11. HA / failover

Copy the durable-FTS wiring: `promote()` reloads the registry + watermark and starts the
seal + compaction schedulers + CDC-seq re-seed; `demote()` aborts them. Writes are
active-only (CDC append happens on the writer); the REST catalog's read endpoints are
node-agnostic. A standby that promotes resumes from the watermark in the last snapshot — no
double-publish, no gap.

## 12. Testing

- **Unit:** type mapping; LWW collapse per `(table, pk)`; CDC encode/decode; equality-delete
  emission; watermark monotonicity; registry default/override resolution.
- **Integration:** write via bluedb → seal → read the Iceberg table back with a reader
  (`iceberg-rust` reader and/or DuckDB's iceberg extension) → assert rows incl. updates &
  deletes; **restart/failover mid-stream → resume from watermark → exactly-once** (no lost,
  no duplicated rows); opt-out → no Iceberg table; opt-in-later → backfill then incremental.
- **REST catalog:** an Iceberg client (`iceberg-rust` / pyiceberg) does `loadTable` against
  bluedb and reads the data end-to-end.
- **Object-store emulator round-trip** using the existing harness.
- **Creds-gated real-warehouse smoke** (Snowflake/Databricks/BigQuery register + join) —
  outside CI.

## 13. Risks & mitigations

- **Self-authored Iceberg commit (RESOLVED by spike → now the locked mechanism, §5.2).**
  iceberg-rust can't commit deletes/removals via its public high-level API (`TableCommit`
  isn't constructible). We sidestep it entirely: self-author manifests + snapshot via public
  `spec` writers and publish via our own catalog — no `update_table`, no fork, no `unsafe`,
  on published 0.9.1. Residual risk = hand-assembled metadata correctness, guarded by the
  round-trip + external-reader tests.
- **Build cost** — `iceberg-rust` + `arrow` + `parquet` are heavy on a tree that already
  OOMs Docker on `--release` (the documented gotcha). No gate per directive; mitigate via
  the existing debug-build dev image + cargo cache mounts, build with the cluster stopped.
- **Compaction memory** — frequent sealing creates many small files; compaction must NOT load
  a table/partition into RAM (this tree already OOMs Docker). Mitigated by bin-packed,
  streaming, sort-merge compaction (peak ≈ one target-sized file) + a single throttled worker
  + seal backpressure when the backlog grows (§5.1). **Peak memory is independent of table
  size and file count** — that is the property to defend in review and tests.
- **Per-commit CDC overhead** — extra keys per mirrored row in every WriteBatch. Bounded
  (batched into the same group commit); opted-out tables pay nothing.
- **REST catalog scope** — implement only the read-only discovery/loadTable subset;
  writes stay internal.

## 14. Non-goals (bounded scope)

Not in this work: Delta or non-Iceberg formats; warehouse → bluedb write-back; cross-region
catalog federation; query-time pushdown from the warehouse into bluedb (the join runs in the
warehouse over published data, by design); time-travel beyond what Iceberg snapshots give
for free.

**Future work (not now):** a **native streaming sink** (Snowpipe Streaming / BigQuery Storage
Write API / Databricks streaming) for true ~1–2s SLT-grade freshness on hot tables, as an
alternative to the Iceberg path — at the cost of per-warehouse connectors and losing the
single-format simplicity. Also an **Arrow Flight SQL** egress for live columnar query of
bluedb itself (orthogonal to this mirror).

**Deploy requirement (not code):** end-to-end seconds needs the warehouse's **fast Iceberg
metadata-refresh** mode enabled (catalog auto-refresh / short metadata-cache staleness) —
document per warehouse in ops. The hosted REST catalog serves the latest snapshot instantly,
but the warehouse polls at its own cadence.

See [[project_bluedb]], [[project_bluedb_http_write_path]], [[project_bluedb_fts_sql_integrated]],
[[project_common_storage_migration_clearance]].
