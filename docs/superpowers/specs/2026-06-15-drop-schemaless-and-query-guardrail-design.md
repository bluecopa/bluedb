# Decision — Drop schemaless tables; require schema + PK; online ALTER (field-ids); reject scan/sort queries

**Date:** 2026-06-15
**Status:** Decision + **spike implemented in `bluedb-sql`** (branch `worktree-spike+schema-regime`); see §6.
**Affects:** `bluedb-sql`, `bluedb-server` (DDL surface), `bluedb-sqltest`, `README`/docs, the lakehouse-mirror spec
**Supersedes:** the "schemaless ⇒ no DDL ⇒ no clearance" thesis in `README.md` and project memory (`project_common_storage_migration_clearance`)
**No backward-compat:** bluedb has never been deployed — there is no existing data and no on-disk-format or migration constraint. Formats can change freely; no migration tooling is needed.

## 1. Context

`bluedb-sql` (GlueSQL over SlateDB) today supports **schemaless tables**
(`column_defs == None`, rows stored as `DataRow::Map`). Schemaless rows:

- get an auto-increment `Key::I64` rowid via `append_data` (`storage.rs:691`);
- **cannot carry secondary indexes** — GlueSQL's `validate_index_expr` rejects
  index exprs on them (`storage.rs:516`);
- are therefore efficiently accessible only by the synthetic rowid. Every
  field predicate or `ORDER BY` on a schemaless table is a **full table scan +
  in-memory sort**.

That is incompatible with the performance stance below, and the
"schema-in-payload → no clearance" benefit schemaless was bought for is being
deliberately dropped.

## 2. Decisions

1. **Remove schemaless support entirely.** Reject column-less `CREATE TABLE` at
   the SQL/DDL surface (extend `precheck`); delete the `column_defs == None`
   paths (registry accept-anything at `registry.rs:91`/`:110`, the `DataRow::Map`
   handling). GlueSQL core keeps the capability; bluedb refuses to expose it.

2. **Every table requires a PRIMARY KEY.** Removes the auto-rowid `append_data`
   path (`storage.rs:691`). Guarantees a point/range access path on every table
   and a clean Iceberg identity column for the lakehouse mirror.

3. **Keep every read bounded — no unbounded full scan or in-memory sort.**
   Enforced at plan time — bluedb owns `Planner::plan` + the schema map, and the
   precedent exists (`pushdown::reject_cross_products`,
   `precheck::unsupported_reason`):
   - **No `WHERE` (a bare `SELECT * FROM t`)** → *not* rejected; auto-bounded to
     the first `UNFILTERED_SCAN_CAP` (= 100) rows in primary-key order, by
     injecting/clamping a `LIMIT`. Rows stream out of the store in PK order, so
     this is a bounded prefix scan, no sort. The safe-browse default. To read
     more, page with a PK predicate (`WHERE pk > cursor`), which is index-served
     and uncapped.
   - **`WHERE` on only non-indexed columns** → reject. A `LIMIT` does *not* bound
     it — the filter runs after the scan, so it can read the whole table to find
     even a handful of matches.
   - **`ORDER BY` not satisfiable by PK / index order** → reject (in-memory sort;
     a `LIMIT` doesn't make it cheap, and re-ordering by the PK would silently
     return the wrong rows).
   - **NOT overridable.** There is deliberately no client/PRAGMA/scope bypass —
     nobody can request an unbounded scan, by accident or injection. This binds
     **every** external surface, `/admin/sql` included. **Engine internals are
     exempt** because they go through the `Store` traits directly, not
     `Glue::execute` (`delete_schema` row collection, FTS seal drain, lakehouse
     reconciliation/seal scans, `GLUE_OBJECTS` introspection) — and that is where
     any future operator-grade scan/export/`INSERT…SELECT` rebuild must live.
   - Index/PK-served range scans of **any size** stay allowed; PK/index-bounded
     aggregation stays allowed. Whole-table analytics belongs in the warehouse
     via the Iceberg mirror.
   - **Actionable rejection (Firestore-style).** A rejection names the offending
     column(s) and hands back the exact DDL to fix it, e.g. `CREATE INDEX
     t_name ON t (name);` for a filter, or the same for an `ORDER BY` column — so
     the caller can copy-paste the index the query needs instead of guessing.

4. **Reposition: drop the "no DDL clearance" promise.** bluedb's pitch becomes
   object-storage-native + HA / Jepsen-proven + warehouse-federated OLTP — not
   "schemaless avoids migrations." Update the README architecture table + docs.

5. **ALTER TABLE = online schema evolution (field-ids + table-id indirection).**
   With schemas now mandatory, `ALTER TABLE` is the only evolution path. It
   already works today via GlueSQL's default `AlterTable` (empty
   `impl AlterTable for SlateDbStorage {}`, `storage.rs:1082`; exercised by the
   alter-table conformance tests) — but **eagerly**: the generic default rewrites
   every row through Store/StoreMut (O(n) re-encode + re-PUT per ALTER). Replace
   it with online metadata ops:
   - **Stable column field-ids.** Every column gets a monotonic, never-reused
     `field_id` in the schema record. Rows stay positional `DataRow::Vec`, but
     the read path reconstructs logical rows via a `field_id → storage-position`
     map. Then: **ADD COLUMN** = assign a new id + append (old rows read-pad
     NULL/default, O(1)); **RENAME COLUMN** = rename the id in the schema (O(1));
     **DROP COLUMN** = tombstone the id + skip on read (O(1) for rows) and drop
     its index entries (O(n) over that one index). ADD COLUMN `NOT NULL` without
     a default needs a default or an eager backfill.
   - **Stable table-id.** Keys key on a `table_id` instead of the table name
     (`<tenant>[TAG_DATA]<table_id><pk>`); a small catalog maps `name → id`. Then
     **RENAME TABLE** = update the name→id mapping only (O(1)) — no row/index
     re-key. Requires reworking `keyspace.rs`.
   - **Rejected — require explicit recreate + copy:** change/add PRIMARY KEY (the
     PK *is* the physical row key → full rebuild) and ALTER COLUMN TYPE (value
     re-encode). Return a clear error pointing at CREATE-new + `INSERT … SELECT`
     + DROP + RENAME (RENAME is now O(1), so this rebuild pattern is ergonomic).
   - **Iceberg alignment:** bluedb field-ids ↔ Iceberg field-ids, so each online
     ALTER maps 1:1 onto an Iceberg schema-evolution commit with **no data-file
     rewrite** on either side — the field-id work pays off in bluedb *and* the
     mirror.

## 3. Implementation surface (for the other session)

- `bluedb-sql`: `precheck.rs` (reject column-less `CREATE` + scan/sort queries),
  `registry.rs` (drop schemaless accept paths), `storage.rs` (drop
  `DataRow::Map` + auto-rowid `append_data`; require PK), a plan-time guardrail
  pass over the schema map's PK + index info (alongside `pushdown.rs`).
- `bluedb-sql` (ALTER): replace the empty `impl AlterTable` (`storage.rs:1082`)
  with online ops; add a per-column `field_id` to the persisted schema +
  a `field_id → position` map; rework `keyspace.rs` to key on a stable
  `table_id` with a `name → id` catalog; update `Store::scan_data`/`fetch_data`
  to reconstruct logical rows; `IndexMut` drop on DROP COLUMN.
- `bluedb-server`: `/schema` DDL must require columns + a PK; wire the guardrail
  override to the admin scope / a PRAGMA.
- `bluedb-sqltest` / `gluesql_suite`: exclude/adjust GlueSQL's schemaless
  conformance tests (currently part of the 205/205 storage suite).
- `README.md` + `docs/`: rewrite the schemaless / no-clearance framing.
- Lakehouse-mirror spec: simplifies — no `(rowid, JSON)` fallback; every table
  maps to a typed Iceberg schema.

## 4. Open items

- ~~Exact override mechanism for the guardrail (admin scope vs PRAGMA vs an
  `/* allow_scan */` hint).~~ **Resolved: there is NO override.** "Don't let
  anyone shoot themselves" — the guardrail is a hard invariant on every external
  surface (no scope/PRAGMA/flag bypass, `/admin/sql` included). A bare scan is
  auto-bounded to 100 rows instead of rejected, so usability doesn't need a
  bypass.
- Whether the guardrail also rejects whole-table aggregation / `GROUP BY`, or
  only access shape (default here: **access shape only**).
- ~~Migration story for existing schemaless data~~ — **N/A: never deployed, no
  existing data.**
- Field-id on-disk format, and whether rows written before an `ALTER` are
  read-padded forever or background-compacted. (Pure perf — *not* a compat
  question; intra-database old rows predate the column either way.)
- ~~Re-keying existing data when `keyspace.rs` switches to table-id keys~~ —
  **N/A: nothing deployed; every table is id-keyed from creation.**

## 6. Implementation status — spike (branch `worktree-spike+schema-regime`, 2026-06-15)

All four implemented + verified **in `bluedb-sql`** (full crate suite green:
**91 lib + 205 gluesql conformance + integration**, 0 failures, nothing excluded):

| Part | Where | Tests |
|------|-------|-------|
| **Scan/sort guardrail** | `guardrail.rs` + `plan()` hook gated by a `strict` connection | 9 unit + 4 integration |
| **Remove schemaless + require PK** | `schema_rules.rs` + `insert_schema`, gated by `strict` | 3 unit + 4 integration |
| **Online ALTER (field-ids)** | `colcat.rs` + `AlterTable` override + read/write translation | 6 unit + 3 integration + gluesql `alter_table_*`/`migrate` |
| **Table-id indirection + O(1) RENAME** | `keyspace.rs` (data/index keyed by `u64` id) + `rename_schema` | full conformance (id-keyed) + 2 rename integration |

**Key design refinement over §2:** the regime is enforced at the **user surface**
— a "strict" connection (`Database::connection_guarded()`), *not* ripped out of
the raw engine. So raw `SlateDbStorage` stays a faithful GlueSQL backend and the
conformance suite keeps its full value (205/205, **no tests excluded**). The
server should vend `connection_guarded()` for `/sql` and `/tables`;
admin/internal connections stay unguarded. The column catalog is **absent for
never-altered tables** (→ identity, zero translation), so existing rows are
untouched; it is written only on the first diverging ADD/DROP.

### Follow-ups

- ~~**`bluedb-ledger` does not compile until updated.**~~ **DONE.** Added
  `ProjectedTable::resolve_id(&Substrate, &Keyspace) -> Result<Option<u64>>`
  (bluedb-sql), which reads the `name → id` mapping straight off the substrate
  the way the engine does — so the `Ledger` (which holds a `Substrate`, not a
  `Database`) can resolve ids without a SQL connection. Both apply paths
  (`create_accounts`, `create_transfers`) resolve the projection table id(s)
  **once per batch** and thread them into `encode_row`. **Key semantic:**
  `resolve_id` returns `None` when the projection table was never created
  (`ensure_schema` not called) — the native apply still commits and the SQL
  mirror is simply skipped for that batch. This preserves the engine's previous
  tolerance (apply works without the projection tables; most ledger unit tests
  never call `ensure_schema`) while keeping the projection exact whenever the
  tables exist. Verified: full `bluedb-ledger` suite green (112 tests, incl. the
  three `sql_projection_*` round-trips; the transfers projection is table id 2,
  so a wrong/hardcoded id would fail the `WHERE id = …` selects).
- ~~**`bluedb-server`:** vend `connection_guarded()` for user routes; add a
  PRAGMA / admin-scope bypass.~~ **DONE.** Both `AppState::connection()` and
  `connection_serialized()` now vend guarded connections
  (`Database::connection_guarded` / new `connection_serialized_guarded`), so
  **every** route — `/sql`, `/tables`, `/schema/*`, and `/admin/sql` — runs the
  scan/sort guardrail + the no-schemaless/PK-required regime. **No bypass** was
  added (see §4). Engine-internal paths (ledger projection `ensure_schema`, FTS
  maintenance) use the `Database` directly, off the guarded surface, so they
  still scan as needed. Guardrail change: an unfiltered `SELECT` is **bounded**
  (capped to `UNFILTERED_SCAN_CAP = 100`, PK order) rather than rejected
  (`guardrail::bound_or_reject`); a non-indexed `WHERE`/`ORDER BY` is still
  rejected. The FTS/trigram empty-hit sentinel changed from `1 = 0` to an
  index-served `pk IN (NULL)` so the empty case stays within the guardrail.
  Verified: full workspace green (incl. 205 gluesql conformance, 7 guardrail
  integration, server `api`/`fts`/`schema`/`authz`/`http2`).
- **Reject PK/type ALTER:** dropping the PK column already fails on a strict
  connection (`insert_schema` re-enforces a PK after ALTER); GlueSQL has no
  ALTER COLUMN TYPE, so there is nothing else to reject.
- **Perf:** `table_id` costs one extra point read per data op — cache per
  connection. The table-id counter is read-increment-write — serialize under the
  write lease to harden against concurrent CREATEs.
