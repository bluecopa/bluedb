# DataFusion Front-Door Flip — Design

**Status:** design (spike-proven; full build not started)
**Date:** 2026-06-17
**Branch:** `spike/htap-unified-query` (spike code + this doc; commit-only)
**Phase:** Phase 3 of the [unified-query / HTAP design](2026-06-17-unified-query-layer-design.md). Subsumes the earlier "phase 1 (joins)" and "phase 2 (window functions)" asks.

---

## 1. Problem & decision

bluedb's read path is GlueSQL-first: every `SELECT` is planned by GlueSQL over SlateDB, and the scan/sort **guardrail** rejects non-indexed full scans (routing them to a side DataFusion-on-Iceberg path). GlueSQL cannot serve the two things grid/analytical clients most need:

- **Joins** — in-memory and limited; the guardrail rejects multi-table scans outright.
- **Window functions** — silently wrong (a correctness bug, not a rejection).

DataFusion does both natively and correctly. The decision: **make DataFusion the primary _read_ planner**, retiring GlueSQL's `SELECT` path. Writes and DDL stay on GlueSQL — DataFusion is read-only over our substrate, so "front door" always means **reads only**.

Doing this properly makes joins and window functions *fall out for free*: once every referenced table is a DataFusion `TableProvider`, multi-table planning, windows, CTEs, and subqueries all just work.

### Decisions (locked)

| Fork | Decision | Rationale |
|---|---|---|
| Aggressiveness | **Big-bang on `/sql`** — flip all `/sql` SELECTs at once, behind a flag, gated on a latency + correctness battery. | One dialect/path; no long-lived dual-routing classifier. |
| Surfaces | **`/sql` first; REST `GET /tables` stays the GlueSQL→SlateDB fast path.** | REST point/filter reads are already optimal; no reason to route them through Arrow. |
| Consistency | **Exact read-your-writes union** (not bounded staleness). | The HTAP promise; the spike proved it's cheap (stream bulk + tiny tail). |
| PK-less tables | **Error.** No Iceberg-only fallback. | The merge/pushdown key on the PK; aligns with the "require PK on every table" schema direction. |

Decided without a fork (reversible if challenged): the pushdown provider pushes **both PK and (eventually) secondary-index** predicates to the row store so only true scans hit Iceberg; DataFusion-dialect **conformance re-baseline is a follow-on**; **PG-wire** (`datafusion-postgres`) is a clean seam but deferred.

---

## 2. Architecture

```
            ┌──────────────────────── reads ────────────────────────┐
POST /sql ──┤ DataFusion SessionContext                              │
            │   └─ BluedbSchemaProvider  (resolves table names)      │
            │        └─ BluedbTableProvider (per table, per query)   │
            │             ├─ pk = <lit>  → row-store point read ─────┼──▶ SlateDB (fresh, OLTP)
            │             └─ else        → streaming RYW union ──────┼──▶ Iceberg (bulk, ≤ sealed_seq)
            │                                                        │    ∪ CDC tail (> sealed_seq)
GET /tables ┤ GlueSQL → SlateDB  (unchanged lean point/filter path) │
            └────────────────────────────────────────────────────────┘
writes / DDL ──▶ GlueSQL → SlateDB  (unchanged;  CDC mirror → Iceberg downstream)
```

The whole flip is a stack of three DataFusion-native components, all read-only, all per-query and logically stateless:

1. **`BluedbSchemaProvider`** (`datafusion::catalog::SchemaProvider`) — DataFusion calls `table(name).await` for every table a query references (joins, CTEs, subqueries included). It mints a `BluedbTableProvider` on demand. No per-query `register_table`; resolution is uniform.

2. **`BluedbTableProvider`** (`datafusion::catalog::TableProvider`) — one per table, the heart. Its `schema()` comes from the `ColumnCatalog` slot+1 field-ids, so the SlateDB and Iceberg schemas line up natively. It forks each `scan()` by predicate:
   - **`pk = <literal>`** → `supports_filters_pushdown` returns `Exact` (DataFusion drops the predicate); `scan()` does a **row-store point read** (`Store::fetch_data`) and **never opens Iceberg**. SlateDB is authoritative for any PK's current row, so a point read never needs the merge — this is the OLTP-latency gate.
   - **anything else** → the **streaming RYW union** below.

3. **The streaming RYW union** — built as a DataFusion plan, not a materialized batch:
   - **Bulk**: the sealed Iceberg snapshot streams from Parquet via `IcebergStaticTableProvider` (merge-on-read deletes already applied by the reader).
   - **Tail**: the unsealed CDC delta (`scan_cdc(sealed_seq)` → `collapse_lww`) as two small in-memory batches — the upsert rows and the set of touched keys.
   - **Merge**: `SELECT * FROM ice WHERE pk NOT IN (SELECT pk FROM tail_keys) UNION ALL SELECT * FROM tail_upserts`, then projection + limit. Only the small tail is in RAM; the columnar bulk streams. The `NOT IN` anti-join is key-type-agnostic, so the composite-PK `__bluedb_pk BYTEA` surrogate works with no special-casing.

### What changes / what stays

- **Removed on `/sql`:** the guardrail reject→route dance; GlueSQL no longer executes `/sql` SELECTs. Full streaming scans are survivable, so the guardrail's *reject* purpose is obsolete (a cost-based guardrail is a later option).
- **Retained:** GlueSQL for **all writes + DDL**; the **REST `GET /tables`** lean path; the composite-PK *encoding* (`pkcodec`) and the FTS `@@` rewrite (re-targeted to feed DataFusion — `@@` → `pk IN (…)` after tantivy).
- **Composes with in-flight work:** foyer (P3) caches the Iceberg file reads; the per-tenant CDC watermark (P1) drives freshness; R1 `NodeRegistry` + R2 HA-302 do the coordinator→writer redirect.

---

## 3. Consistency & freshness

SlateDB is the source of truth; Iceberg is a lagging columnar mirror. The union is a **performance** construction, not a correctness crutch — it reads the bulk columnar and tops up only the un-sealed tail.

- **PK reads** are exact by construction: SlateDB holds the authoritative current row, so a point read is always read-your-writes.
- **Scan/merge reads** are exact RYW on the writer: pin one Iceberg metadata read → take its snapshot *and* its `sealed_seq` (snapshot-summary property `bluedb.cdc_watermark`) → replay CDC strictly after that seq → anti-join + last-writer-wins. Insert/update/delete after the seal are all reflected without a re-seal.
- **Coordinators** (stateless, no fresh tail) serve Iceberg-only at the snapshot's `sealed_seq`. A client needing fresher carries `X-Bluedb-Min-Watermark`; `PRAGMA bluedb_read_wait_seal_n` decides wait-vs-**302 redirect to the writer** (via R1 registry + R2 HA-302). This is the existing P4 + HA mechanism; the front-door reuses it unchanged.

---

## 4. What the spike proved

All on `spike/htap-unified-query`, commit-only, TDD where a behavior was added. Tests in `crates/bluedb-query/tests/`.

| Claim de-risked | Evidence | Commit |
|---|---|---|
| Exact RYW union as a transparent provider; joins + windows fall out; PK-less errors | `unified_merge.rs` (ryw / union / join / window / pk-less) | `c97774f` |
| PK reads stay fast — point-get, **skip Iceberg** (`fast_path=1 / merge_path=0` for a post-seal id) | `pushdown.rs` | `8ee0036` |
| Merge **streams** the bulk (no whole-batch MemTable); **composite-PK** union works on the BYTEA surrogate | `unified_merge.rs::ryw_union_on_composite_pk_table` (RED→GREEN) | `52e6f5d` |
| **Auto table resolution** — a JOIN with no explicit `register_table`, tail freshness through the join | `schema_provider.rs` | `51914ec` |

Net: the two load-bearing risks (arbitrary SQL served fresh from a transparent union; PK reads stay fast) are retired, plus composite keys and idiomatic catalog resolution. What remains is engineering, not risk.

---

## 5. Remaining work to ship the full flip

1. **Secondary-index pushdown** — `supports_filters_pushdown` currently returns `Exact` only for the PK; secondary-indexed predicates fall to the merge path (a parity gap vs the old guardrail's "indexed → pass"). Push them to the SlateDB secondary index → PK → row, like the PK fast path.
2. **Composite-PK fast-path point-get** — encode `(a=?,b=?)` covering all components via `pkcodec` → `Key::Bytea(surrogate)` → point read. Today composite point reads correctly fall to the merge path (correct, not fast).
3. **`/sql` cutover wiring** — register `BluedbSchemaProvider` on the per-tenant SessionContext; route `POST /sql` `Query` to it; keep `Insert/Update/Delete` and DDL on GlueSQL; remove the guardrail reject→route path. Gate cutover on the battery in §6.
4. **FTS re-target** — `@@` rewrite feeds DataFusion (`pk IN (…)`) instead of GlueSQL.
5. **`SELECT *` on composite tables** — project out the `__bluedb_pk` surrogate (today it leaks on `SELECT *`; explicit column lists are fine).
6. **Conformance re-baseline** on the DataFusion read dialect; **PG-wire** (`datafusion-postgres`) as a thin front (deferred).

## 6. Cutover gate (battery before flipping `/sql`)

- **Latency:** a `WHERE pk = ?` read via DataFusion ≈ today's GlueSQL fast path (the pushdown provider, not an Iceberg scan).
- **Correctness:** the RYW union matches GlueSQL row-for-row across a query battery (filters, sorts, aggregates, joins, windows, composite keys, post-seal mutations).

---

## 7. Risks, cuts, deferred

- **Inner-context plan execution** — the streaming merge builds its plan in an inner `SessionContext` and returns it from `scan()`, executed under the outer task context. Validated by the integration tests (self-contained Iceberg FileIO + in-memory tail); revisit if a custom `MergeExec` is ever needed for performance.
- **CDC retention** must cover the unsealed tail (fast seal → short tail). Per-query CDC replay can be cached + invalidated on seal later.
- **Dialect divergence** — reads on the DataFusion dialect, writes/DDL on GlueSQL; conformance re-baseline is a follow-on.
- **`SELECT *` composite leak** — see §5.5.
- **PK-less tables** are rejected; depends on / reinforces the "require PK" schema direction.

---

## 8. Cutover status (built 2026-06-17)

The full flip is **built and merged into the spike branch**, full workspace green
(server 126 + all integration suites, query, lakehouse, the 205-test storage
conformance, ledger, evidence, fts, ha, cache). Commits: `query_via_catalog` +
non-mirrored branch (`2ca39b4`) → `/sql` cutover (`c71c4bc`) → composite
surrogate-hide (`634ecd5`).

What shipped beyond §2: `POST /sql` SELECTs route to DataFusion via
`BluedbSchemaProvider` (no GlueSQL fallback); writes/DDL/`GET /tables` unchanged;
FTS re-targeted through `rewrite_for` → DataFusion; the freshness gate preserved.
Correctness hardening surfaced by the battery: PK fast-path gated on the PK's
Arrow type (Int64/Utf8 only — decimal/u128/composite fall to the merge); the
merge gated on **mirror-enablement** (a non-mirrored table is served from the row
store via `current_record_batch`, since a bare writer handle can leave a spurious
empty snapshot); `build_arrow_column` extended to u64/u128/i128 → `decimal(p,0)`.

### Read-dialect changes (re-baselined)

Behaviors that changed on the **read** path, each accepted (DataFusion is more
capable / more standard); writes are unchanged:

- **Un-indexed filters / sorts / `LIKE` / joins / window functions / cartesian
  products are now served** (were rejected by the scan/sort guardrail or absent).
  The guardrail no longer gates `/sql` reads (it still applies on `GET /tables`).
  Test updated: `fts.rs::trigram_like_over_http_read_your_writes` (un-indexed
  `LIKE` now 200 + matching row, was 400).
- **Result order without `ORDER BY`**, **`NULL` ordering**, and **empty-match
  no-`GROUP BY` aggregates** now follow standard-SQL / analytical-engine
  semantics rather than the previous storage-order / no-row behaviors.
- User docs re-baselined: `docs/sql/{README,limitations,statements,
  query-guardrail,functions,expressions,query-syntax}.md`, `docs/index.md`,
  `docs/concepts/architecture.md`, `README.md`.

### Read-dialect conformance harness (built 2026-06-17)

A second sqllogictest backend, `DataFusionTester`, was added to `bluedb-sqltest`
alongside the existing `GlueTester`. It drives the **read front door** exactly as
the server does: a single top-level `SELECT` is planned + executed by DataFusion
over the `BluedbSchemaProvider`, while writes/DDL run on the GlueSQL path — the
same `is_read_query` split as `POST /sql`.

- **Gated corpus** — `crates/bluedb-sqltest/df/*.slt` (basic, windows, cte, joins,
  setops, aggregates, subquery, scalar, nulls, coercion) runs as a must-pass
  `cargo test` (`tests/datafusion_conformance.rs`). Every record is hand-verified
  against DataFusion semantics; the suite is green. This is the regression gate
  proving the analytical dialect GlueSQL cannot serve.
- **Coverage report** — the `conformance` binary gained `--engine glue|df`, so the
  broad external SQLite/DuckDB corpus (`fetch_corpus.sh`) can be measured through
  the DataFusion path too. On the 5 seed files the contrast is the flip itself:
  GlueSQL accepts 27/30 (window/CTE rejected), DataFusion accepts 30/30.
- **Divergence found + pinned** — `WHERE <decimal-col> = '9.99'` (decimal vs a
  *quoted* number) matches on the old GlueSQL coerce shim but **not** on the
  DataFusion front door (it coerces int-vs-string and decimal-vs-numeric, but not
  decimal-vs-quoted-string). Captured in `df/coercion.slt`. A narrow edge; whether
  to replicate the shim on the read path is a product decision, not a blocker.
- **Keyless tables (corpus-only)** — the product requires a PRIMARY KEY, but the
  external corpus assumes keyless tables. The require-PK rule is a server-side
  toggle (`schema_rules::enforce`) that is **off** on the harness's raw GlueSQL
  connection, so keyless `CREATE`/`INSERT` already land (GlueSQL keys rows
  internally, exposing only the user columns). Only the read needed a shim: a
  harness `CorpusSchemaProvider` resolves keyed tables through the real provider
  and reads keyless tables back from GlueSQL into an in-memory DataFusion table
  for the query. **No surrogate, nothing hidden, zero product change.** Proven by
  `df/keyless.slt` (bag semantics, aggregates, window functions, joins over
  keyless tables). This unblocks running the broad external corpus through `df`.

### Broad corpus run (DuckDB, 863 files / 7,494 records)

`fetch_corpus.sh` + `conformance --engine df crates/bluedb-sqltest/slt/corpus/duckdb`.

The **aggregate** accept-rate is a poor read-dialect metric: it scores `statement`
records (CREATE/INSERT/DDL) alongside queries, and those go through gluesql in
*both* backends. The corpus's `CREATE`s use length-parameterized / exotic type
decls (`VARCHAR(n)`, `HUGEINT`, `BIT`, `STRUCT`, `CREATE TYPE`) that gluesql's
translate rejects in both; add DuckDB-only statements (`EXPLAIN`, `DESCRIBE`,
`SUMMARIZE`, `PIVOT`) and negative `statement error` tests, and the write side
dominates. So `conformance` reports a **read-path-only** figure: score `query`
records, exclude **cascades** (queries downstream of a failed setup statement —
a write-path gap, not a read one). That isolates the read dialect:

| read path only (non-cascade queries) | DataFusion | GlueSQL |
|---|---|---|
| read-accepted | **34.5%** (1,244) | 27.1% (1,110) |
| PASS (correct of accepted) | **787 (63.3%)** | 581 (52.3%) |
| unsupported rejections | **584** | 1,832 |

Isolated to the read path, DataFusion is **ahead** — more queries accepted, higher
correctness, ~3× fewer `unsupported` (it serves windows / CTEs / set operations
GlueSQL refuses). The remaining read rejections are DuckDB-specific query syntax
and functions (`PIVOT`, `LIST`/`STRUCT` literals, the DuckDB function library),
not standard-SQL gaps — the DuckDB corpus is a DuckDB-idiom yardstick, so this is
a floor. (Aggregate, for reference: DataFusion 28.8% accept / 1,681 PASS vs
GlueSQL 33.8% / 1,983, both capped by the shared write-path ceiling.)

**Why not 100% on reads** (the 2,361 read rejections, from the query-only
`READ-PATH REJECTIONS` ranking): ~80% are DuckDB-only features DataFusion is
*correct* to reject — parser errors for DuckDB syntax (`MATERIALIZED` CTEs,
`PIVOT`, `SELECT * EXCLUDE/REPLACE`) = 777; the DuckDB function library
(`list_value`, `typeof`, `struct_*`, `histogram`, `array_value`); exotic types
(`HUGEINT`, `BLOB`, `GEOMETRY`); the `rowid` pseudo-column. ~115 are an EXPLAIN
routing artifact (routed to GlueSQL, which rejects it). **Only ~150 (~6%) are
genuine DataFusion v52 limits** worth tracking: some correlated `ScalarSubquery`/
`Exists` shapes don't decorrelate ("Physical plan does not support …"), `> ANY`
(only `= ANY`), `DISTINCT ON`, and a `simplify_expressions` optimizer hiccup. The
458 wrong-results are mostly result *ordering* (no `ORDER BY`) and formatting
(`true`/`false` vs `1`/`0`, float precision), not wrong answers. Net: 100% is
unreachable on a DuckDB-idiom corpus by construction; the genuine engine gaps are
a thin, named set, and the curated `df/` gate proves the mainstream dialect.

### Still deferred (flagged)

- **Secondary-index pushdown** — non-PK indexed predicates currently take the
  merge path (correct, not point-fast). Perf follow-up.
- **PG-wire** (`datafusion-postgres`) — out of scope.
- **HA-302 cross-node redirect** — a non-writer still 503s on a fresher-than-
  sealed read (no writer-URL resolution yet).
- **`result_large_err`** — pre-existing workspace-wide clippy lint (`AppError`
  size); not addressed (would need boxing `AppError`).
