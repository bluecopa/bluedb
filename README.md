# bluedb

Object-storage-native data substrate, in Rust. Durable state lives in object
storage (S3 / GCS / Azure Blob) via **SlateDB**; compute is **stateless and
horizontally scalable**. Spun out of `fx-runtime` design work as its own project,
delivered as a standalone HTTP service (the integration surface; an earlier
PyO3-embedding path was dropped). It speaks a PostgREST-style data plane, raw
parameterized SQL, **SQL-integrated full-text search** (Postgres `@@`/`ts_rank`,
no separate search cluster), and a **TigerBeetle-style double-entry ledger** — all
over the one object-storage substrate.

## Why this exists

The driver is enterprise **DDL-clearance pain** → wanting **one schema-flexible
storage substrate** that is object-storage-native and tri-cloud (S3/ABS/GCS),
instead of per-table DDL migrations on Postgres. bluedb is the first concrete
slice of that substrate.

## Architecture (target)

| Layer | Choice |
|-------|--------|
| **Storage foundation** | SlateDB (LSM on object storage). Single-writer safety via SlateDB's `writer_epoch` + object-store compare-and-set fencing. |
| **Ingest** | Stateless producers → object storage → manifest-backed queue → single consumer (WarpStream / OpenData-`buffer` style). Decouples write throughput from the single writer. |
| **SQL** | GlueSQL over a SlateDB-backed `Store` (schemaless tables ⇒ no DDL, no migration, no clearance). Parameterized (`$N`) by construction — injection-proof. |
| **Full-text (BM25)** | tantivy + a **vendored copy of Quickwit's `quickwit-directories` read path** over object storage. Index = tantivy *splits* in object storage + our own manifest. **SQL-integrated** (Postgres `@@`/`ts_rank` rewritten to `pk IN (…)` against the index, plus trigram `LIKE`) with read-your-writes via an in-memory live segment sealed to durable splits — no separate search cluster. |
| **Ledger** | A **TigerBeetle-style double-entry ledger** (`bluedb-ledger`): typed `Account`/`Transfer` (u128), full TB result-code parity, two-phase transfers, applied inside the serialized writer as one atomic `WriteBatch`. A crash-consistent SQL projection makes balances queryable. |
| **HTTP surface** | A four-capability ladder (`bluedb-server`, axum, h1+h2c): data plane (`/tables`), parameterized SQL (`/sql`), structured DDL (`/schema/*`), and an off-by-default arbitrary-SQL admin escape hatch — with per-route bearer-token authz. |

## High availability

- **Intra-region:** K8s Lease leader election + SlateDB epoch fencing → RPO 0, automatic failover.
- **Multi-region:** active-passive, warm standby, RPO > 0. Gated promotion via a Temporal saga (runs in `fx-runtime`'s `fx_worker`). Arbiter = HA multi-AZ Postgres lease row (or human break-glass). Cross-bucket split-brain is prevented by the arbiter + "wait out the old lease", since CAS fencing stops at the bucket boundary.
- **For FTS specifically:** search needs no coordination or replication (each region reads its own object-storage replica); only the split-indexer follows the active-passive path.

## Crates

- `bluedb-storage` — the object-store **seam**: a minimal read-only `BlobStore` trait (async read-byte-range) + the additive `BlobStoreMut` write seam (put/delete/ordered scan-prefix), `SlateDbBlobStore` (slatedb 0.13) with lifecycle helpers, and `ChunkedBlobStore` (large values split across ordered keys). The vendored Quickwit `Storage` adapter sits on top of `BlobStore`.
- `bluedb-fts` — BM25 full-text search over object storage: `tantivy` + the vendored `quickwit-directories` read path, hosted on `bluedb-storage`. Indexer (docs→split), split manifest/catalog, multi-split merged search, lazy hotcache open (range-fetch, no whole-split load), the full **index lifecycle** (incremental append, generation-scoped logical deletes so a same-id update is a true in-place replace, merge/compaction that physically drops dead docs), a **GC executor** (delete superseded/orphaned splits + retention policies), a **compaction policy** (count/tombstone-ratio thresholds), **per-field analyzers** (keyword / stemming / whitespace, registered on lazily-opened splits), and **query niceties** (pagination, highlighting, FTS combined with structured filters).
- `bluedb-sql` — SQL over the substrate: GlueSQL `Store`/`StoreMut` on SlateDB. Order-preserving key encoding (schema/data/index namespaces, `Key::to_cmp_be_bytes()` keys), schemaless tables, CREATE/INSERT/SELECT/UPDATE/DELETE/ORDER BY, **real transactions** (overlay + `DbSnapshot` + atomic `WriteBatch`, snapshot isolation, true ROLLBACK), **secondary indexes** (`CREATE/DROP INDEX`, index-backed scans), enforced **uniqueness** (PK + `UNIQUE` columns), **tenant-namespaced** keyspace, a **schema-as-data registry**, and a **`Database`** handle vending write-serialized, snapshot-isolated **concurrent connections** (a shared write lease prevents lost updates).
- `bluedb-rest` — PostgREST-style query DSL → SQL translation (filters/operators/order/limit/offset + INSERT/UPDATE/DELETE), with identifier allow-listing and literal escaping. The input-table-v2 API-parity surface; self-contained (no dependency on `bluedb-sql`).
- `bluedb-engine` — the **facade** that composes the pillars: `rest_sql` runs a `bluedb-rest` DSL request end-to-end against `bluedb-sql` and `execute_sql` runs a parameterized single statement; `FtsIndex` is the durable full-text engine over one index (append/update/delete/search + a policy-driven compaction coordinator and background scheduler). The **SQL-integrated FTS** lives here too: `fts_sql` is the pre-parse rewrite that turns Postgres `to_tsvector(…) @@ *_tsquery(…)`/`ts_rank` (and trigram `LIKE`) into a `pk IN (…)` query gluesql can run; `LiveSegment` is the in-memory tantivy NRT tier; and `FtsEngine` maintains it from a SQL **commit tap** (read-your-writes), unions live ∪ durable splits at query time, seals live→durable in the background, and persists index definitions so they survive restart.
- `bluedb-ledger` — a **TigerBeetle-style double-entry ledger** over the substrate: typed `Account`/`Transfer` (u128 amounts), the full TB validation order and named result-code set, two-phase transfers (pending/post/void) with timeouts, linked chains, balancing/closing, and imported events. Each batch is applied inside bluedb's serialized writer and committed as **one atomic `WriteBatch`** (reusing the lease + epoch fencing + durable-before-ack). Canonical state is native postcard records; a **crash-consistent SQL projection** (dual-written into the same `WriteBatch`) makes accounts/balances queryable as ordinary tables.
- `bluedb-server` — the **HTTP/REST service** (axum, HTTP/1.1 + h2c) over `bluedb-engine` + `bluedb-ledger`, a four-surface capability ladder: `/tables/{table}` PostgREST-style CRUD; `/sql` one **parameterized** non-DDL statement (with `@@`/`ts_rank`/trigram FTS rewritten transparently); `/schema/*` structured DDL incl. `fulltext-indexes` + `trigram-indexes`; `/admin/sql` arbitrary SQL (off by default, audited); `/ledger/{accounts,transfers}` batched create + lookup; `/health` and `/admin/{status,promote,demote}`. Per-route **bearer-token authz** (open when unconfigured); writes gated to the active writer; the durable FTS engine + seal scheduler bind on promote. The integration surface — consumers talk to it over HTTP.
- `bluedb-ha` — **single-writer high availability**: a `LeaseProvider` seam (in-memory impl built; Postgres/K8s/NATS plug in) + a `WriterController` (promote/demote, background renewal, self-fencing within a safety margin, monotonic fencing-token `epoch` that composes with SlateDB's `writer_epoch`). Deterministically tested with an injectable clock.
- _(planned / deployment layer)_ a concrete shared `LeaseProvider`; cross-region replication + failover orchestration; `bluedb-buffer` (ingest).

## Provenance & licensing

bluedb's own code is **Apache-2.0**. `crates/bluedb-fts/vendor/` will hold a
minimal copy of **`quickwit-directories` + a thin `quickwit-storage` slice**,
copied from [`quickwit-oss/quickwit@main`](https://github.com/quickwit-oss/quickwit)
which is **Apache-2.0** (relicensed from AGPL after the Datadog acquisition).

- Per-file Apache-2.0 headers are preserved verbatim; see `crates/bluedb-fts/vendor/NOTICE`.
- **Do NOT** depend on the crates.io `quickwit-*` packages — they are stale
  `v0.3.0` (2022) and **AGPL**. Copy from `main` only.

## Status

**M1 (FTS), M2 (SQL), M3 (engine + HTTP service), and the M4 single-writer core
are complete**, and three follow-on tracks have landed on `dev`: the **HTTP
surface & write-path hardening** (Spec A), **SQL-integrated full-text search**
(Spec B), and the **double-entry ledger** (`bluedb-ledger`). The `bluedb-engine`
facade composes the pillars, `bluedb-server` exposes them over HTTP, and
`bluedb-ha` provides single-writer election + self-fencing with the service
gating writes to the active node. (Python/PyO3 embedding was dropped; the HTTP
service is the integration surface.) `cargo test --workspace` is green (380 tests) and
`cargo clippy --workspace --all-targets` is clean. Remaining is the M4
**deployment layer** (a concrete shared lease store + cross-region
replication/orchestration) and a couple of scoped follow-ups noted below.

- **HTTP surface & write path (Spec A):** the data plane is **parameterized by
  construction** (`bluedb-rest` emits `$N` placeholders + typed params; the
  engine binds via `execute_with_params`) — injection-proof. The raw endpoint is
  split into a four-surface ladder — `/sql` (one parameterized non-DDL
  statement), `/schema/*` (structured JSON → validated DDL), `/admin/sql`
  (arbitrary, off by default, audited) — with **per-route bearer-token authz**
  (open when unconfigured). `flush_interval` is a tunable knob
  (`BLUEDB_FLUSH_INTERVAL_MS`, default 25 ms); the listener speaks HTTP/1.1 + h2c.
- **SQL-integrated FTS (Spec B):** `CREATE FULLTEXT INDEX` on a text column, then
  query it through SQL — `to_tsvector('english', body) @@ plainto_tsquery($q)` +
  `ts_rank` over `/sql`, combinable with structured filters / `ORDER BY` /
  pagination. A pre-parse pass rewrites `@@`/`ts_rank` to `pk IN (…)`; a
  **commit tap** maintains an in-memory live segment for **read-your-writes**; a
  background **seal** folds live → durable tantivy splits; index definitions
  persist so they survive restart. Trigram indexes accelerate `col LIKE '%…%'`.
  (Regex `~` is the one designed-but-deferred slice.)
- **Ledger (`bluedb-ledger`, phases A–H):** TigerBeetle-parity double-entry
  engine (107 engine tests) + an atomic SQL projection + `/ledger/*` HTTP
  (batched create with per-item result codes, u128 as JSON strings). A Jepsen
  `ledger` workload exists but is **not yet validated on a live cluster against
  the current group-commit write path** — the outstanding ledger follow-up.

- **FTS (`bluedb-fts`):** vendored read path on the tantivy fork (`6270552`),
  `impl<B: BlobStore> Storage for B` bridge; a real **indexer**; a real **hotcache** +
  **lazy open** that range-fetches footer+hotcache and never loads the whole split
  (proven by a counting blob store: `get_all` == 0); a split **manifest/catalog**;
  **multi-split** merged BM25 search; and the full **index lifecycle** —
  incremental append, **generation-scoped tombstones** (a same-id update is a true
  in-place replace, proven end-to-end), and **merge/compaction** that physically
  drops dead docs and reports superseded splits for GC.
- **SQL (`bluedb-sql`):** GlueSQL `Store`/`StoreMut` over SlateDB
  (`gluesql-core =0.19.0`); order-preserving key encoding. **Real transactions** —
  write-buffer overlay + point-in-time `DbSnapshot` + atomic `WriteBatch` commit
  (snapshot isolation, read-your-own-writes, true ROLLBACK incl. index entries).
  **Secondary indexes** (`CREATE/DROP INDEX`, order-preserving prefix-free value
  encoding so string range/ORDER-BY scans sort by content not length, maintained
  through the txn overlay). **Tenant-namespaced** keyspace. A **schema-as-data
  registry** with write-time validation.
- **REST (`bluedb-rest`):** PostgREST-style DSL → SQL translation — filters/operators
  (`eq,neq,gt,gte,lt,lte,like,ilike,in,is`, `not.` negation), `select`/`order`/
  `limit`/`offset`, INSERT/UPDATE/DELETE; identifier allow-listing + literal escaping.
- **Storage (`bluedb-storage`):** `SlateDbBlobStore` over `slatedb` 0.13. **Durability
  proven across `Db` reopen**; `BlobStoreMut` write seam; lifecycle helpers;
  `ChunkedBlobStore` for large values.

See [ROADMAP.md](ROADMAP.md) for what's checked off and what remains.

**Next (M4 deployment layer):** the single-writer machinery + service gating are
done; what remains needs external systems — a concrete `LeaseProvider` (a
Postgres lease row / Kubernetes `Lease` / NATS KV) for real multi-node election,
object-store cross-region replication, and an operator/controller that drives
`/admin/promote`+`/admin/demote` for gated cross-region failover (+ a SlateDB
checkpoint recovery hook on promotion). Stateless readers need no coordination —
only the writer (and the FTS indexer/compactor) is the single writer the lease
fences.
