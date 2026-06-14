# bluedb Spec B — increment B4 (part 1): durable tier + union searcher + seal

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Give the FTS index a durable tier so it isn't purely ephemeral. Today the live segment (B2a/B2c) is in-memory only. B4 adds: (1) a durable `bluedb_fts::FtsIndex` per fulltext index, over the **same** SlateDB substrate as the SQL data; (2) a **union searcher** — every `@@` query searches durable splits ∪ the live segment, with the live tier authoritative for any pk it covers; (3) a **seal** that folds the live segment into a durable split off the commit path. Restart-durability (persisting index defs + reopen + background scheduler) is **part 2** (B4-3, a follow-up plan).

**Why the seal is off-commit:** `FtsIndex::append`/`delete` are async (blob I/O) but `CommitObserver::on_commit` is sync (B2c). So the commit tap maintains only the in-memory live segment; the durable tier is advanced by an async seal. Between seals, the live segment is authoritative — it holds the latest version (or tombstone) of every pk it has touched, so the union searcher must **mask** any durable hit whose pk the live segment covers (Spec B §4.2/§5: live ∪ splits, live wins).

**Architecture:**
- Durable schema mirrors the live one via `bluedb_fts::mapping::IndexMapping`: a `keyword("id")` field (the pk, stored as a string → `IdField`) + a `text("body", analyzer)` field with the SAME analyzer as the live segment, so tokenization matches. The four analyzers (`raw`/`default`/`en_stem`/`whitespace`) are already in tantivy's default tokenizer manager, so opened splits need no extra registration.
- The union searcher passes an **identical, explicit-operator** query string to both tiers (so each tier's `QueryParser` default conjunction is irrelevant — see Task 2's `translate_query` change). Durable hits whose pk ∈ `live.covered()` are dropped; the survivors merge with the live hits by score.
- `seal` (async): drain the live segment's `(pk, text)` docs + tombstone set, `FtsIndex::update(pks, docs)` (re-add, superseding any prior durable copy) + `FtsIndex::delete(tombstoned pks)`, then reset the live segment (clear its index, tombstones, and covered set).

**Scope (this plan):** id-returning durable search; durable tier + union; seal; in-process tests. **NOT here (B4-3):** persisting fulltext-index *definitions* so they survive restart, reopening them on engine/server start, and the background seal scheduler. **NOT in B4 at all (deferred to HA-M4, per `fts.rs`'s own note "today one process owns the index"):** cross-node failover replay from the SQL watermark + MATCH gating. Same `INTEGER PRIMARY KEY` restriction as B2c.

**Tech stack:** Rust; `bluedb_fts` (`FtsIndex`, `IdField`, `mapping`, `multi_split_search_filtered`); `bluedb_storage::SlateDbBlobStore` (`from_substrate`); `bluedb_sql::Database::substrate()`.

---

## Task 1: `FtsIndex::search_ids` — durable search returning `(id, score)`

**Files:** `crates/bluedb-fts/src/search.rs` (new `multi_split_search_filtered_ids`), `crates/bluedb-engine/src/fts.rs` (`FtsIndex::search_ids`), tests in each.

The durable `FtsIndex::search` returns `MultiSplitHit` (score + doc address) and **discards** the stored id it already computes internally. The union searcher needs the id (the pk). Add an id-returning variant.

- [ ] **Failing test (bluedb-fts)** in `search.rs` tests (mirror the existing `multi_split_search_filtered` test harness — find it): build a 2-split in-RAM scenario (the existing tests show how to make `SplitHandle`s / `Index`es), assert `multi_split_search_filtered_ids(...) -> Vec<(String, f32)>` returns the right `(id, score)` pairs, tombstones excluded, last-write-wins dedup preserved.
- [ ] **Implement** `multi_split_search_filtered_ids` as a near-copy of `multi_split_search_filtered` (same over-fetch, tombstone filter, generation dedup) but whose kept result is `(id, score)` — it already computes `id` per `Candidate`; return `(candidate.id, candidate.hit.score)` for the kept set, sorted by descending score then stable tie-break, truncated to `limit`. Keep the original fn untouched (additive).
- [ ] **Failing test (engine)** in `crates/bluedb-engine/tests/fts.rs` (reuse `schema()`/`doc()`/`new_blob()`): append docs with ids `"2"`,`"5"`, then `index.search_ids("financial", &[body_f], 10).await` → `[("2", _), ("5", _)]` (or whatever matches), and a deleted id is absent.
- [ ] **Implement** `FtsIndex::search_ids(&self, query, fields, limit) -> Result<Vec<(String, f32)>>` in `fts.rs`: same body as `search` but call `multi_split_search_filtered_ids`.
- [ ] Run `cargo test -p bluedb-fts` + `cargo test -p bluedb-engine --test fts` (pass), `cargo build -p bluedb-fts -p bluedb-engine 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(fts): id-returning search (multi_split_search_filtered_ids + FtsIndex::search_ids)"
```

## Task 2: unify query translation to explicit operators

**Files:** `crates/bluedb-engine/src/live_segment.rs` (+ its tests).

So both tiers parse identically regardless of `QueryParser` default conjunction, `translate_query` must emit **explicit** operators (the durable `FtsIndex::search` path can't call `set_conjunction_by_default`).

- [ ] **Change** `translate_query`'s `Plain` arm: join the cleaned terms with `" AND "` (was space-joined relying on `set_conjunction_by_default`). `ToTsQuery` already emits explicit `AND`/`OR`/`-`. `Websearch` unchanged.
- [ ] Make `translate_query` `pub(crate)` (the union searcher in `fts_engine.rs` will reuse it for the durable query — one translation, both tiers).
- [ ] In `LiveSegment::search`, you may drop the `set_conjunction_by_default` special-case (operators are now explicit) — but leaving it is harmless; either way assert behavior is unchanged.
- [ ] **Update the two affected unit tests**: `translate_plain_strips_operators` now expects `"invoice AND overdue"` (both cases). The behavioral search tests (`plain_terms_conjoin_excluding_single_term_rows`, etc.) must still pass (AND semantics preserved).
- [ ] Run `cargo test -p bluedb-engine --lib live_segment` (pass). Commit:
```bash
git commit -am "refactor(engine): translate_query emits explicit AND so both FTS tiers parse identically"
```

## Task 3: LiveSegment seal-support — covered set + drain

**Files:** `crates/bluedb-engine/src/live_segment.rs` (+ tests).

- [ ] **Failing tests:**
  - `covered()` returns every pk passed to `index` or `tombstone` since construction/last drain.
  - `drain_for_seal()` returns `(Vec<(i64,String)>, HashSet<i64>)` = (live docs as `(pk, body)`, the tombstone set), then **resets**: a subsequent `search` returns nothing, `covered()` is empty, tombstones cleared. The drained docs contain the latest text per pk (post-update), and tombstoned pks are NOT in the docs vec (they're in the tombstone set).
- [ ] **Implement:**
  - Add `covered: Mutex<HashSet<i64>>`. `index(pk,..)` and `tombstone(pk)` insert `pk` into it. Add `pub fn covered(&self) -> HashSet<i64>` (clone under lock).
  - `pub fn drain_for_seal(&self) -> Result<(Vec<(i64, String)>, HashSet<i64>)>`:
    - `ensure_fresh()?` then read all live docs: a searcher over `tantivy::query::AllQuery` collecting every doc (use a collector that returns all addresses — e.g. `tantivy::collector::DocSetCollector`, or `TopDocs::with_limit(searcher.num_docs() as usize).order_by_score()`; pick what compiles in the fork). For each doc extract `pk` (`get_first(pk_field).as_i64()`) + `body` (`get_first(body_field).as_str()`). Skip any pk in the tombstone set (defensive).
    - Snapshot the tombstone set.
    - Reset: `writer.delete_all_documents()?; writer.commit()?; reader.reload()?;` clear `tombstones` and `covered`; set `dirty=false`.
    - Return `(docs, tombs)`.
  (`delete_all_documents` exists on the tantivy `IndexWriter`; verify the fork's exact name.)
- [ ] Run `cargo test -p bluedb-engine --lib live_segment` (pass). Commit:
```bash
git commit -am "feat(engine): LiveSegment seal-support — covered set + drain_for_seal (reset)"
```

## Task 4: durable tier in `FtsEngine` + union searcher + seal

**Files:** `crates/bluedb-engine/src/fts_engine.rs` (+ test `crates/bluedb-engine/tests/seal.rs`).

- [ ] **Failing end-to-end test** (`tests/seal.rs`, over a real `Database` — mirror `ryw.rs`):
```rust
// create docs(id INTEGER PRIMARY KEY, body TEXT); create fulltext index (english).
// FtsEngine::new_durable(database.substrate()) so it has a blob store.
// insert rows 1,2 (observed conn) -> live segment.
// @@ query -> sees row 1 (from live).      [pre-seal: union = live]
// fts.seal().await -> folds live into a durable split, resets live.
// @@ query -> STILL sees row 1 (now from the durable tier; live is empty).   [post-seal: union = durable]
// update row 1 body to no longer match, insert row 3 matching -> live segment.
// @@ query -> sees row 3, NOT row 1 (live covers pk 1 -> stale durable hit masked).
// delete row 3 -> @@ query empty for row 3; seal again -> still empty (durable tombstone).
```
- [ ] **Implement:**
  - `FtsEngine` gains an optional durable backing. Add `pub fn new_durable(substrate: bluedb_storage::Substrate) -> Arc<Self>` (keeps `new()` for the pure/in-memory case used by existing tests). Store `blob: Option<Arc<SlateDbBlobStore>>` (= `SlateDbBlobStore::from_substrate(substrate)` wrapped in Arc) on the engine. (Export `Substrate` from `bluedb_storage` if not already; `Database::substrate()` returns it.)
  - `IndexDef` gains `durable: Option<Arc<FtsIndex>>` and `body_field`/`id_field`/`analyzer` as needed. In `create_fulltext_index`, when `self.blob` is `Some`, build the durable index:
    ```rust
    let analyzer = analyzer_for_config(analyzer_str); // reuse live_segment's mapping
    let mapping = IndexMapping::new().keyword("id").text("body", analyzer);
    let schema = mapping.build_schema();
    let id_field = schema.get_field("id")?; let body_field = schema.get_field("body")?;
    let index_id = format!("fts/{table}/{text_column}");
    let durable = Arc::new(FtsIndex::new(index_id, blob.clone(), schema, IdField(id_field), CompactionPolicy::default()));
    ```
    Store `durable`, `body_field` (durable's), and remember the analyzer. (`analyzer_for_config` is in `live_segment.rs` — make it `pub(crate)`.)
  - **Union search** in `rewrite_for`: after picking the def, build hits from both tiers using the SAME translated query (`live_segment::translate_query(&pred.query, pred.kind)`):
    - live: `def.segment.search(&pred.query, pred.kind, LIMIT)` → `Vec<FtsHit>` (already filters its own tombstones).
    - durable (if `Some`): `durable.search_ids(&translated, &[durable_body_field], LIMIT).await?` → parse each id to `i64` → drop any pk in `def.segment.covered()` → `FtsHit{pk, score}`.
    - merge live + durable_filtered, sort by score desc, truncate to LIMIT, dedup not needed (covered filter guarantees disjoint pks). Pass the merged hits to the rewrite. **Refactor note:** `rewrite_fts_query` currently takes an `&impl FtsSearcher`. Easiest: build a small adapter `UnionSearcher { hits }` implementing `FtsSearcher` that just returns the precomputed merged hits, and pass it — OR add an internal rewrite entrypoint that accepts a `Vec<FtsHit>` directly. Pick the cleaner one; keep `rewrite_fts_query` working for the pure-live path (B2c tests).
  - **`pub async fn seal(&self) -> Result<()>`**: for each `(table, defs)` and each def with a `durable`: `let (docs, tombs) = def.segment.drain_for_seal()?;` build `TantivyDocument`s (`id = pk.to_string()`, `body`) → `durable.update(docs.iter().map(|(pk,_)| pk.to_string()), tantivy_docs).await?` (re-add, supersede); `durable.delete(tombs.iter().map(|pk| pk.to_string())).await?`. Hold the engine read lock only to snapshot the Arc handles, then do the async work without the lock held (clone `Arc<LiveSegment>`/`Arc<FtsIndex>` out first).
- [ ] Run `cargo test -p bluedb-engine` (ALL pass — `ryw.rs` still green: it uses `FtsEngine::new()` with no durable tier, so union = live only) + `cargo build -p bluedb-engine 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): durable FTS tier + union searcher (live wins) + seal (live -> split)"
```

## Self-Review
- Spec B coverage (B4 part 1): durable splits over the same object-storage substrate (§4.2) ✓; background-style seal off the commit path ✓; union searcher live ∪ splits with live authoritative (covered-mask) so RYW holds across a seal for insert/update/delete (§5) ✓; durable tombstones via `FtsIndex::delete` at seal ✓.
- Deferred to B4 part 2 (B4-3): persisting fulltext-index *definitions* (durable registry) + reopening them + the background seal scheduler — without these, sealed splits survive in the substrate but aren't reconnected after a process restart (the def map is in-memory). Deferred to HA-M4: cross-node failover replay from the SQL watermark + MATCH gating (consistent with `fts.rs`'s "one process owns the index today").
- Correctness hinge: the live `covered` set masks stale durable hits for any pk with a live version (updated or deleted) — verified by the update-after-seal and delete-after-seal cases. The explicit-AND translation (Task 2) keeps both tiers' parsing identical.
- Isolation: existing `ryw.rs`/`fts.rs` tests unaffected (`FtsEngine::new()` = no durable tier → union = live only; `FtsIndex` unchanged except the additive `search_ids`).
