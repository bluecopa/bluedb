# bluedb Spec B — increment B4 part 2 (B4-3): durable registry + reopen + seal scheduler

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Make the durable FTS tier survive a restart and self-maintain. B4 part 1 gave durable splits + union + seal, but the fulltext-index *definitions* (which table/column/analyzer/pk is indexed) live only in the in-memory `FtsEngine.indexes` map — so after a restart the splits are orphaned (nothing re-declares them). B4-3 adds: (1) a **durable registry** — index defs persisted to the substrate; (2) **reopen** — rebuild the in-memory defs from the registry on engine open, reconnecting each durable `FtsIndex` to its existing splits (via the stable `fts/{table}/{column}` index_id); (3) a **background seal scheduler** that periodically folds live→durable and bounds split growth via compaction.

**Architecture:**
- `PersistedDef { table, column, column_ordinal, pk_column, analyzer }` (serde). Persisting the `column_ordinal` means reopen needs **no schema fetch** (the ordinal is stable for a table's lifetime).
- Registry is one JSON blob at a fixed key (e.g. `fts/_registry`) holding `Vec<PersistedDef>`, read/written through the engine's `SlateDbBlobStore` (durable mode only). `create_fulltext_index` appends/updates the def then rewrites the blob.
- `FtsEngine::reopen(substrate)` builds a durable engine, loads the registry, and for each `PersistedDef` rebuilds the `IndexDef` directly (fresh empty `LiveSegment::new(analyzer)` + a durable `FtsIndex` reconnected by the stable index_id + the persisted ordinal/pk). The reconnected `FtsIndex` finds its manifest+splits in the substrate automatically (it loads the manifest lazily per search — see `fts.rs`).
- `spawn_seal_scheduler(self: Arc<Self>, interval)` mirrors `FtsIndex::spawn_compaction_scheduler`: a ticked loop that calls `self.seal()` then `self.compact_all()` (each durable index's `maybe_compact`), logging+continuing on error.

**Scope (B4-3):** engine-level registry + reopen + scheduler + compact_all, proven by a simulated-restart test. **NOT here:** binding the durable engine into the HTTP server's promote/demote lifecycle + spawning the scheduler there (B4-4, a small but HA-lifecycle-sensitive follow-up). **Deferred (HA-M4):** cross-node failover replay of the *un-sealed* live window from the SQL watermark — after a crash, writes committed since the last seal are not in any split; SQL remains the source of truth, but rebuilding that live tail is the M4 replay concern (`fts.rs`: "one process owns the index today").

**Tech stack:** Rust; `serde`/`serde_json` (already in the workspace); the B4-part-1 `FtsEngine`/`FtsIndex`; `SlateDbBlobStore` (`get_all`/`put` as used in `fts.rs`'s manifest persistence).

---

## Task 1: durable registry — persist on create, reopen on open

**Files:** `crates/bluedb-engine/src/fts_engine.rs` (+ test `crates/bluedb-engine/tests/fts_restart.rs`).

- [ ] **Failing test** (`fts_restart.rs`, simulated restart over a shared `Arc<Db>`):
```rust
// db over InMemory; database = Database::new(db.clone()).
// engine1 = FtsEngine::new_durable(database.substrate());
// create docs(id INTEGER PRIMARY KEY, body TEXT); engine1.create_fulltext_index_auto(&conn, "docs","body","english").
// insert rows 1 ('quarterly invoice overdue'), 2 ('weather') via an observed conn.
// engine1.seal().await;  // all data now durable
// drop(engine1);         // in-memory live segment + def map gone
// engine2 = FtsEngine::reopen(database.substrate()).await;   // rebuilds defs from the registry
// @@ query via engine2.execute_fts on a fresh Glue over database.connection_serialized()
//   -> returns row 1.   // PROVES the def + durable splits reconnected after "restart"
```
- [ ] **Implement:**
  - `#[derive(Serialize, Deserialize, Clone)] struct PersistedDef { table, column, column_ordinal, pk_column, analyzer }`.
  - A registry key const (`const FTS_REGISTRY_KEY: &str = "fts/_registry";`) and:
    - `async fn load_registry(blob: &SlateDbBlobStore) -> Result<Vec<PersistedDef>>` — `get_all` → `serde_json::from_slice`; absent → `Ok(vec![])` (mirror `fts.rs::load_manifest`'s present/absent handling).
    - `async fn persist_registry(&self) -> Result<()>` — only when `self.blob` is `Some`: snapshot all current defs → `Vec<PersistedDef>` → `blob.put(FTS_REGISTRY_KEY, json)`.
  - `IndexDef` stores enough to round-trip (`column`, `pk_column`, `column_ordinal`, `analyzer`, the durable `Arc<FtsIndex>`, `durable_body_field`, the `Arc<LiveSegment>`).
  - `create_fulltext_index` (and `_auto`): after inserting the in-memory def, call `self.persist_registry().await?` (durable mode only — no-op otherwise).
  - `pub async fn reopen(substrate: Substrate) -> Result<Arc<Self>>`: build the blob; `load_registry`; for each `PersistedDef` construct the `IndexDef` directly — `LiveSegment::new(&analyzer)?`, the durable `FtsIndex` via the same builder `create_fulltext_index` uses (stable `index_id = format!("fts/{table}/{column}")`), set `column_ordinal`/`pk_column` from the persisted values (NO schema fetch). Insert into the map. Return the `Arc`.
  - Guard against duplicate registry entries (a re-`create` of the same (table,column) should replace, not append — dedup by (table,column) in `persist_registry` or on insert).
- [ ] Run `cargo test -p bluedb-engine` (ALL pass, incl. new `fts_restart`) + `cargo build -p bluedb-engine 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): durable FTS registry — persist index defs + reopen reconnects splits"
```

## Task 2: background seal scheduler + compact_all

**Files:** `crates/bluedb-engine/src/fts_engine.rs` (+ test).

- [ ] **Failing test** (multi-thread runtime, like `fts.rs::background_scheduler_compacts`): durable engine + index; insert rows via an observed conn (live, un-sealed); spawn `engine.clone().spawn_seal_scheduler(Duration::from_millis(20))`; poll (bounded ~5s) until a fresh `@@` query — after dropping/clearing the live segment's role — is served from the durable tier. Simplest reliable assertion: after the scheduler has run, `segment.covered()` for the index is empty (the live tier was drained by a scheduled seal) AND the `@@` query still returns the row (now durable). Abort the handle at the end.
- [ ] **Implement:**
  - `pub async fn compact_all(&self) -> Result<()>` — snapshot the durable `Arc<FtsIndex>` handles out of the lock, then `idx.maybe_compact().await` each (ignore `Ok(None)`; log errors).
  - `pub fn spawn_seal_scheduler(self: Arc<Self>, interval: Duration) -> JoinHandle<()>` — mirror `FtsIndex::spawn_compaction_scheduler`: `tokio::time::interval`, skip the first tick, then each tick `self.seal().await` then `self.compact_all().await`, logging+continuing on `Err` (a transient blob error must not kill the loop).
- [ ] Run `cargo test -p bluedb-engine` (pass) + build clean. Commit:
```bash
git commit -am "feat(engine): background seal scheduler + compact_all (bounds durable split growth)"
```

## Self-Review
- Spec B coverage (B4-3): durable registry so index defs + their splits survive a restart (reopen reconnects via the stable index_id) ✓; background seal off the request path (§4.2) ✓; split growth bounded by scheduled compaction ✓.
- Restart-durability is now real at the engine level: a simulated restart (drop engine, reopen over the same substrate) finds the sealed data. The un-sealed live window (writes since the last seal) is the only loss surface — SQL is the source of truth and rebuilding that tail on failover is the deferred HA-M4 replay.
- Deferred: B4-4 (wire the durable engine into the server's promote lifecycle + spawn the scheduler there) — small but touches the role-swap path. Cross-node failover replay → HA-M4.
- Isolation: `FtsEngine::new()` (in-memory) path unchanged — `persist_registry`/`reopen` are no-ops/unused without a blob. Existing `ryw`/`seal`/`fts` tests unaffected.
