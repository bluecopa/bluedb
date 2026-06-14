# bluedb Spec B — increment B4-4: wire durable FTS into the server lifecycle

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Make the durable FTS tier (B4-1/B4-3) actually take effect in the running server. Today `AppState` holds a single in-memory `FtsEngine::new()` — so the deployed server's index is ephemeral even though the engine now supports durability. B4-4 binds a **durable** `FtsEngine` to the active writer on `promote` (reopening any persisted index defs + their splits) and runs the **background seal scheduler**; on `demote` it stops sealing.

**Why this is tractable (no replica-FTS needed):** the FTS read path is `POST /sql` → `exec_sql`, which calls `require_active()`. A passive node 503s those requests. So FTS only ever runs on the **active** node — B4-4 only has to bind/unbind the durable engine across the promote/demote transitions, not serve FTS on replicas.

**Architecture:**
- `Inner.fts` becomes `tokio::sync::RwLock<Arc<FtsEngine>>` (was `Arc<FtsEngine>`), defaulting to `FtsEngine::new()` (in-memory) before the first promote. A `seal_handle: std::sync::Mutex<Option<JoinHandle<()>>>` tracks the scheduler task.
- `promote()`: after opening the writer `Database`, `FtsEngine::reopen(database.substrate())` (reconnects persisted defs + splits), abort any prior `seal_handle`, `spawn_seal_scheduler(interval)`, store the handle, and swap `fts`.
- `demote()`: abort the `seal_handle` and swap `fts` back to `FtsEngine::new()` (a demoted node rejects FTS reads anyway, so an empty in-memory engine is safe).
- Connection builders + handlers read the current engine via `self.inner.fts.read().await.clone()`.
- Seal interval from `BLUEDB_FTS_SEAL_INTERVAL_MS` (default **30000** = 30s) — long enough that sub-second tests never seal (so the seal's drain→write window can't race a test), production-tunable. (The Spec B PRAGMA-per-DB tuning stays deferred; this is the node-level default.)

**Scope (B4-4):** the promote/demote wiring + the env-configured scheduler + an HTTP test that durable FTS still serves `@@` (RYW unchanged) with the durable engine bound. **NOT here:** serving FTS on a read replica; cross-node failover replay of the un-sealed live tail (HA-M4); per-DB PRAGMA seal tuning.

**Tech stack:** Rust, axum, the B4 `FtsEngine` (`reopen`, `spawn_seal_scheduler`), `tokio::sync::RwLock`.

---

## Task 1: swap `fts` to a durable, reopened engine on promote/demote + scheduler

**Files:** `crates/bluedb-server/src/lib.rs` (Inner field, promote, demote, connection builders, `fts()` accessor, exec_sql/schema handlers), `crates/bluedb-server/src/main.rs` (doc note for the env var), `crates/bluedb-server/src/schema.rs` (async `fts()`), `crates/bluedb-server/tests/fts.rs` (extend).

- [ ] **Failing/extended test** in `tests/fts.rs`: the existing end-to-end FTS-over-HTTP test should still pass with the durable engine bound (promote now reopens a durable engine over the test's in-memory DB — empty registry → behaves like before; the `@@` query is served from the live tier via the union). Add an assertion that a SECOND fulltext index definition + query works, and (optional, if easy) that after an explicit no-op the engine is durable-mode (e.g. via a tiny `pub(crate)` test hook `AppState::fts_is_durable()` returning whether the bound engine has a blob — only if it doesn't bloat the API; otherwise skip). Primary goal: prove nothing regressed and the wiring compiles/runs.
- [ ] **Implement:**
  - `Inner.fts: tokio::sync::RwLock<Arc<FtsEngine>>` (init `RwLock::new(FtsEngine::new())`). Add `seal_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>` (init `None`).
  - A const/helper: `fn fts_seal_interval() -> Duration` reading `BLUEDB_FTS_SEAL_INTERVAL_MS` (parse, default 30_000ms) — mirror the existing `parse_flush_interval_ms` style.
  - `promote()`: after `let database = Database::new(Arc::new(db));` (keep the existing `*self.inner.db.write().await = Some(database.clone());` — note `Database: Clone`):
    ```rust
    let fts = FtsEngine::reopen(database.substrate()).await
        .map_err(|e| AppError::internal(format!("reopen fts: {e}")))?;
    // stop a prior scheduler (re-promote) before starting a new one
    if let Some(h) = self.inner.seal_handle.lock().unwrap().take() { h.abort(); }
    let handle = fts.clone().spawn_seal_scheduler(fts_seal_interval());
    *self.inner.seal_handle.lock().unwrap() = Some(handle);
    *self.inner.fts.write().await = fts;
    ```
    (`Database::substrate()` returns the `Substrate`; `FtsEngine::reopen` takes it. Confirm `Database` derives `Clone` — it does — so you can store it in `db` AND read its substrate.)
  - `demote()`: before/after the existing demote logic, `if let Some(h) = self.inner.seal_handle.lock().unwrap().take() { h.abort(); }` and `*self.inner.fts.write().await = FtsEngine::new();`.
  - `connection()` / `connection_serialized()`: `let fts = self.inner.fts.read().await.clone();` then `Ok(db.connection().with_commit_observer(fts))` (and the serialized variant).
  - `fts()` accessor → `pub(crate) async fn fts(&self) -> Arc<FtsEngine> { self.inner.fts.read().await.clone() }`. Update `exec_sql` (`state.fts().await.execute_fts(...)`) and `schema::create_fulltext_index` (`state.fts().await.create_fulltext_index_auto(...)`).
  - `main.rs` `//!`: document `BLUEDB_FTS_SEAL_INTERVAL_MS` (default 30000) and that durable FTS is active on the writer (sealed splits survive restart; reads run on the active node).
- [ ] Run `cargo test -p bluedb-server` (ALL pass — existing api/authz/schema/http2/fts tests unaffected: a fresh in-memory DB → empty registry → durable engine behaves like the in-memory one for these; the 30s interval never seals during sub-second tests) + `cargo build -p bluedb-server 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(server): bind durable FTS engine on promote + background seal scheduler (BLUEDB_FTS_SEAL_INTERVAL_MS)"
```

## Self-Review
- Spec B coverage (B4-4): the deployed active node now runs a durable, self-sealing FTS index — `CREATE FULLTEXT INDEX` defs persist (B4-3 registry) and reconnect on restart, the background seal folds live→durable on the node-level interval, and split growth is bounded by scheduled compaction. FTS reads remain active-node-only (`require_active`), so no replica-FTS is needed.
- Lifecycle: promote reopens + schedules; demote aborts the scheduler + reverts to an empty in-memory engine (safe — a passive node serves no FTS reads). Re-promote aborts the prior scheduler before starting a new one (no leak).
- Test-safety: the 30s default seal interval means the seal's known non-atomic drain→write window (B4-3 note) cannot race a sub-second test; the scheduler mechanism itself is already proven at the engine level.
- Deferred (HA-M4): replica/standby FTS reads; cross-node failover replay of the un-sealed live tail (writes since the last seal — SQL remains source of truth); per-DB PRAGMA seal tuning (only the node-level env default here).
