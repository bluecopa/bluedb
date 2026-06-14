# bluedb A2 — flush_interval default 25ms (open-time setting) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Open the writer SlateDB `Db` with a `flush_interval` defaulting to **25 ms** (down from SlateDB's 100 ms), overridable via `BLUEDB_FLUSH_INTERVAL_MS`, to cut per-request write latency for the HTTP profile.

**Architecture:** `flush_interval` is a SlateDB `Settings` field fixed at `Db` open — it is **not** a runtime-mutable PRAGMA. So this is an **open-time** setting applied in `bluedb-server`'s `promote()` (the writer-Db open site, `lib.rs:93`), via `Db::builder(...).with_settings(...)`. A pure `parse_flush_interval_ms` helper makes the env parsing unit-testable.

**Tech Stack:** Rust, slatedb 0.13 (`Settings`, `Db::builder().with_settings().build()`), axum.

**Deviation note (documented):** Spec A §4.5 framed `flush_interval` as a "PRAGMA". SlateDB fixes `flush_interval` at `Db` open and provides no API to change it on a live `Db`, so a runtime SQL PRAGMA is not implementable. A2 implements it as an **open-time config** (`BLUEDB_FLUSH_INTERVAL_MS`, default 25). The FTS *seal* interval (Spec B) remains a true runtime PRAGMA because that is bluedb's own code.

---

## Task 1: 25 ms default flush_interval at writer open, env-overridable

**Files:**
- Modify: `crates/bluedb-server/src/lib.rs` — add `parse_flush_interval_ms` + `writer_settings`; use them in `promote()`'s `Db::open` (line ~93). Add a `#[cfg(test)] mod flush_interval_cfg`.

- [ ] **Step 1: Write the failing test**

Add a `#[cfg(test)]` module to `crates/bluedb-server/src/lib.rs`:

```rust
#[cfg(test)]
mod flush_interval_cfg {
    use super::parse_flush_interval_ms;
    use std::time::Duration;

    #[test]
    fn defaults_to_25ms_and_parses_override() {
        assert_eq!(parse_flush_interval_ms(None), Duration::from_millis(25));
        assert_eq!(parse_flush_interval_ms(Some("50")), Duration::from_millis(50));
        assert_eq!(parse_flush_interval_ms(Some("100")), Duration::from_millis(100));
        // Garbage / empty falls back to the 25ms default (never panics).
        assert_eq!(parse_flush_interval_ms(Some("abc")), Duration::from_millis(25));
        assert_eq!(parse_flush_interval_ms(Some("")), Duration::from_millis(25));
        // 0 is honored as given (caller's choice; SlateDB treats Some(0) as its own edge).
        assert_eq!(parse_flush_interval_ms(Some("0")), Duration::from_millis(0));
    }
}
```

- [ ] **Step 2: Run test, expect FAIL** — `cargo test -p bluedb-server flush_interval_cfg` → `cannot find function parse_flush_interval_ms`.

- [ ] **Step 3: Implement the helpers + wire into `promote()`**

Add near the top of `crates/bluedb-server/src/lib.rs` (after the imports; ensure `use slatedb::Settings;` and `use std::time::Duration;` are present — add them if missing):

```rust
/// bluedb's default WAL flush interval (overrides SlateDB's 100 ms) — chosen for
/// the latency-sensitive HTTP profile. Override with `BLUEDB_FLUSH_INTERVAL_MS`.
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 25;

/// Parse a `BLUEDB_FLUSH_INTERVAL_MS` value into a `Duration`. `None`, empty, or
/// unparseable → the 25 ms default (total + non-panicking).
fn parse_flush_interval_ms(raw: Option<&str>) -> Duration {
    let ms = raw
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS);
    Duration::from_millis(ms)
}

/// SlateDB `Settings` for the writer `Db`: bluedb's `flush_interval` default,
/// env-overridable, everything else left at SlateDB defaults.
fn writer_settings() -> Settings {
    let mut settings = Settings::default();
    settings.flush_interval = Some(parse_flush_interval_ms(
        std::env::var("BLUEDB_FLUSH_INTERVAL_MS").ok().as_deref(),
    ));
    settings
}
```

Then change the writer-`Db` open in `promote()` (currently `let db = Db::open(self.inner.db_path.clone(), self.inner.object_store.clone()).await ...`) to use the builder:

```rust
        let db = Db::builder(self.inner.db_path.clone(), self.inner.object_store.clone())
            .with_settings(writer_settings())
            .build()
            .await
            .map_err(|err| AppError::internal(format!("open writer db: {err}")))?;
```

(Keep the surrounding `*self.inner.db.write().await = Some(Database::new(Arc::new(db)));` unchanged. `Db::builder` is exported as `slatedb::Db::builder`; `DbBuilder` need not be imported.)

- [ ] **Step 4: Run test, expect PASS** — `cargo test -p bluedb-server flush_interval_cfg`.

- [ ] **Step 5: Build + regression** — `cargo build -p bluedb-server` (clean) and `cargo test -p bluedb-server` (all existing tests still pass — the writer open now goes through the builder; the existing CRUD round-trip test exercises `promote()` so it covers this).

- [ ] **Step 6: Doc + commit**

Add an env-var line to the `//!` config doc block at the top of `crates/bluedb-server/src/main.rs` (alongside the other `BLUEDB_*` vars):
```
//! - `BLUEDB_FLUSH_INTERVAL_MS` — WAL flush interval in ms (default 25). Set at
//!   writer open; lower = lower write latency + more object-store PUTs under load.
```
Commit:
```bash
git add crates/bluedb-server/src/lib.rs crates/bluedb-server/src/main.rs
git commit -m "feat(server): writer flush_interval defaults to 25ms, BLUEDB_FLUSH_INTERVAL_MS override"
```

---

## Self-Review
- Spec A §4.5 coverage: 25 ms default ✓ (open-time, deviation documented); env override ✓; documented ✓. The "PRAGMA" framing is replaced by open-time config with a written rationale.
- Placeholder scan: none. Type consistency: `parse_flush_interval_ms(Option<&str>) -> Duration`; `writer_settings() -> Settings`; both used in `promote()`.
- The pure helper avoids env-var test flakiness (no global env mutation in the test).
