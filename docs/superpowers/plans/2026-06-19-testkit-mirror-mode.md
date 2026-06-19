# Mirror-enabled testkit mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `bluedb-testkit` run the embedded server with the lakehouse mirror on, backed by a local temp object store, so a Python test can `write → seal → read` against the Iceberg mirror (and DuckDB can read it through `/catalog/v1`).

**Architecture:** `serve(mirror=True)` points the embedded server's lakehouse object store at a temp-dir `LocalFileSystem` with a `file://` base and defaults mirroring on for every tenant, through the existing `promote()` path. `db.seal()` drives the existing `AppState::seal_now()` synchronously via the runtime `Handle`. A separate writer fix lets UUID columns seal.

**Tech Stack:** Rust (arrow, iceberg-rust, object_store, tokio), PyO3/maturin, pytest, DuckDB (optional).

Spec: `docs/superpowers/specs/2026-06-19-testkit-mirror-mode-design.md`. Worktree: `/Users/satya/work/bc/bluedb-testkit-mirror` on `feat/testkit-mirror-mode`. Run all `cargo`/`git` from the worktree root.

---

### Task 1: UUID column seals (writer FixedSizeBinary(16))

**Files:**
- Modify: `crates/bluedb-lakehouse/src/writer.rs` (`build_arrow_column`, ~line 1133; imports ~line 28)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `crates/bluedb-lakehouse/src/writer.rs`:

```rust
#[test]
fn build_arrow_column_handles_uuid() {
    use arrow_schema::DataType as ArrowDataType;
    let u: u128 = 0x0123_4567_89ab_cdef_0123_4567_89ab_cdef;
    let cells: Vec<&Value> = vec![&Value::Uuid(u)];
    let arr = build_arrow_column(&ArrowDataType::FixedSizeBinary(16), &cells).unwrap();
    let fsb = arr
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .expect("FixedSizeBinaryArray");
    assert_eq!(fsb.len(), 1);
    assert_eq!(fsb.value(0), &u.to_be_bytes());
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-lakehouse build_arrow_column_handles_uuid 2>&1`
Expected: FAIL — the `other =>` arm returns `Err("arrow column type FixedSizeBinary(16) not supported")`.

- [ ] **Step 3: Add the import**

In `crates/bluedb-lakehouse/src/writer.rs`, find the `arrow_array` import block (the one bringing in `Date32Array`, `Decimal128Array`, etc.) and add `FixedSizeBinaryBuilder`. If imports are per-type, add:

```rust
use arrow_array::FixedSizeBinaryBuilder;
```

- [ ] **Step 4: Add the match arm**

In `build_arrow_column`, immediately before the final `other => { return Err(...) }` arm (~line 1133), insert:

```rust
        // iceberg-rust maps Iceberg `uuid` to Arrow `FixedSizeBinary(16)`.
        // gluesql stores a u128; we write its 16 big-endian bytes.
        ArrowDataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::new(16);
            for v in cells {
                match v {
                    Value::Uuid(u) => b
                        .append_value(u.to_be_bytes())
                        .map_err(|e| LakehouseError::Schema(format!("uuid array: {e}")))?,
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p bluedb-lakehouse build_arrow_column_handles_uuid 2>&1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-lakehouse/src/writer.rs
git commit -F - <<'EOF'
feat(lakehouse): seal UUID columns (FixedSizeBinary(16) writer arm)

build_arrow_column had no UUID case, so sealing a UUID column errored. Write
Value::Uuid(u128) as its 16 big-endian bytes into a FixedSizeBinary(16) array,
matching iceberg-rust's Arrow mapping for Iceberg `uuid`.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 2: Manager default-mirror-on for every new tenant

**Files:**
- Modify: `crates/bluedb-lakehouse/src/manager.rs` (`struct LakehouseManager` ~line 72; `open` ~line 94; `engine_for` ~line 128)

- [ ] **Step 1: Write the failing test**

Add to `crates/bluedb-lakehouse/tests/manager.rs` (reuse that file's existing `cfg()` helper + setup pattern — open a manager over a temp `LocalFileSystem`, write CDC rows for an arbitrary tenant, seal, assert mirrored). Minimal version:

```rust
#[tokio::test]
async fn default_mirror_on_makes_new_tenants_opt_out() {
    // (Follow the file's existing harness to build `manager` + a `Database`.)
    let (manager, _db, _tmp) = open_test_manager().await; // existing helper in this file
    manager.set_default_mirror(true);
    let eng = manager.engine_for("ws1_sol2_copa_collection_v2").await.unwrap();
    assert!(eng.is_mirrored("any_table"), "a fresh tenant must mirror by default when default_mirror_on");
}
```

If `manager.rs` test file has no reusable opener, model the body on an existing test there (they already construct a manager + temp fs). The assertion is the contract: `set_default_mirror(true)` ⇒ a brand-new tenant's `is_mirrored` is true.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-lakehouse default_mirror_on_makes_new_tenants_opt_out 2>&1`
Expected: FAIL — `set_default_mirror` doesn't exist (compile error).

- [ ] **Step 3: Add the field + setter**

In `crates/bluedb-lakehouse/src/manager.rs`, add `use std::sync::atomic::{AtomicBool, Ordering};` at the top. Add a field to `struct LakehouseManager`:

```rust
    /// When true, a freshly-opened tenant (no persisted registry choice) mirrors
    /// by default. Set by the testkit's mirror mode; prod leaves it false.
    default_mirror_on: AtomicBool,
```

In `open(...)`, initialize it where the struct is built:

```rust
            default_mirror_on: AtomicBool::new(false),
```

Add the setter in `impl LakehouseManager`:

```rust
    /// Make every subsequently-opened tenant mirror by default (testkit mirror
    /// mode). Call right after `open`, before any writes.
    pub fn set_default_mirror(&self, on: bool) {
        self.default_mirror_on.store(on, Ordering::Relaxed);
    }
```

- [ ] **Step 4: Apply it at engine creation**

In `engine_for`, right after the engine is built and before `map.insert(...)`, add:

```rust
            if self.default_mirror_on.load(Ordering::Relaxed) {
                engine
                    .apply_pragma(bluedb_sql::LhPragma::GlobalDefault(true))
                    .await?;
            }
```

(`LhPragma` is already imported in this file for `apply_pragma`; use the existing path — if it's imported unqualified, write `LhPragma::GlobalDefault(true)`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p bluedb-lakehouse default_mirror_on_makes_new_tenants_opt_out 2>&1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-lakehouse/src/manager.rs crates/bluedb-lakehouse/tests/manager.rs
git commit -F - <<'EOF'
feat(lakehouse): manager default-mirror-on for new tenants

set_default_mirror(true) makes every freshly-opened tenant mirror by default
(applied at engine_for via GlobalDefault). Powers the testkit's auto-on mirror
mode for arbitrary tenants; prod leaves it false. Per-table PRAGMA opt-out and
later global PRAGMAs still work.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 3: Server builders — lakehouse object store + default-on

**Files:**
- Modify: `crates/bluedb-server/src/lib.rs` (`AppStateInner` ~line 210/259; builders ~line 338-355; `new` ~line 286; `promote` ~line 588)

- [ ] **Step 1: Write the failing test**

Add to `crates/bluedb-server/tests/` (new file `tests/lakehouse_builders.rs`):

```rust
use bluedb_server::AppState;
use slatedb::object_store::local::LocalFileSystem;
use std::sync::Arc;

#[tokio::test]
async fn mirror_builders_wire_a_local_fs_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let abs = std::fs::canonicalize(tmp.path()).unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(&abs).unwrap());
    let store = Arc::new(slatedb::object_store::memory::InMemory::new());
    let lease = Arc::new(bluedb_ha::LocalLeaseProvider::new());
    let writer = Arc::new(bluedb_ha::WriterController::new(
        "t", lease, Arc::new(bluedb_ha::SystemClock),
        std::time::Duration::from_secs(3600), std::time::Duration::from_secs(5),
    ));
    let state = AppState::new(store, "bluedb".into(), writer)
        .with_lakehouse_object_store(fs)
        .with_lakehouse_base(format!("file://{}", abs.display()))
        .with_lakehouse_default_on(true);
    state.promote().await.expect("promote builds the manager");
    // A write+seal for an arbitrary tenant lands a file:// metadata location.
    // (Exercised end-to-end in the testkit task; here we assert promote succeeds
    // and seal_now is callable with the fs-backed manager.)
    state.seal_now().await.expect("seal_now ok");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-server mirror_builders_wire_a_local_fs_mirror 2>&1`
Expected: FAIL — `with_lakehouse_object_store` / `with_lakehouse_default_on` don't exist.

- [ ] **Step 3: Add the `lakehouse_default_on` field**

In `AppStateInner` (near `lakehouse_base`, ~line 259), add:

```rust
    /// Testkit mirror mode: default mirroring on for every tenant.
    lakehouse_default_on: bool,
```

In `AppState::new` where `inner` is constructed (~line 295-307), add:

```rust
                lakehouse_default_on: false,
```

- [ ] **Step 4: Add the two builders**

After `with_lakehouse_base` (~line 355) in `impl AppState`:

```rust
    /// Set the object store the lakehouse mirror writes to (distinct from the
    /// SlateDB store). Startup-only; no-op once the `Arc<Inner>` is shared.
    pub fn with_lakehouse_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.lakehouse_object_store = store;
        }
        self
    }

    /// Default mirroring on for every tenant (testkit mirror mode). Startup-only.
    pub fn with_lakehouse_default_on(mut self, on: bool) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.lakehouse_default_on = on;
        }
        self
    }
```

- [ ] **Step 5: Apply default-on in `promote`**

In `promote`, right after `*self.inner.lakehouse.write().await = Some(lakehouse);` (~line 597), replace that line with capturing the manager and applying the flag:

```rust
        if self.inner.lakehouse_default_on {
            lakehouse.set_default_mirror(true);
        }
        *self.inner.lakehouse.write().await = Some(lakehouse);
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test -p bluedb-server mirror_builders_wire_a_local_fs_mirror 2>&1`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/bluedb-server/src/lib.rs crates/bluedb-server/tests/lakehouse_builders.rs
git commit -F - <<'EOF'
feat(server): with_lakehouse_object_store + with_lakehouse_default_on

Lets a caller (the testkit) point the Iceberg mirror at a separate object store
(a temp LocalFileSystem) with a file:// base, and default mirroring on for every
tenant. promote() applies default-on to the manager it opens.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 4: Embedded server mirror mode + seal + warehouse_path (Rust)

**Files:**
- Modify: `crates/bluedb-py/Cargo.toml` (add `tempfile` to `[dependencies]`)
- Modify: `crates/bluedb-py/src/embedded.rs` (`EmbeddedConfig`, `EmbeddedServer`, `start`, plus new `seal`/`warehouse_path` + tests)

- [ ] **Step 1: Write the failing test**

Add to `#[cfg(test)] mod tests` in `crates/bluedb-py/src/embedded.rs`:

```rust
#[test]
fn mirror_mode_seals_and_serves_file_uris_with_type_fidelity() {
    let s = EmbeddedServer::start(EmbeddedConfig { mirror: true, ..Default::default() }).unwrap();
    let token = s.token().unwrap().to_string();
    let bearer = format!("Bearer {token}");
    let post = |path: &str, body: &str| {
        ureq::post(&format!("{}{}", s.base_url(), path))
            .set("Authorization", &bearer)
            .set("Content-Type", "application/json")
            .send_string(body)
    };
    // DDL via /admin/sql; one row with decimal/date/timestamp/uuid.
    post("/admin/sql", r#"{"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY, amt DECIMAL(10,2), d DATE, ts TIMESTAMP, uid UUID)"}"#).unwrap();
    post("/tables/t", r#"{"id":1,"amt":9.99,"d":"2026-01-01","ts":"2026-01-01 00:00:00","uid":"00000000-0000-0000-0000-000000000001"}"#).unwrap();

    s.seal().expect("seal");

    let meta: serde_json::Value = ureq::get(&format!("{}/catalog/v1/namespaces/default/tables/t", s.base_url()))
        .set("Authorization", &bearer)
        .call().unwrap().into_json().unwrap();
    let loc = meta["metadata-location"].as_str().unwrap();
    assert!(loc.starts_with("file://"), "loadTable URI must be file://, got {loc}");
    // type fidelity: the Iceberg schema reports uuid/decimal/date/timestamp.
    let types = meta["metadata"]["schemas"][0]["fields"].to_string();
    assert!(types.contains("uuid"), "uuid type must survive: {types}");
    assert!(types.contains("decimal"), "decimal type must survive: {types}");

    assert!(s.warehouse_path().unwrap().len() > 0);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p bluedb-py mirror_mode_seals_and_serves_file_uris_with_type_fidelity 2>&1`
Expected: FAIL — `EmbeddedConfig` has no `mirror`, no `seal`/`warehouse_path`.

- [ ] **Step 3: Add the `tempfile` dependency**

In `crates/bluedb-py/Cargo.toml` under `[dependencies]` add (match an existing version in the workspace lock):

```toml
tempfile = "3"
```

- [ ] **Step 4: Extend config + struct + imports**

In `crates/bluedb-py/src/embedded.rs`:

Add imports near the top:

```rust
use bluedb_server::{build_app, AppState};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use tokio::runtime::Handle;
```

Add `mirror` to `EmbeddedConfig` (and its `Default`):

```rust
    pub mirror: bool,
```
```rust
            mirror: false,
```

Extend `EmbeddedServer`:

```rust
pub struct EmbeddedServer {
    base_url: String,
    token: Option<String>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    warehouse: Option<tempfile::TempDir>,   // kept alive; Drop cleans it
    runtime_handle: Handle,
    app_state: AppState,                     // clone for in-process seal()
}
```

- [ ] **Step 5: Wire mirror in `start` + capture the handle/state**

Change the ready channel payload and the thread body. The ready channel becomes:

```rust
let (ready_tx, ready_rx) =
    std::sync::mpsc::channel::<anyhow::Result<(SocketAddr, Handle, AppState)>>();
```

Before spawning, create the temp warehouse on the main thread and pass its path in:

```rust
let warehouse = if cfg.mirror { Some(tempfile::tempdir()?) } else { None };
let warehouse_path = warehouse.as_ref().map(|d| d.path().to_path_buf());
```

Add `mirror` (and `warehouse_path`) to the destructure / move set. Inside the spawned closure, after building the runtime, grab the handle and move it in:

```rust
                let handle = rt.handle().clone();
                rt.block_on(async move {
                    if let Some(ms) = flush_interval_ms { std::env::set_var("BLUEDB_FLUSH_INTERVAL_MS", ms.to_string()); }
                    let object_store = Arc::new(InMemory::new());
                    let lease = Arc::new(LocalLeaseProvider::new());
                    let writer = Arc::new(WriterController::new(
                        "testkit-node", lease, Arc::new(SystemClock),
                        Duration::from_secs(3600), Duration::from_secs(5),
                    ));
                    let mut state = AppState::new(object_store, db_path, writer)
                        .with_admin_sql_enabled(admin_sql);
                    if let Some(p) = &warehouse_path {
                        let abs = match std::fs::canonicalize(p) {
                            Ok(a) => a,
                            Err(e) => { let _ = ready_tx.send(Err(e.into())); return; }
                        };
                        let fs = match LocalFileSystem::new_with_prefix(&abs) {
                            Ok(f) => Arc::new(f),
                            Err(e) => { let _ = ready_tx.send(Err(e.into())); return; }
                        };
                        state = state
                            .with_lakehouse_object_store(fs)
                            .with_lakehouse_base(format!("file://{}", abs.display()))
                            .with_lakehouse_default_on(true);
                    }
                    if evidence_signing { state = state.with_local_signer_for_tests(); }
                    if let Some(a) = authz { state = state.with_authz(a); }
                    if let Err(e) = state.promote().await {
                        let _ = ready_tx.send(Err(anyhow::anyhow!("promote failed: {e:?}")));
                        return;
                    }
                    let state_for_handle = state.clone();
                    let app = build_app(state);
                    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                        Ok(l) => l, Err(e) => { let _ = ready_tx.send(Err(e.into())); return; }
                    };
                    let addr = match listener.local_addr() {
                        Ok(a) => a, Err(e) => { let _ = ready_tx.send(Err(e.into())); return; }
                    };
                    let _ = ready_tx.send(Ok((addr, handle.clone(), state_for_handle)));
                    let _ = axum::serve(listener, app)
                        .with_graceful_shutdown(async move { let _ = shutdown_rx.await; })
                        .await;
                });
```

Receive the extended payload and populate the struct:

```rust
        let (addr, runtime_handle, app_state) = ready_rx.recv()??;
        Ok(EmbeddedServer {
            base_url: format!("http://{addr}"),
            token,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
            warehouse,
            runtime_handle,
            app_state,
        })
```

- [ ] **Step 6: Add `seal` + `warehouse_path` methods**

In `impl EmbeddedServer`:

```rust
    /// Synchronously seal buffered writes into the Iceberg mirror (mirror mode).
    /// No-op shape on a non-mirror server (seal_now returns Ok with no manager).
    pub fn seal(&self) -> anyhow::Result<()> {
        self.runtime_handle
            .block_on(self.app_state.seal_now())
            .map_err(|e| anyhow::anyhow!("seal failed: {e:?}"))
    }

    /// The local warehouse directory (mirror mode), else `None`.
    pub fn warehouse_path(&self) -> Option<String> {
        self.warehouse.as_ref().map(|d| d.path().display().to_string())
    }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cargo test -p bluedb-py mirror_mode_seals_and_serves_file_uris_with_type_fidelity 2>&1`
Expected: PASS (proves mirror boot, seal, `file://` URIs, and DECIMAL/DATE/TIMESTAMP/UUID fidelity).

- [ ] **Step 8: Add the watermark + namespace verification test**

Add another test in the same module:

```rust
#[test]
fn watermark_header_and_underscore_tenant_namespace() {
    let s = EmbeddedServer::start(EmbeddedConfig { mirror: true, ..Default::default() }).unwrap();
    let bearer = format!("Bearer {}", s.token().unwrap());
    let tenant = "ws1_sol2_copa_collection_v2";
    let r = ureq::post(&format!("{}/admin/sql", s.base_url()))
        .set("Authorization", &bearer)
        .set("X-Bluedb-Tenant", tenant)
        .set("Content-Type", "application/json")
        .send_string(r#"{"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY)"}"#).unwrap();
    // write returns a watermark header
    let w = ureq::post(&format!("{}/tables/t", s.base_url()))
        .set("Authorization", &bearer).set("X-Bluedb-Tenant", tenant)
        .set("Content-Type", "application/json")
        .send_string(r#"{"id":1}"#).unwrap();
    assert!(w.header("X-Bluedb-Watermark").is_some(), "write must surface a watermark");
    s.seal().unwrap();
    // superuser lists the underscore-named namespace
    let ns: serde_json::Value = ureq::get(&format!("{}/catalog/v1/namespaces/{tenant}/tables", s.base_url()))
        .set("Authorization", &bearer).call().unwrap().into_json().unwrap();
    assert!(ns.to_string().contains("\"t\""), "namespace==tenant lists the table: {ns}");
}
```

Run: `cargo test -p bluedb-py 2>&1` — expect all embedded tests PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/bluedb-py/Cargo.toml crates/bluedb-py/src/embedded.rs Cargo.lock
git commit -F - <<'EOF'
feat(testkit): mirror mode — fs-backed Iceberg mirror + sync seal()

EmbeddedConfig.mirror points the lakehouse at a temp LocalFileSystem with a
file:// base and defaults mirroring on; the runtime Handle + an AppState clone
are sent back so seal() runs seal_now() synchronously in-process. warehouse_path
exposes the temp dir (Drop cleans it). Tests prove write->seal->read, file://
loadTable URIs, DECIMAL/DATE/TIMESTAMP/UUID fidelity, watermark headers, and
underscore-tenant namespaces under a superuser token.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 5: PyO3 surface — mirror param + seal() + warehouse_path

**Files:**
- Modify: `crates/bluedb-py/src/lib.rs` (`TestServer` struct/getters ~line 21-28; `new` signature ~line 32; add methods)

- [ ] **Step 1: Extend the constructor signature**

In `crates/bluedb-py/src/lib.rs`, add `mirror=false` to the `#[pyo3(signature = ...)]` and the `new` params, and set `cfg.mirror`:

```rust
    #[pyo3(signature = (authz=None, token=None, admin_sql=true, flush_interval_ms=None, db_path=None, evidence_signing=false, mirror=false))]
    fn new(
        // ...existing params...,
        mirror: bool,
    ) -> PyResult<Self> {
        // where EmbeddedConfig is built, set:
        //   mirror,
        //   ..rest
    }
```

(Match the existing construction of `EmbeddedConfig` in `new` and add the `mirror` field.)

- [ ] **Step 2: Add `seal` + `warehouse_path` to `#[pymethods]`**

```rust
    /// Synchronously seal writes into the Iceberg mirror (mirror mode).
    fn seal(&self) -> PyResult<()> {
        self.inner
            .seal()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// The local warehouse directory (mirror mode), or None.
    #[getter]
    fn warehouse_path(&self) -> Option<String> {
        self.inner.warehouse_path()
    }
```

(`self.inner` is the `EmbeddedServer`; match the existing field name used by `stop`/`base_url`.)

- [ ] **Step 3: Build the extension**

Run: `cargo build -p bluedb-py --features python 2>&1`
Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-py/src/lib.rs
git commit -F - <<'EOF'
feat(testkit): expose mirror=, seal(), warehouse_path to Python (PyO3)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 6: Python wrapper + bluedb_mirrored fixture

**Files:**
- Modify: `crates/bluedb-py/python/bluedb_testkit/__init__.py` (`Handle`)
- Modify: `crates/bluedb-py/python/bluedb_testkit/pytest_plugin.py` (new fixture)
- Create: `crates/bluedb-py/python/tests/test_mirror.py`

- [ ] **Step 1: Add `seal` + `warehouse_path` to `Handle`**

In `__init__.py`, in `class Handle`:

```python
    @property
    def warehouse_path(self) -> str | None:
        return self._server.warehouse_path

    def seal(self) -> None:
        """Synchronously seal writes into the Iceberg mirror (mirror mode)."""
        self._server.seal()
```

- [ ] **Step 2: Add the `bluedb_mirrored` fixture**

In `pytest_plugin.py`:

```python
@pytest.fixture
def bluedb_mirrored():
    """Function-scoped: a fresh instance with the Iceberg mirror on (local fs)."""
    with serve(mirror=True) as db:
        yield db
```

- [ ] **Step 3: Write the Python test (DuckDB gated)**

Create `crates/bluedb-py/python/tests/test_mirror.py`:

```python
import httpx


def test_write_seal_read_through_mirror(bluedb_mirrored):
    db = bluedb_mirrored
    h = db.headers()
    httpx.post(db.url("/admin/sql"), headers=h,
               json={"sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"}).raise_for_status()
    httpx.post(db.url("/tables/t"), headers=h, json={"id": 1, "body": "hi"}).raise_for_status()
    db.seal()
    meta = httpx.get(db.url("/catalog/v1/namespaces/default/tables/t"), headers=h).json()
    assert meta["metadata-location"].startswith("file://")
    assert db.warehouse_path


def test_duckdb_reads_the_mirror(bluedb_mirrored):
    duckdb = __import__("importlib").util.find_spec("duckdb")
    if duckdb is None:
        import pytest
        pytest.skip("duckdb not installed")
    import duckdb as ddb
    db = bluedb_mirrored
    h = db.headers()
    httpx.post(db.url("/admin/sql"), headers=h,
               json={"sql": "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)"}).raise_for_status()
    httpx.post(db.url("/tables/t"), headers=h, json={"id": 1, "body": "hi"}).raise_for_status()
    db.seal()
    loc = httpx.get(db.url("/catalog/v1/namespaces/default/tables/t"), headers=h).json()["metadata-location"]
    con = ddb.connect()
    con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
    rows = con.execute("SELECT id, body FROM iceberg_scan(?) ORDER BY id", [loc]).fetchall()
    assert rows == [(1, "hi")]
```

- [ ] **Step 4: Build + run the Python tests**

Run:
```bash
cd crates/bluedb-py && maturin develop --features python 2>&1 && python -m pytest python/tests/test_mirror.py -p no:cacheprovider 2>&1; cd ../..
```
Expected: `test_write_seal_read_through_mirror` PASS; `test_duckdb_reads_the_mirror` PASS if DuckDB present, else SKIP.

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-py/python/bluedb_testkit/__init__.py crates/bluedb-py/python/bluedb_testkit/pytest_plugin.py crates/bluedb-py/python/tests/test_mirror.py
git commit -F - <<'EOF'
feat(testkit): Handle.seal()/warehouse_path + bluedb_mirrored fixture + tests

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

### Task 7: Document the DuckDB-through-catalog recipe

**Files:**
- Modify: `crates/bluedb-py/README.md` (or the testkit section of mkdocs under `docs/`)

- [ ] **Step 1: Add a "Mirror mode + DuckDB" section**

Document, with a runnable snippet: `serve(mirror=True)` / the `bluedb_mirrored` fixture, `db.seal()`, fetching `metadata-location` from `GET /catalog/v1/namespaces/{ns}/tables/{t}`, and the DuckDB recipe:

```python
import duckdb, httpx
from bluedb_testkit import serve

with serve(mirror=True) as db:
    h = db.headers()
    httpx.post(db.url("/admin/sql"), headers=h,
               json={"sql": "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)"})
    httpx.post(db.url("/tables/docs"), headers=h, json={"id": 1, "body": "hello"})
    db.seal()                                  # rows now in the Iceberg mirror
    loc = httpx.get(db.url("/catalog/v1/namespaces/default/tables/docs"),
                    headers=h).json()["metadata-location"]   # file:///…
    con = duckdb.connect(); con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
    print(con.execute("SELECT * FROM iceberg_scan(?)", [loc]).fetchall())
```

Note: namespace == tenant (default tenant → `default`); a superuser token lists/loads any namespace; the warehouse is a temp dir (`db.warehouse_path`) cleaned when the server stops.

- [ ] **Step 2: Commit**

```bash
git add crates/bluedb-py/README.md
git commit -F - <<'EOF'
docs(testkit): mirror mode + DuckDB-through-catalog recipe

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Self-review

**Spec coverage:** (1) mirror mode → Tasks 3-6; (2) sync seal → Task 4 `seal()` + Task 5/6 exposure; (3) `file://` URIs → Task 4 test; (4) DuckDB recipe → Tasks 6-7; (5) watermark → Task 4 Step 8; (6) namespace==tenant + superuser → Task 4 Step 8; (7) type fidelity incl. UUID → Task 1 + Task 4 Step 1. All covered.

**Placeholder scan:** none — every code step has concrete code. Two soft spots flagged for the implementer to match local style, not invent behavior: the `manager.rs` test harness (Task 2 Step 1 — reuse the file's existing manager opener) and the `EmbeddedConfig` construction site in PyO3 `new` (Task 5 Step 1).

**Type consistency:** `set_default_mirror`/`default_mirror_on` (Task 2) ↔ `with_lakehouse_default_on`/`lakehouse_default_on` (Task 3) ↔ `EmbeddedConfig.mirror` (Task 4) ↔ `mirror=` PyO3 param (Task 5) ↔ `serve(mirror=True)`/`bluedb_mirrored` (Task 6). `seal()` returns `anyhow::Result<()>` (Rust) → `PyResult<()>` (PyO3) → `None` (Python). `warehouse_path` is `Option<String>` throughout. Consistent.
