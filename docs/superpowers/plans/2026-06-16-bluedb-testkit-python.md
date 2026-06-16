# bluedb-testkit (Python embeddable test instance) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `bluedb-testkit`, a pip-installable PyO3 wheel that embeds the real `bluedb-server` axum app in the Python test process over an in-memory store, authenticated by default, exposing a `base_url` + auth headers for app-repo integration tests.

**Architecture:** A new in-workspace crate `crates/bluedb-py` with two clean layers: a **pure-Rust core** (`embedded.rs` + `authz_cfg.rs`, no PyO3) that boots `build_app(AppState)` over `InMemory` + `LocalLeaseProvider` on `127.0.0.1:0` on a dedicated tokio runtime thread, unit-testable with plain `cargo test`; and a **thin PyO3 layer** (`lib.rs`) exposing a `TestServer` class. A small Python package (`bluedb_testkit`) adds `serve()` + `url()`/`headers()` ergonomics and pytest fixtures.

**Tech Stack:** Rust, PyO3 0.22 (abi3), maturin, axum 0.8, tokio, slatedb (`object_store::memory::InMemory`), `bluedb-server` lib API (`build_app`, `AppState`, `authz::Authz`), `bluedb-ha` (`LocalLeaseProvider`, `SystemClock`, `WriterController`). Python: pytest, httpx (test-time).

---

## File Structure

- `crates/bluedb-py/Cargo.toml` — crate manifest; `crate-type = ["cdylib", "rlib"]`; deps on `bluedb-server`/`bluedb-ha` (path), `slatedb`/`tokio`/`anyhow` (workspace), `axum` 0.8, `pyo3` 0.22; dev-dep `ureq`.
- `crates/bluedb-py/pyproject.toml` — maturin backend, `module-name = bluedb_testkit._bluedb_testkit`, `python-source = python`, `features = ["pyo3/extension-module"]`, pytest11 entry point.
- `crates/bluedb-py/src/authz_cfg.rs` — `AuthzSpec` enum + `resolve()` → `(Option<Authz>, Option<token>)`. Pure Rust. **Tested.**
- `crates/bluedb-py/src/embedded.rs` — `EmbeddedConfig`, `EmbeddedServer` (runtime thread, readiness handshake, graceful shutdown, `base_url`, `token`). Pure Rust. **Tested.**
- `crates/bluedb-py/src/lib.rs` — PyO3 `TestServer` (`#[pyclass]`) + `parse_authz` + `#[pymodule] _bluedb_testkit`.
- `crates/bluedb-py/python/bluedb_testkit/__init__.py` — `Handle` (`base_url`/`token`/`url()`/`headers()`/`stop()`), `serve()` context manager, re-exports.
- `crates/bluedb-py/python/bluedb_testkit/pytest_plugin.py` — `bluedb` (function) + `bluedb_session` (session) fixtures.
- `crates/bluedb-py/python/tests/test_testkit.py` — Python integration tests.
- `crates/bluedb-py/README.md` — usage + build notes.

**Workspace note:** `crates/*` glob auto-includes `bluedb-py`. PyO3 is behind the
optional `python` feature (off by default), so `cargo build --workspace` /
`cargo test --workspace` build the pure-Rust core with **no libpython dependency** —
the existing all-Rust workflow is unaffected. Only `maturin` (or `--features python`)
pulls in PyO3. No root `Cargo.toml` edit is required (path deps + existing workspace
deps cover everything).

---

### Task 1: Crate scaffold that compiles and imports

**Files:**
- Create: `crates/bluedb-py/Cargo.toml`
- Create: `crates/bluedb-py/pyproject.toml`
- Create: `crates/bluedb-py/src/lib.rs`
- Create: `crates/bluedb-py/python/bluedb_testkit/__init__.py`

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "bluedb-py"
version = "0.0.0"
edition.workspace = true
license.workspace = true
description = "Python-embeddable, test-only bluedb instance (PyO3 wheel: bluedb-testkit)."

[lib]
name = "_bluedb_testkit"
crate-type = ["cdylib", "rlib"]

[dependencies]
bluedb-server = { path = "../bluedb-server" }
bluedb-ha     = { path = "../bluedb-ha" }
slatedb       = { workspace = true }
tokio         = { workspace = true }
anyhow        = { workspace = true }
axum          = { version = "0.8", features = ["http2"] }
pyo3          = { version = "0.22", features = ["abi3-py39"], optional = true }

[features]
default = []
# `maturin` enables this (+ pyo3/extension-module) when building the wheel.
# Default builds omit PyO3 entirely, so `cargo build`/`cargo test` need no libpython.
python = ["dep:pyo3"]

[dev-dependencies]
ureq = "2"
```

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[build-system]
requires = ["maturin>=1.5,<2.0"]
build-backend = "maturin"

[project]
name = "bluedb-testkit"
version = "0.0.0"
description = "In-process, test-only bluedb for Python integration tests."
requires-python = ">=3.9"
classifiers = ["Private :: Do Not Upload"]

[project.optional-dependencies]
test = ["pytest", "httpx"]

[project.entry-points.pytest11]
bluedb_testkit = "bluedb_testkit.pytest_plugin"

[tool.maturin]
module-name = "bluedb_testkit._bluedb_testkit"
python-source = "python"
features = ["python", "pyo3/extension-module"]
```

- [ ] **Step 3: Write a minimal `src/lib.rs`**

```rust
//! `bluedb-testkit` — a PyO3 wheel that embeds the real bluedb-server axum app
//! in the Python test process. See `docs/superpowers/specs/2026-06-16-bluedb-testkit-python-design.md`.
//!
//! The PyO3 surface lives behind the `python` feature (enabled by maturin) so the
//! pure-Rust core compiles and tests without libpython.

#[cfg(feature = "python")]
mod py {
    use pyo3::prelude::*;

    #[pymodule]
    fn _bluedb_testkit(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("DEFAULT_TOKEN", "bluedb-test-superuser")?;
        Ok(())
    }
}
```

- [ ] **Step 4: Write `python/bluedb_testkit/__init__.py` (placeholder import)**

```python
from ._bluedb_testkit import DEFAULT_TOKEN

__all__ = ["DEFAULT_TOKEN"]
```

- [ ] **Step 5: Verify it builds**

Run: `cargo build -p bluedb-py 2>&1`
Expected: compiles to a cdylib + rlib, no errors.

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-py
git commit -m "feat(testkit): scaffold bluedb-py PyO3 crate"
```

---

### Task 2: AuthzSpec → Authz translation (pure Rust, tested)

**Files:**
- Create: `crates/bluedb-py/src/authz_cfg.rs`
- Modify: `crates/bluedb-py/src/lib.rs` (declare `mod authz_cfg;`)

- [ ] **Step 1: Write the failing test (in `authz_cfg.rs`)**

```rust
//! Translate Python-supplied auth config into a server `Authz` plus the default
//! bearer token (if any) that `db.headers()` should auto-inject.
use bluedb_server::authz::{Authz, Scope};

/// Default superuser token value when auth is on and no token is supplied.
pub const DEFAULT_TOKEN: &str = "bluedb-test-superuser";

/// Normalized auth configuration handed down from the PyO3 layer.
#[derive(Debug, Clone)]
pub enum AuthzSpec {
    /// No enforcement (`serve(authz=False)`).
    Open,
    /// One superuser token, auto-injected by `db.headers()` (the default).
    DefaultSuperuser { token: String },
    /// Custom token -> items, each item a scope or `tenant:<name>`.
    Map(Vec<(String, Vec<String>)>),
    /// Raw `tok=scope,..;tok2=..` string (prod parity).
    RawEnv(String),
}

impl Default for AuthzSpec {
    fn default() -> Self {
        AuthzSpec::DefaultSuperuser { token: DEFAULT_TOKEN.to_string() }
    }
}

impl AuthzSpec {
    /// Build the optional `Authz` and the default token to auto-inject.
    /// `Open` => `(None, None)`. Custom maps/strings => no auto token.
    pub fn resolve(&self) -> anyhow::Result<(Option<Authz>, Option<String>)> {
        match self {
            AuthzSpec::Open => Ok((None, None)),
            AuthzSpec::DefaultSuperuser { token } => {
                let raw = format!("{token}=superuser");
                let authz = Authz::parse_env(&raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid default token '{token}'"))?;
                Ok((Some(authz), Some(token.clone())))
            }
            AuthzSpec::Map(entries) => {
                let raw = entries
                    .iter()
                    .map(|(tok, items)| format!("{tok}={}", items.join(",")))
                    .collect::<Vec<_>>()
                    .join(";");
                let authz = Authz::parse_env(&raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid authz map (unknown scope or empty tenant)"))?;
                Ok((Some(authz), None))
            }
            AuthzSpec::RawEnv(raw) => {
                let authz = Authz::parse_env(raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid authz string"))?;
                Ok((Some(authz), None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_superuser_with_token() {
        let (authz, token) = AuthzSpec::default().resolve().unwrap();
        let a = authz.expect("default has authz");
        assert!(a.allows(Some(DEFAULT_TOKEN), Scope::SchemaAdmin));
        assert_eq!(token.as_deref(), Some(DEFAULT_TOKEN));
    }

    #[test]
    fn open_has_no_authz_no_token() {
        let (authz, token) = AuthzSpec::Open.resolve().unwrap();
        assert!(authz.is_none());
        assert!(token.is_none());
    }

    #[test]
    fn map_builds_scoped_tokens_without_default() {
        let spec = AuthzSpec::Map(vec![
            ("reader".into(), vec!["data:read".into()]),
            ("acme".into(), vec!["data:read".into(), "tenant:acme".into()]),
        ]);
        let (authz, token) = spec.resolve().unwrap();
        let a = authz.unwrap();
        assert!(a.allows(Some("reader"), Scope::DataRead));
        assert!(!a.allows(Some("reader"), Scope::DataWrite));
        assert!(a.allows_tenant(Some("acme"), "acme"));
        assert!(!a.allows_tenant(Some("acme"), "globex"));
        assert!(token.is_none());
    }

    #[test]
    fn bad_scope_errors() {
        let spec = AuthzSpec::Map(vec![("x".into(), vec!["data:bogus".into()])]);
        assert!(spec.resolve().is_err());
    }
}
```

- [ ] **Step 2: Declare the module in `lib.rs`**

Add `mod authz_cfg;` at the crate root (ungated — the core must compile/test
without the `python` feature), after the doc comment and before `mod py`:

```rust
mod authz_cfg;
```

And update the `DEFAULT_TOKEN` export inside `mod py` to reuse the constant:

```rust
        m.add("DEFAULT_TOKEN", crate::authz_cfg::DEFAULT_TOKEN)?;
```

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p bluedb-py authz_cfg 2>&1`
Expected: 4 tests pass. (`Scope` and `Authz::allows*`/`parse_env` are public in `bluedb-server`.)

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-py/src/authz_cfg.rs crates/bluedb-py/src/lib.rs
git commit -m "feat(testkit): AuthzSpec -> Authz translation"
```

---

### Task 3: EmbeddedServer core — boot the real app on a runtime thread (tested)

**Files:**
- Create: `crates/bluedb-py/src/embedded.rs`
- Modify: `crates/bluedb-py/src/lib.rs` (declare `mod embedded;`)

- [ ] **Step 1: Write `embedded.rs` (implementation + tests together)**

```rust
//! Boot the real `bluedb-server` axum app over an in-memory store on a dedicated
//! tokio runtime thread, bound to an ephemeral loopback port. Pure Rust — no PyO3
//! — so it is unit-testable with `cargo test`.
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use bluedb_ha::{LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use slatedb::object_store::memory::InMemory;
use tokio::sync::oneshot;

use crate::authz_cfg::AuthzSpec;

/// Config for one embedded test server.
#[derive(Debug, Clone)]
pub struct EmbeddedConfig {
    pub db_path: String,
    pub admin_sql: bool,
    pub authz: AuthzSpec,
    pub evidence_signing: bool,
    pub flush_interval_ms: Option<u64>,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            db_path: "bluedb".to_string(),
            admin_sql: true,
            authz: AuthzSpec::default(),
            evidence_signing: false,
            flush_interval_ms: None,
        }
    }
}

/// A running embedded server. `shutdown()` (or `Drop`) stops it and joins the thread.
pub struct EmbeddedServer {
    base_url: String,
    token: Option<String>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl EmbeddedServer {
    /// Boot the server. Blocks until the listener is bound (or boot fails).
    pub fn start(cfg: EmbeddedConfig) -> anyhow::Result<EmbeddedServer> {
        let (authz, token) = cfg.authz.resolve()?;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<anyhow::Result<SocketAddr>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let EmbeddedConfig { db_path, admin_sql, evidence_signing, flush_interval_ms, .. } = cfg;

        let thread = std::thread::Builder::new()
            .name("bluedb-testkit".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.into()));
                        return;
                    }
                };
                rt.block_on(async move {
                    if let Some(ms) = flush_interval_ms {
                        // Read by bluedb-server at writer-open time. Process-global;
                        // pytest-xdist isolates per worker process.
                        std::env::set_var("BLUEDB_FLUSH_INTERVAL_MS", ms.to_string());
                    }
                    let object_store = Arc::new(InMemory::new());
                    let lease = Arc::new(LocalLeaseProvider::new());
                    // No HA renewal loop runs here, and the controller self-fences
                    // (goes Passive) once within `margin` of lease expiry. A single
                    // in-process writer never contends, so we use a long TTL to keep
                    // the node Active for the whole test session.
                    let writer = Arc::new(WriterController::new(
                        "testkit-node",
                        lease,
                        Arc::new(SystemClock),
                        Duration::from_secs(3600),
                        Duration::from_secs(5),
                    ));
                    let mut state = AppState::new(object_store, db_path, writer)
                        .with_admin_sql_enabled(admin_sql);
                    if evidence_signing {
                        state = state.with_local_signer_for_tests();
                    }
                    if let Some(a) = authz {
                        state = state.with_authz(a);
                    }
                    if let Err(e) = state.promote().await {
                        let _ = ready_tx.send(Err(anyhow::anyhow!("promote failed: {e:?}")));
                        return;
                    }
                    let app = build_app(state);
                    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                        Ok(l) => l,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.into()));
                            return;
                        }
                    };
                    let addr = match listener.local_addr() {
                        Ok(a) => a,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.into()));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(addr));
                    let _ = axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await;
                });
            })?;

        let addr = ready_rx.recv()??;
        Ok(EmbeddedServer {
            base_url: format!("http://{addr}"),
            token,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Signal graceful shutdown and join the runtime thread. Idempotent.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for EmbeddedServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_is_public_and_ports_are_isolated() {
        let a = EmbeddedServer::start(EmbeddedConfig::default()).unwrap();
        let b = EmbeddedServer::start(EmbeddedConfig::default()).unwrap();
        assert_ne!(a.base_url(), b.base_url(), "each instance binds its own port");

        // /health requires no token even with auth on.
        let resp = ureq::get(&format!("{}/health", a.base_url())).call().unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[test]
    fn default_auth_guards_routes() {
        let s = EmbeddedServer::start(EmbeddedConfig::default()).unwrap();
        let token = s.token().expect("default has a token").to_string();

        // No token on a guarded route -> 401.
        let unauth = ureq::get(&format!("{}/tables/nope", s.base_url())).call();
        match unauth {
            Err(ureq::Error::Status(code, _)) => assert_eq!(code, 401),
            other => panic!("expected 401, got {other:?}"),
        }

        // Superuser token -> not 401 (404/200 for a missing table is fine).
        let authed = ureq::get(&format!("{}/tables/nope", s.base_url()))
            .set("Authorization", &format!("Bearer {token}"))
            .call();
        if let Err(ureq::Error::Status(code, _)) = authed {
            assert_ne!(code, 401, "superuser token must pass authz");
        }
    }

    #[test]
    fn open_mode_has_no_token_and_allows_unauthenticated() {
        let s = EmbeddedServer::start(EmbeddedConfig {
            authz: AuthzSpec::Open,
            ..Default::default()
        })
        .unwrap();
        assert!(s.token().is_none());
        let r = ureq::get(&format!("{}/tables/nope", s.base_url())).call();
        if let Err(ureq::Error::Status(code, _)) = r {
            assert_ne!(code, 401, "open mode must not require a token");
        }
    }
}
```

- [ ] **Step 2: Declare the module in `lib.rs`**

Add `mod embedded;` at the crate root (ungated), after `mod authz_cfg;`:

```rust
mod embedded;
```

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p bluedb-py 2>&1`
Expected: the 4 `authz_cfg` tests plus the 3 `embedded` tests pass. Default features
exclude PyO3, so no libpython is involved. The first build compiles the full
`bluedb-server` dependency tree (slatedb, tantivy, axum) — allow a generous timeout
(e.g. 600000 ms).

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-py/src/embedded.rs crates/bluedb-py/src/lib.rs
git commit -m "feat(testkit): EmbeddedServer boots real app on runtime thread"
```

---

### Task 4: PyO3 TestServer wrapper

**Files:**
- Modify: `crates/bluedb-py/src/lib.rs`

- [ ] **Step 1: Replace `src/lib.rs` body with the full PyO3 surface**

The two core modules stay declared at the crate root (ungated) so `cargo test`
still builds them; everything that touches PyO3 lives inside `#[cfg(feature = "python")] mod py`.

```rust
//! `bluedb-testkit` — a PyO3 wheel that embeds the real bluedb-server axum app
//! in the Python test process. See
//! `docs/superpowers/specs/2026-06-16-bluedb-testkit-python-design.md`.
//!
//! The PyO3 surface lives behind the `python` feature (enabled by maturin) so the
//! pure-Rust core (`authz_cfg`, `embedded`) compiles and tests without libpython.

mod authz_cfg;
mod embedded;

#[cfg(feature = "python")]
mod py {
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::authz_cfg::{AuthzSpec, DEFAULT_TOKEN};
use crate::embedded::{EmbeddedConfig, EmbeddedServer};

/// A running in-process bluedb, bound to an ephemeral loopback port.
#[pyclass]
struct TestServer {
    inner: Option<EmbeddedServer>,
    #[pyo3(get)]
    base_url: String,
    #[pyo3(get)]
    token: Option<String>,
}

#[pymethods]
impl TestServer {
    #[new]
    #[pyo3(signature = (authz=None, token=None, admin_sql=true, flush_interval_ms=None, db_path=None, evidence_signing=false))]
    fn new(
        py: Python<'_>,
        authz: Option<Py<PyAny>>,
        token: Option<String>,
        admin_sql: bool,
        flush_interval_ms: Option<u64>,
        db_path: Option<String>,
        evidence_signing: bool,
    ) -> PyResult<Self> {
        let spec = parse_authz(py, authz, token)?;
        let cfg = EmbeddedConfig {
            db_path: db_path.unwrap_or_else(|| "bluedb".to_string()),
            admin_sql,
            authz: spec,
            evidence_signing,
            flush_interval_ms,
        };
        let server = py
            .allow_threads(|| EmbeddedServer::start(cfg))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(TestServer {
            base_url: server.base_url().to_string(),
            token: server.token().map(str::to_string),
            inner: Some(server),
        })
    }

    /// Stop the server and join its thread. Idempotent.
    fn stop(&mut self) {
        if let Some(mut s) = self.inner.take() {
            s.shutdown();
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> bool {
        self.stop();
        false
    }
}

/// Map the Python `authz`/`token` arguments to an `AuthzSpec`.
/// `None` => default superuser; `True`/`False` => default superuser / open;
/// `str` => raw env string; `dict[str, list[str]]` => scoped token map.
fn parse_authz(py: Python<'_>, authz: Option<Py<PyAny>>, token: Option<String>) -> PyResult<AuthzSpec> {
    let default_token = || token.clone().unwrap_or_else(|| DEFAULT_TOKEN.to_string());
    let Some(obj) = authz else {
        return Ok(AuthzSpec::DefaultSuperuser { token: default_token() });
    };
    let b = obj.bind(py);
    if let Ok(flag) = b.extract::<bool>() {
        return Ok(if flag {
            AuthzSpec::DefaultSuperuser { token: default_token() }
        } else {
            AuthzSpec::Open
        });
    }
    if let Ok(s) = b.extract::<String>() {
        return Ok(AuthzSpec::RawEnv(s));
    }
    if let Ok(d) = b.downcast::<PyDict>() {
        let mut entries = Vec::with_capacity(d.len());
        for (k, v) in d.iter() {
            let tok: String = k.extract()?;
            let items: Vec<String> = v.extract()?;
            entries.push((tok, items));
        }
        return Ok(AuthzSpec::Map(entries));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "authz must be None, bool, str, or dict[str, list[str]]",
    ))
}

#[pymodule]
fn _bluedb_testkit(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TestServer>()?;
    m.add("DEFAULT_TOKEN", DEFAULT_TOKEN)?;
    Ok(())
}

} // mod py
```

- [ ] **Step 2: Verify both the core and the PyO3 layer**

Run: `cargo test -p bluedb-py 2>&1 && cargo check -p bluedb-py --features python 2>&1`
Expected: the 7 core tests pass (default features, no PyO3); `cargo check --features python`
type-checks the PyO3 layer (validates the `#[pyclass]`/`#[pymethods]`/`#[pymodule]`
macros and argument types) **without linking libpython** — so it works regardless of
whether a linkable libpython is present. The real link + run is proven by maturin in
Task 5. (`cargo check` still runs PyO3's build script, which only needs a `python3`
interpreter on PATH to read the ABI config — present on this machine.)

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-py/src/lib.rs
git commit -m "feat(testkit): PyO3 TestServer class + authz arg parsing"
```

---

### Task 5: Python package — Handle + serve()

**Files:**
- Modify: `crates/bluedb-py/python/bluedb_testkit/__init__.py`

- [ ] **Step 1: Write the full `__init__.py`**

```python
"""In-process, test-only bluedb for Python integration tests.

Example:
    from bluedb_testkit import serve

    with serve() as db:                       # authenticated, superuser key
        import httpx
        httpx.post(db.url("/sql"), headers=db.headers(),
                   json={"sql": "SELECT 1", "params": []})
"""
from contextlib import contextmanager

from ._bluedb_testkit import DEFAULT_TOKEN, TestServer

__all__ = ["serve", "Handle", "TestServer", "DEFAULT_TOKEN"]

# Sentinel so headers(token=None) can mean "send no Authorization header",
# distinct from headers() meaning "use the default token".
_DEFAULT = object()


class Handle:
    """Ergonomic wrapper over a running :class:`TestServer`."""

    def __init__(self, server: TestServer):
        self._server = server

    @property
    def base_url(self) -> str:
        return self._server.base_url

    @property
    def token(self):
        return self._server.token

    def url(self, path: str) -> str:
        return self._server.base_url + path

    def headers(self, token=_DEFAULT, tenant: str | None = None) -> dict:
        """Build request headers. Omit ``token`` to use the default key (if any);
        pass ``token=None`` to send no Authorization header; pass a string to
        override. ``tenant`` adds ``X-Bluedb-Tenant``."""
        h: dict[str, str] = {}
        tok = self._server.token if token is _DEFAULT else token
        if tok is not None:
            h["Authorization"] = f"Bearer {tok}"
        if tenant is not None:
            h["X-Bluedb-Tenant"] = tenant
        return h

    def stop(self) -> None:
        self._server.stop()


@contextmanager
def serve(**kwargs):
    """Start an in-process bluedb and yield a :class:`Handle`. Accepts the same
    keyword args as :class:`TestServer` (``authz``, ``token``, ``admin_sql``,
    ``flush_interval_ms``, ``db_path``, ``evidence_signing``)."""
    server = TestServer(**kwargs)
    handle = Handle(server)
    try:
        yield handle
    finally:
        handle.stop()
```

- [ ] **Step 2: Build the extension into the Python env**

Run (in a venv with maturin):
```bash
cd crates/bluedb-py && pip install maturin httpx pytest && maturin develop
```
Expected: `🛠 Installed bluedb-testkit` (the extension imports as `bluedb_testkit`).

- [ ] **Step 3: Smoke-test from Python**

Run:
```bash
cd crates/bluedb-py && python -c "
from bluedb_testkit import serve
import httpx
with serve() as db:
    r = httpx.post(db.url('/sql'), headers=db.headers(), json={'sql':'SELECT 1','params':[]})
    print(db.base_url, r.status_code)
    assert r.status_code == 200, r.text
print('ok')
"
```
Expected: prints a `127.0.0.1:<port>` URL, `200`, then `ok`.

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-py/python/bluedb_testkit/__init__.py
git commit -m "feat(testkit): Python Handle + serve() context manager"
```

---

### Task 6: pytest fixtures

**Files:**
- Create: `crates/bluedb-py/python/bluedb_testkit/pytest_plugin.py`

- [ ] **Step 1: Write the plugin**

```python
"""pytest fixtures shipped with bluedb-testkit (registered via the pytest11
entry point in pyproject.toml).

    def test_query(bluedb):           # fresh, authenticated instance per test
        import httpx
        r = httpx.post(bluedb.url("/sql"), headers=bluedb.headers(),
                       json={"sql": "SELECT 1", "params": []})
        assert r.status_code == 200
"""
import pytest

from . import serve


@pytest.fixture
def bluedb():
    """Function-scoped: a fresh, isolated in-memory bluedb per test."""
    with serve() as db:
        yield db


@pytest.fixture(scope="session")
def bluedb_session():
    """Session-scoped: one shared instance for suites that don't need per-test
    isolation (faster)."""
    with serve() as db:
        yield db
```

- [ ] **Step 2: Verify the plugin loads**

Run:
```bash
cd crates/bluedb-py && maturin develop && python -c "import bluedb_testkit.pytest_plugin as p; print(p.bluedb, p.bluedb_session)"
```
Expected: prints two fixture function objects, no import error.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-py/python/bluedb_testkit/pytest_plugin.py
git commit -m "feat(testkit): bluedb / bluedb_session pytest fixtures"
```

---

### Task 7: Python integration tests

**Files:**
- Create: `crates/bluedb-py/python/tests/test_testkit.py`

- [ ] **Step 1: Write the tests**

```python
import httpx
import pytest

from bluedb_testkit import DEFAULT_TOKEN, serve


def _create_and_insert(db, headers):
    # Structured DDL (writer-gated) + a parameterized insert via /sql.
    r = httpx.post(
        db.url("/schema/tables"),
        headers=headers,
        json={"name": "t", "columns": [
            {"name": "id", "type": "INTEGER", "primary_key": True},
            {"name": "v", "type": "TEXT"},
        ]},
    )
    assert r.status_code in (200, 201), r.text
    r = httpx.post(
        db.url("/sql"),
        headers=headers,
        json={"sql": "INSERT INTO t (id, v) VALUES ($1, $2)", "params": [1, "hello"]},
    )
    assert r.status_code == 200, r.text


def test_default_auth_happy_path(bluedb):
    assert bluedb.token == DEFAULT_TOKEN
    _create_and_insert(bluedb, bluedb.headers())
    r = httpx.post(
        bluedb.url("/sql"),
        headers=bluedb.headers(),
        json={"sql": "SELECT v FROM t WHERE id = $1", "params": [1]},
    )
    assert r.status_code == 200, r.text
    assert "hello" in r.text


def test_missing_token_is_401(bluedb):
    r = httpx.get(bluedb.url("/tables/t"))  # no Authorization header
    assert r.status_code == 401


def test_wrong_scope_is_403():
    with serve(authz={"ro": ["data:read"]}) as db:
        # data:read token cannot write via /sql (needs data:query) ...
        # use a write-only check: POST /schema/tables needs schema:admin.
        r = httpx.post(
            db.url("/schema/tables"),
            headers=db.headers(token="ro"),
            json={"name": "x", "columns": [{"name": "id", "type": "INTEGER", "primary_key": True}]},
        )
        assert r.status_code == 403


def test_cross_tenant_is_403():
    # A non-superuser, tenant-bound token reaches only its own tenant.
    with serve(authz={"acme": ["data:read", "tenant:acme"]}) as db:
        ok = httpx.get(db.url("/tables/whatever"), headers=db.headers(token="acme", tenant="acme"))
        assert ok.status_code != 403  # allowed for its own tenant (404/200 fine)
        denied = httpx.get(db.url("/tables/whatever"), headers=db.headers(token="acme", tenant="globex"))
        assert denied.status_code == 403


def test_open_mode_needs_no_token():
    with serve(authz=False) as db:
        assert db.token is None
        r = httpx.get(db.url("/tables/whatever"), headers=db.headers())
        assert r.status_code != 401  # open mode allows unauthenticated


def test_instances_are_isolated():
    with serve() as a, serve() as b:
        assert a.base_url != b.base_url
```

- [ ] **Step 2: Run the tests**

Run:
```bash
cd crates/bluedb-py && maturin develop && pytest python/tests/test_testkit.py 2>&1
```
Expected: all tests pass. If `test_wrong_scope_is_403` returns 200, confirm the route's required scope in `crates/bluedb-server/src/main.rs:40-47` and adjust the asserted route/scope to a genuine mismatch.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-py/python/tests/test_testkit.py
git commit -m "test(testkit): Python integration tests (auth, isolation)"
```

---

### Task 8: README + build/CI notes

**Files:**
- Create: `crates/bluedb-py/README.md`

- [ ] **Step 1: Write the README**

````markdown
# bluedb-testkit

In-process, test-only bluedb for Python integration tests. Embeds the real
`bluedb-server` axum app over an in-memory store on an ephemeral loopback port —
no S3, no Docker, no subprocess. **Authenticated by default.**

## Install (built wheel)

```bash
pip install bluedb-testkit
```

## Use

```python
from bluedb_testkit import serve
import httpx

with serve() as db:                       # authenticated with a superuser key
    httpx.post(db.url("/sql"), headers=db.headers(),
               json={"sql": "SELECT 1", "params": []})
```

`serve(...)` knobs: `authz` (`True` default superuser / `False` open / `dict` /
raw string), `token`, `admin_sql`, `flush_interval_ms`, `db_path`,
`evidence_signing`. Helpers: `db.base_url`, `db.token`, `db.url(path)`,
`db.headers(token=..., tenant=...)`.

pytest fixtures (auto-registered): `bluedb` (fresh per test) and
`bluedb_session` (shared). Instances are isolated → safe under `pytest-xdist`.

## Build from source

```bash
pip install maturin
cd crates/bluedb-py
maturin develop            # dev install into the active venv
maturin build --release    # produce a wheel under target/wheels/
```

Requires Python dev headers. Core Rust tests: `cargo test -p bluedb-py`.
Workspace builds on Python-less machines: `cargo build --workspace --exclude bluedb-py`.
````

- [ ] **Step 2: Verify the full suite once more**

Run:
```bash
cargo test -p bluedb-py 2>&1 && (cd crates/bluedb-py && maturin develop && pytest python/tests 2>&1)
```
Expected: Rust core tests pass; Python tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-py/README.md
git commit -m "docs(testkit): usage + build notes"
```

---

## Self-Review notes

- **Spec coverage:** in-process axum boot (Task 3), default auth + opt-down/sideways (Tasks 2,4,7), thin Python surface `serve()`/`url()`/`headers()` (Task 5), pytest fixtures function+session (Task 6), isolation/xdist (Tasks 3,7), packaging maturin wheel (Tasks 1,8), out-of-scope items untouched (no HA/lakehouse wiring). Covered.
- **flush_interval_ms** is honored via the existing `BLUEDB_FLUSH_INTERVAL_MS` env read at writer-open (process-global; documented limitation, fine for xdist's per-process workers).
- **PyO3/maturin testing gotcha:** PyO3 is an optional dep behind the `python` feature (off by default); maturin enables `python` + `pyo3/extension-module`. So `cargo test -p bluedb-py` builds the pure-Rust core with no libpython, and the extension is built only by maturin (or `--features python`).
- **Type consistency:** `EmbeddedConfig`/`AuthzSpec` field and variant names match across `authz_cfg.rs`, `embedded.rs`, and `lib.rs`; `Handle.headers()` sentinel semantics match the README.
