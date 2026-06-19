//! Boot the real `bluedb-server` axum app over an in-memory store on a dedicated
//! tokio runtime thread, bound to an ephemeral loopback port. Pure Rust — no PyO3
//! — so it is unit-testable with `cargo test`.
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use bluedb_ha::{LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use tokio::runtime::Handle;
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
    /// Run with the lakehouse mirror on, backed by a local temp object store.
    pub mirror: bool,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            db_path: "bluedb".to_string(),
            admin_sql: true,
            authz: AuthzSpec::default(),
            evidence_signing: false,
            flush_interval_ms: None,
            mirror: false,
        }
    }
}

/// A running embedded server. `shutdown()` (or `Drop`) stops it and joins the thread.
pub struct EmbeddedServer {
    base_url: String,
    token: Option<String>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    /// Mirror mode: the temp warehouse dir, kept alive so `Drop` cleans it.
    warehouse: Option<tempfile::TempDir>,
    /// Handle to the server's runtime, for driving `seal()` synchronously.
    runtime_handle: Handle,
    /// A clone of the running app's state (shared `Arc` inner) for `seal()`.
    app_state: AppState,
}

impl EmbeddedServer {
    /// Boot the server. Blocks until the listener is bound (or boot fails).
    pub fn start(cfg: EmbeddedConfig) -> anyhow::Result<EmbeddedServer> {
        let (authz, token) = cfg.authz.resolve()?;
        let (ready_tx, ready_rx) =
            std::sync::mpsc::channel::<anyhow::Result<(SocketAddr, Handle, AppState)>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Mirror mode: a temp warehouse dir kept alive on this (caller) side so
        // `Drop` cleans it; the runtime thread builds a LocalFileSystem over its path.
        let warehouse = if cfg.mirror { Some(tempfile::tempdir()?) } else { None };
        let warehouse_path = warehouse.as_ref().map(|d| d.path().to_path_buf());

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
                // The caller drives `seal()` via this handle; the runtime stays
                // alive for the server's lifetime under `rt.block_on` below.
                let handle = rt.handle().clone();
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
                    if let Some(p) = &warehouse_path {
                        // Mirror to a local temp dir under a file:// base so the
                        // Iceberg metadata a warehouse (DuckDB) loads has resolvable
                        // locations; default mirroring on for every tenant.
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
                    let state_for_handle = state.clone();
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
                    let _ = ready_tx.send(Ok((addr, handle.clone(), state_for_handle)));
                    let _ = axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        })
                        .await;
                });
            })?;

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
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Synchronously seal buffered writes into the Iceberg mirror (mirror mode).
    /// On a non-mirror server this is a no-op (no manager bound).
    pub fn seal(&self) -> anyhow::Result<()> {
        self.runtime_handle
            .block_on(self.app_state.seal_now())
            .map_err(|e| anyhow::anyhow!("seal failed: {e:?}"))
    }

    /// The local warehouse directory (mirror mode), else `None`.
    pub fn warehouse_path(&self) -> Option<String> {
        self.warehouse.as_ref().map(|d| d.path().display().to_string())
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

    #[test]
    fn mirror_mode_seals_and_serves_file_uris_with_type_fidelity() {
        let s = EmbeddedServer::start(EmbeddedConfig { mirror: true, ..Default::default() }).unwrap();
        let bearer = format!("Bearer {}", s.token().unwrap());
        let sql = |body: &str| {
            ureq::post(&format!("{}/admin/sql", s.base_url()))
                .set("Authorization", &bearer)
                .set("Content-Type", "application/json")
                .send_string(body)
                .unwrap()
        };
        sql(r#"{"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY, amt DECIMAL(10,2), d DATE, ts TIMESTAMP, uid UUID)"}"#);
        sql(r#"{"sql":"INSERT INTO t VALUES (1, 9.99, DATE '2026-01-01', TIMESTAMP '2026-01-01 00:00:00', GENERATE_UUID())"}"#);

        s.seal().expect("seal");

        let body = ureq::get(&format!("{}/catalog/v1/namespaces/default/tables/t", s.base_url()))
            .set("Authorization", &bearer)
            .call()
            .unwrap()
            .into_string()
            .unwrap();
        let meta: serde_json::Value = serde_json::from_str(&body).unwrap();
        let loc = meta["metadata-location"].as_str().expect("metadata-location");
        assert!(loc.starts_with("file://"), "loadTable URI must be file://, got {loc}");
        // Type fidelity: the Iceberg schema reports uuid + decimal + date + timestamp.
        let fields = meta["metadata"]["schemas"][0]["fields"].to_string();
        for ty in ["uuid", "decimal", "date", "timestamp"] {
            assert!(fields.contains(ty), "{ty} type must survive into the mirror: {fields}");
        }
        assert!(!s.warehouse_path().unwrap().is_empty());
    }

    #[test]
    fn watermark_header_and_underscore_tenant_namespace() {
        let s = EmbeddedServer::start(EmbeddedConfig { mirror: true, ..Default::default() }).unwrap();
        let bearer = format!("Bearer {}", s.token().unwrap());
        let tenant = "ws1_sol2_copa_collection_v2";
        ureq::post(&format!("{}/admin/sql", s.base_url()))
            .set("Authorization", &bearer)
            .set("X-Bluedb-Tenant", tenant)
            .set("Content-Type", "application/json")
            .send_string(r#"{"sql":"CREATE TABLE t (id INTEGER PRIMARY KEY)"}"#)
            .unwrap();
        let w = ureq::post(&format!("{}/tables/t", s.base_url()))
            .set("Authorization", &bearer)
            .set("X-Bluedb-Tenant", tenant)
            .set("Content-Type", "application/json")
            .send_string(r#"{"id":1}"#)
            .unwrap();
        assert!(w.header("X-Bluedb-Watermark").is_some(), "write must surface a watermark");
        s.seal().unwrap();
        let body = ureq::get(&format!("{}/catalog/v1/namespaces/{tenant}/tables", s.base_url()))
            .set("Authorization", &bearer)
            .call()
            .unwrap()
            .into_string()
            .unwrap();
        assert!(body.contains("\"name\":\"t\""), "namespace==tenant lists the table: {body}");
    }
}
