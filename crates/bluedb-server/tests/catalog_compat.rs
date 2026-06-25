//! Iceberg **REST catalog** compatibility: an independent client (python +
//! DuckDB) discovers a mirrored table through bluedb's `/catalog/v1/*` and reads
//! it via the returned `metadata-location` — proving the catalog speaks the
//! Iceberg REST protocol and hands back warehouse-resolvable URIs.
//!
//! Runs the server over a **local-filesystem** object store (so DuckDB can read
//! the files) on a real socket. `#[ignore]` — needs python3 + the duckdb iceberg
//! extension. Run with:
//!   cargo test -p bluedb-server --test catalog_compat -- --ignored --nocapture

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::Router;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::ObjectStore;
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) {
    let builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(request).await.unwrap();
    assert!(
        resp.status().is_success(),
        "{method} {uri} -> {}",
        resp.status()
    );
    let _ = resp.into_body().collect().await;
}

// Multi-thread: the test calls a blocking python client while `axum::serve` runs
// concurrently, so a single-threaded runtime would deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs python3 + duckdb iceberg extension (network install)"]
async fn rest_catalog_is_iceberg_compatible_and_duckdb_reads_through_it() {
    // Local-fs object store at a tempdir so DuckDB can read the Parquet/metadata.
    let dir = tempfile::tempdir().unwrap();
    let abs = std::fs::canonicalize(dir.path()).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&abs).unwrap());
    let base = format!("file://{}", abs.display());

    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer)
        .with_admin_sql_enabled(true)
        .with_lakehouse_base(base);
    state.promote().await.expect("promote");
    let app = build_app(state.clone());

    // Mirror on; create + write through /sql; update + delete (equality deletes).
    call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    )
    .await;
    call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"}
            ]
        })),
    )
    .await;
    call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO docs VALUES (1,'a'),(2,'b'),(3,'c')"})),
    )
    .await;
    call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "UPDATE docs SET body='x' WHERE id=1"})),
    )
    .await;
    call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "DELETE FROM docs WHERE id=2"})),
    )
    .await;
    state.seal_now().await.expect("seal");

    // Serve on a real socket so the python client can hit the REST catalog.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let serve = app.clone();
    let handle = tokio::spawn(async move {
        axum::serve(listener, serve).await.unwrap();
    });

    let script = format!(
        r#"
import json, sys, urllib.request
BASE = "http://127.0.0.1:{port}/catalog"
def get(p):
    with urllib.request.urlopen(BASE + p) as r:
        return json.load(r)

# --- Iceberg REST protocol conformance ---
cfg = get("/v1/config")
assert "defaults" in cfg and "overrides" in cfg, cfg
ns = get("/v1/namespaces")
assert ["default"] in ns["namespaces"], ns
tbls = get("/v1/namespaces/default/tables")
names = [i["name"] for i in tbls["identifiers"]]
assert "docs" in names, tbls
lt = get("/v1/namespaces/default/tables/docs")
loc = lt["metadata-location"]
assert loc.startswith("file://") and loc.endswith(".metadata.json"), loc
md = json.dumps(lt["metadata"])
assert '"id"' in md and '"body"' in md, md

# --- Independent read THROUGH the catalog's metadata-location (DuckDB) ---
import duckdb
con = duckdb.connect(); con.execute("INSTALL iceberg"); con.execute("LOAD iceberg")
rows = [list(r) for r in con.execute(
    "SELECT id, body FROM iceberg_scan(?) ORDER BY id", [loc]
).fetchall()]
assert rows == [[1, "x"], [3, "c"]], rows
print("CATALOG_COMPAT_OK", rows)
"#
    );

    // Run the blocking python client off the async executor so `axum::serve`
    // keeps making progress.
    let out = tokio::task::spawn_blocking(move || {
        Command::new("python3")
            .arg("-c")
            .arg(&script)
            .output()
            .expect("python3")
    })
    .await
    .unwrap();
    handle.abort();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("CATALOG_COMPAT_OK"),
        "catalog compat failed:\n--- stdout ---\n{stdout}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    println!("{}", stdout.trim());
}
