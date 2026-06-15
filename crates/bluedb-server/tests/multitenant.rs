//! End-to-end multi-tenancy: the `X-Bluedb-Tenant` header routes writes/reads to
//! isolated keyspaces, each tenant mirrors into its own Iceberg namespace, and
//! `tenant:`-bound tokens can't cross tenants.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{authz::Authz, build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

async fn promoted(authz: Option<Authz>) -> AppState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let mut state = AppState::new(store, "bluedb", writer).with_admin_sql_enabled(true);
    if let Some(a) = authz {
        state = state.with_authz(a);
    }
    state.promote().await.expect("promote");
    state
}

/// Call with an optional tenant header and optional bearer token.
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: Option<&str>,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-bluedb-tenant", t);
    }
    if let Some(tok) = token {
        builder = builder.header("authorization", format!("Bearer {tok}"));
    }
    let request = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

/// Mirror-on + create `docs` + insert one row, all under `tenant`.
async fn seed(app: &Router, tenant: &str, id: i64, body: &str) {
    let (s, b) = call(
        app,
        "POST",
        "/sql",
        Some(tenant),
        None,
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    )
    .await;
    assert!(s.is_success(), "pragma on [{tenant}]: {s} {b}");
    let (s, _) = call(
        app,
        "POST",
        "/schema/tables",
        Some(tenant),
        None,
        Some(json!({
            "name": "docs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create docs [{tenant}]: {s}");
    let (s, _) = call(
        app,
        "POST",
        "/sql",
        Some(tenant),
        None,
        Some(json!({"sql": format!("INSERT INTO docs VALUES ({id}, '{body}')")})),
    )
    .await;
    assert!(s.is_success(), "insert [{tenant}]: {s}");
}

#[tokio::test]
async fn tenants_are_isolated_end_to_end() {
    let state = promoted(None).await;
    let app = build_app(state.clone());

    seed(&app, "acme", 1, "acme-row").await;
    seed(&app, "globex", 1, "globex-row").await;
    state.seal_now().await.expect("seal");

    // Reads are isolated: each tenant's `docs` holds only its own row, even
    // though both use id=1.
    let (s, acme) = call(&app, "GET", "/tables/docs", Some("acme"), None, None).await;
    assert!(s.is_success(), "acme read: {s}");
    assert!(
        acme.to_string().contains("acme-row"),
        "acme sees its row: {acme}"
    );
    assert!(
        !acme.to_string().contains("globex-row"),
        "acme must not see globex: {acme}"
    );

    let (s, globex) = call(&app, "GET", "/tables/docs", Some("globex"), None, None).await;
    assert!(s.is_success(), "globex read: {s}");
    assert!(globex.to_string().contains("globex-row"));
    assert!(!globex.to_string().contains("acme-row"));

    // Catalog: each tenant publishes its own Iceberg namespace.
    let (s, ns) = call(&app, "GET", "/catalog/v1/namespaces", None, None, None).await;
    assert!(s.is_success());
    let names: Vec<&str> = ns["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n[0].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"acme") && names.contains(&"globex"),
        "namespaces: {names:?}"
    );

    // loadTable resolves per-namespace and stays isolated.
    let (s, load) = call(
        &app,
        "GET",
        "/catalog/v1/namespaces/acme/tables/docs",
        None,
        None,
        None,
    )
    .await;
    assert!(s.is_success(), "load acme.docs: {s} {load}");
    assert!(load["metadata-location"]
        .as_str()
        .unwrap()
        .ends_with(".metadata.json"));
}

#[tokio::test]
async fn tenant_bound_token_cannot_cross_tenants() {
    // acme-token is bound to tenant acme; root is superuser (any tenant).
    let authz = Authz::parse_env(
        "acmetok=data:read,data:write,data:query,schema:admin,tenant:acme;root=superuser",
    )
    .unwrap();
    let state = promoted(Some(authz)).await;
    let app = build_app(state.clone());

    // The bound token works for its own tenant.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some("acme"),
        Some("acmetok"),
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "acme token on acme tenant should work");

    // ...but is forbidden on another tenant.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some("globex"),
        Some("acmetok"),
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "acme token must not reach globex");

    // A superuser token reaches any tenant.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some("globex"),
        Some("root"),
        Some(json!({"sql": "PRAGMA lakehouse_mirror = on"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "superuser reaches globex");
}
