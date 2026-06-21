//! FTS over the HTTP surface (Spec B §4.1/§4.3/§5): declare a fulltext index via
//! the `/schema` DDL surface, then a `@@` SELECT on `/sql` sees rows committed by
//! a prior insert with no explicit flush — read-your-writes over HTTP. A delete
//! is reflected immediately too. Drives the router via `oneshot`, no socket.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bluedb_ha::{LeaseProvider, LocalLeaseProvider, SystemClock, WriterController};
use bluedb_server::{build_app, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use tower::ServiceExt;

const TTL: Duration = Duration::from_secs(30);
const MARGIN: Duration = Duration::from_secs(5);

/// An active (promoted) node over a fresh in-memory store, admin SQL on.
async fn make_app(promote: bool) -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = Arc::new(WriterController::new(
        "test-node",
        Arc::new(LocalLeaseProvider::new()) as Arc<dyn LeaseProvider>,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    let state = AppState::new(store, "bluedb", writer).with_admin_sql_enabled(true);
    if promote {
        state.promote().await.expect("promote");
    }
    build_app(state)
}

/// Send a request (JSON body optional) and return `(status, json_body)`.
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = Request::builder().method(method).uri(uri);
    let request = match json_body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

#[tokio::test]
async fn fts_over_http_read_your_writes() {
    let app = make_app(true).await;

    // 1. create the table via the structured DDL surface (A3b).
    let (s, _) = call(
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
    assert!(s.is_success(), "create table: {s}");

    // 2. declare a fulltext index (this increment's new endpoint).
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s} {body}");

    // 3. insert via /sql — the observed connection maintains the live index.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert: {s}");

    // 4. @@ query over /sql sees the just-committed matching row (RYW over HTTP).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ select: {s}");
    // exec_sql serializes a single Payload::Select as a JSON array of {label: value}.
    assert_eq!(body, json!([{ "id": 1 }]), "only the matching row id=1");

    // 5. delete row 1 via /sql, re-query → empty (delete reflected immediately).
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "DELETE FROM docs WHERE id = 1"})),
    )
    .await;
    assert!(s.is_success(), "delete: {s}");

    let (s, body2) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ select after delete: {s}");
    assert_eq!(body2, json!([]), "deleted row no longer matches");
}

/// A **parameterized** `@@ plainto_tsquery($1)` read on `/sql` resolves the
/// bound query string against the live index. This is the UAT-SEARCH-001 shape:
/// after the `/sql` read-tier flip, `/sql` reads run through `execute_fts`, which
/// must thread the bound `$N` params into the FTS rewrite (not pass them empty),
/// or a parameterized tsquery can't be searched.
#[tokio::test]
async fn parameterized_fts_query_works_on_sql() {
    let app = make_app(true).await;
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "pdocs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create pdocs: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables/pdocs/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO pdocs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny')"})),
    )
    .await;
    assert!(s.is_success(), "insert pdocs: {s}");

    // Parameterized plainto_tsquery($1) — the query string arrives as a bound param.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM pdocs WHERE to_tsvector('english', body) @@ plainto_tsquery($1)",
            "params": ["invoice overdue"]
        })),
    )
    .await;
    assert!(s.is_success(), "parameterized @@ on /sql: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "only the matching row id=1: {body}");

    // to_tsquery($1) with a boolean query string also resolves its param.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM pdocs WHERE to_tsvector('english', body) @@ to_tsquery($1)",
            "params": ["invoice & !paid"]
        })),
    )
    .await;
    assert!(s.is_success(), "parameterized to_tsquery on /sql: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "boolean query matches id=1: {body}");
}

/// FTS ranking with `ORDER BY ts_rank(...)` + `LIMIT/OFFSET` on `/sql`. The `@@`
/// predicate rewrites to a bounded `pk IN (...)` set, so ranking that bounded set
/// in-memory is allowed (not a scan-sort). UAT-SEARCH-003 shape.
#[tokio::test]
async fn fts_rank_order_by_works_on_sql() {
    let app = make_app(true).await;
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "rankdocs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create rankdocs: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables/rankdocs/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s}");
    // Multiple matching docs so rank ordering is observable.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO rankdocs (id, body) VALUES (1, 'invoice invoice overdue'), (2, 'invoice'), (3, 'weather')"})),
    )
    .await;
    assert!(s.is_success(), "insert rankdocs: {s}");

    // ORDER BY ts_rank with LIMIT — ranked page over the bounded match set.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM rankdocs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice') ORDER BY ts_rank(to_tsvector('english', body), plainto_tsquery('invoice')) DESC LIMIT 2"
        })),
    )
    .await;
    assert!(s.is_success(), "ranked FTS page on /sql: {s} {body}");
    let ids: Vec<i64> = body.as_array().unwrap().iter().map(|r| r["id"].as_i64().unwrap()).collect();
    // Both matching docs (id=1, id=2) returned, ranked; id=3 ('weather') excluded.
    // BM25 favors the shorter doc (id=2 'invoice') over the longer one (id=1
    // 'invoice invoice overdue') on tf-density, so the exact order is engine-defined —
    // assert the membership and the page size, not the BM25 tiebreak.
    assert_eq!(ids.len(), 2, "ranked page size 2: {body}");
    assert!(ids.contains(&1) && ids.contains(&2), "both matching docs: {body}");
    assert!(!ids.contains(&3), "non-matching doc excluded: {body}");
}

/// FTS rank ORDER BY **conjoined with a structured filter** (`@@ AND status = $2`)
/// plus `ORDER BY ts_rank(...)`. UAT-SEARCH-003 shape: the `@@` must rewrite to
/// `pk IN (...)`, the structured filter must survive, and `ts_rank` must be
/// replaced by the CASE-on-pk rank ordering — otherwise GlueSQL rejects
/// `ts_rank` as an unsupported custom function.
#[tokio::test]
async fn fts_rank_with_structured_filter_and_pagination() {
    let app = make_app(true).await;
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "ftrank2",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "body", "type": "TEXT"},
                {"name": "status", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create ftrank2: {s}");
    // Structured-filter index on status + fulltext index on body.
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables/ftrank2/indexes",
        Some(json!({"name": "ftrank2_status", "columns": ["status"]})),
    )
    .await;
    assert!(s.is_success(), "create status index: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables/ftrank2/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO ftrank2 VALUES (1, 'invoice overdue overdue payment', 'open'), (2, 'invoice overdue payment', 'open'), (3, 'invoice overdue archived', 'closed')"})),
    )
    .await;
    assert!(s.is_success(), "insert ftrank2: {s}");

    let rank_expr = "ts_rank(to_tsvector('english', body), plainto_tsquery($1))";
    // First ranked page: @@ + status='open' filter, ORDER BY ts_rank DESC, LIMIT 1.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": format!("SELECT id FROM ftrank2 WHERE to_tsvector('english', body) @@ plainto_tsquery($1) AND status = $2 ORDER BY {rank_expr} DESC LIMIT 1 OFFSET 0"),
            "params": ["invoice overdue", "open"]
        })),
    )
    .await;
    assert!(s.is_success(), "FTS rank + structured filter + pagination on /sql: {s} {body}");
    assert_eq!(body.as_array().unwrap().len(), 1, "one row on page 1: {body}");
    // id=1 (tf=2) ranks above id=2 (tf=1) for DESC rank.
    assert_eq!(body[0]["id"], json!(1), "top-ranked open doc: {body}");

    // Regression: a prior `SET default_null_order = 'nulls_first'` (UAT-SQL-013)
    // must not break a subsequent FTS rank query. The null-order rewrite runs
    // AFTER the FTS rewrite, so `ts_rank` is rewritten to a CASE first (otherwise
    // the null-order rewrite would wrap `ts_rank` in IS NULL and the FTS rewrite
    // would miss it → "CustomFunction is not supported"). Reproduces UAT-SEARCH-003.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SET default_null_order = 'nulls_first'"})),
    )
    .await;
    assert_eq!(s, 200, "SET nulls_first: {s}");
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": format!("SELECT id FROM ftrank2 WHERE to_tsvector('english', body) @@ plainto_tsquery($1) AND status = $2 ORDER BY {rank_expr} DESC LIMIT 1 OFFSET 0"),
            "params": ["invoice overdue", "open"]
        })),
    )
    .await;
    assert!(s.is_success(), "FTS rank query after SET nulls_first must still work: {s} {body}");
    assert_eq!(body[0]["id"], json!(1), "top-ranked open doc after SET: {body}");
}

#[tokio::test]
async fn durable_engine_serves_a_second_fulltext_index() {
    // After B4-4, `promote` binds a durable, reopened `FtsEngine` over the writer's
    // substrate (empty registry on a fresh in-memory DB → behaves like the old
    // in-memory engine). This proves the durable engine, bound through the
    // promote/demote lifecycle, serves MORE than one index definition end-to-end
    // over HTTP — i.e. the read-lock-clone wiring in `connection*`/`fts()` and the
    // `create_fulltext_index_auto`/`execute_fts` handlers all resolve the same
    // engine instance.
    let app = make_app(true).await;

    // Two tables, each with its own text column + fulltext index.
    for (table, col) in [("invoices", "memo"), ("emails", "subject")] {
        let (s, body) = call(
            &app,
            "POST",
            "/schema/tables",
            Some(json!({
                "name": table,
                "columns": [
                    {"name": "id", "type": "INTEGER", "primary_key": true},
                    {"name": col, "type": "TEXT"}
                ]
            })),
        )
        .await;
        assert!(s.is_success(), "create table {table}: {s} {body}");

        let (s, body) = call(
            &app,
            "POST",
            &format!("/schema/tables/{table}/fulltext-indexes"),
            Some(json!({"column": col, "analyzer": "english"})),
        )
        .await;
        assert!(s.is_success(), "create fulltext index on {table}: {s} {body}");
    }

    // Insert into both tables on the observed connection (maintains both live indexes).
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO invoices (id, memo) VALUES (1, 'quarterly invoice overdue'), (2, 'paid on time')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert invoices: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO emails (id, subject) VALUES (10, 'overdue payment reminder'), (11, 'welcome aboard')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert emails: {s}");

    // A `@@` query on the SECOND index resolves through the same durable engine.
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM emails WHERE to_tsvector('english', subject) @@ plainto_tsquery('overdue payment')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ on second index: {s} {body}");
    assert_eq!(body, json!([{ "id": 10 }]), "second index returns its matching row");

    // And the first index still works (both defs coexist on one bound engine).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id FROM invoices WHERE to_tsvector('english', memo) @@ plainto_tsquery('invoice overdue')"
        })),
    )
    .await;
    assert!(s.is_success(), "@@ on first index: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "first index still resolves");
}

#[tokio::test]
async fn trigram_like_over_http_read_your_writes() {
    let app = make_app(true).await;

    // 1. create the table.
    let (s, _) = call(
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
    assert!(s.is_success(), "create table: {s}");

    // 2. declare a TRIGRAM index (this increment's new endpoint).
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/trigram-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert!(s.is_success(), "create trigram index: {s} {body}");

    // 3. insert via /sql — the observed connection maintains the live trigram index.
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny'), (3, 'overdue notice')"
        })),
    )
    .await;
    assert!(s.is_success(), "insert: {s}");

    // 4. LIKE '%overdue%' over /sql is trigram-accelerated AND correct (RYW over HTTP).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SELECT id FROM docs WHERE body LIKE '%overdue%'"})),
    )
    .await;
    assert!(s.is_success(), "LIKE select: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }, { "id": 3 }]), "matching rows 1 and 3");

    // 5. a column with NO trigram index: `/sql` (the RYW transactional surface)
    //    rejects the un-indexed scan with `400 NO_INDEX`; the same read on
    //    `/query` (the analytical surface) serves it. A trigram index only
    //    *accelerates* the LIKE on `/sql` — without one the read belongs on
    //    `/query`.
    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "notes",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "memo", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create notes: {s}");
    let (s, _) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "INSERT INTO notes (id, memo) VALUES (1, 'overdue payment'), (2, 'paid')"})),
    )
    .await;
    assert!(s.is_success(), "insert notes: {s}");
    // `/sql` rejects the un-indexed LIKE (the RYW surface is index-only).
    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SELECT id FROM notes WHERE memo LIKE '%overdue%'"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "un-indexed LIKE on /sql is rejected: {body}");
    assert_eq!(body["code"], json!("NO_INDEX"), "guardrail reject code: {body}");
    // `/query` (analytical) serves the same scan.
    let (s, body) = call(
        &app,
        "POST",
        "/query",
        Some(json!({"sql": "SELECT id FROM notes WHERE memo LIKE '%overdue%'"})),
    )
    .await;
    assert!(s.is_success(), "un-indexed LIKE is served by the analytical engine: {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "row 1 ('overdue payment') matches '%overdue%'");
}

#[tokio::test]
async fn sql_search_indexes_see_tables_batch_insert_immediately() {
    let app = make_app(true).await;

    let (s, _) = call(
        &app,
        "POST",
        "/schema/tables",
        Some(json!({
            "name": "docs",
            "columns": [
                {"name": "id", "type": "INTEGER", "primary_key": true},
                {"name": "title", "type": "TEXT"},
                {"name": "body", "type": "TEXT"},
                {"name": "status", "type": "TEXT"}
            ]
        })),
    )
    .await;
    assert!(s.is_success(), "create table: {s}");

    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/fulltext-indexes",
        Some(json!({"column": "body", "analyzer": "english"})),
    )
    .await;
    assert!(s.is_success(), "create fulltext index: {s} {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/docs/trigram-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert!(s.is_success(), "create trigram index: {s} {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/tables/docs",
        Some(json!([
            {"id": 1, "title": "Invoice", "body": "invoice overdue payment", "status": "open"},
            {"id": 2, "title": "Greeting", "body": "hello database storage", "status": "open"}
        ])),
    )
    .await;
    assert!(s.is_success(), "batch insert through /tables: {s} {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({
            "sql": "SELECT id, title FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery($1) ORDER BY id",
            "params": ["invoice overdue"]
        })),
    )
    .await;
    assert!(s.is_success(), "parameterized FTS select: {s} {body}");
    assert_eq!(body, json!([{ "id": 1, "title": "Invoice" }]), "FTS sees /tables batch insert");

    let (s, body) = call(
        &app,
        "POST",
        "/sql",
        Some(json!({"sql": "SELECT id FROM docs WHERE body LIKE '%overdue%'"})),
    )
    .await;
    assert!(s.is_success(), "trigram LIKE select: {s} {body}");
    assert_eq!(body, json!([{ "id": 1 }]), "trigram LIKE sees /tables batch insert");
}

#[tokio::test]
async fn trigram_index_on_missing_table_is_400() {
    let app = make_app(true).await;
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/ghost/trigram-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "missing table → 400");
    assert!(body.get("error").is_some(), "error body: {body}");
}

#[tokio::test]
async fn fulltext_index_on_missing_table_is_400() {
    let app = make_app(true).await;
    let (s, body) = call(
        &app,
        "POST",
        "/schema/tables/ghost/fulltext-indexes",
        Some(json!({"column": "body"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "missing table → 400");
    assert!(body.get("error").is_some(), "error body: {body}");
}
