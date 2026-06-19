//! Integration tests for the `/collections` API.

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

fn node(node_id: &str, store: Arc<dyn ObjectStore>, lease: Arc<dyn LeaseProvider>) -> AppState {
    let writer = Arc::new(WriterController::new(node_id, lease, Arc::new(SystemClock), TTL, MARGIN));
    AppState::new(store, "bluedb", writer)
}

async fn app_with_state() -> (Router, AppState) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()));
    state.promote().await.expect("promote");
    let router = build_app(state.clone());
    (router, state)
}

async fn app() -> Router {
    app_with_state().await.0
}

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
async fn insert_single_doc_generates_id_and_returns_count() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "documents": [{ "name": "ada", "age": 36 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");

    let inserted_count = body["insertedCount"].as_i64().expect("insertedCount missing");
    assert_eq!(inserted_count, 1);

    let ids = body["insertedIds"].as_array().expect("insertedIds missing");
    assert_eq!(ids.len(), 1);

    let id_str = ids[0].as_str().expect("insertedIds[0] is not a string");
    assert_eq!(id_str.len(), 24, "expected 24-char _id, got '{id_str}'");
}

#[tokio::test]
async fn insert_preserves_provided_id() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/things/insert",
        Some(json!({ "documents": [{ "_id": "my-custom-id", "val": 1 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    let ids = body["insertedIds"].as_array().expect("insertedIds missing");
    assert_eq!(ids[0].as_str().unwrap(), "my-custom-id");
}

#[tokio::test]
async fn insert_multiple_docs_returns_all_ids() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/items/insert",
        Some(json!({ "documents": [{ "x": 1 }, { "x": 2 }, { "x": 3 }] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 3);
    let ids = body["insertedIds"].as_array().unwrap();
    assert_eq!(ids.len(), 3);
    // All generated ids are distinct.
    let id_strs: Vec<&str> = ids.iter().map(|v| v.as_str().unwrap()).collect();
    let unique: std::collections::HashSet<_> = id_strs.iter().collect();
    assert_eq!(unique.len(), 3, "ids should be distinct: {id_strs:?}");
}

#[tokio::test]
async fn insert_empty_documents_returns_zero() {
    let app = app().await;
    let (status, body) = call(
        &app,
        "POST",
        "/collections/empty_test/insert",
        Some(json!({ "documents": [] })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "unexpected status: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 0);
}

#[tokio::test]
async fn insert_missing_documents_field_is_400() {
    let app = app().await;
    let (status, _body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "docs": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn find_by_id_and_by_field_with_sort_limit() {
    let (app, state) = app_with_state().await;

    // Insert three documents.
    for d in [
        serde_json::json!({"name": "ada", "age": 36}),
        serde_json::json!({"name": "lin", "age": 28}),
        serde_json::json!({"name": "sam", "age": 41}),
    ] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/people/insert",
            Some(serde_json::json!({ "documents": [d] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert failed: {body}");
    }

    // Seal so the Iceberg mirror has the data (DataFusion fallback requires it).
    state.seal_now().await.expect("seal");

    // Filter by age >= 30 (non-indexed field → guardrail reject → DataFusion),
    // sorted ascending by age, limit 10.
    let (status, body) = call(
        &app,
        "POST",
        "/collections/people/find",
        Some(serde_json::json!({
            "filter": { "age": { "$gte": 30 } },
            "sort": { "age": 1 },
            "limit": 10
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "find failed: {body}");

    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 2, "expected 2 docs (ada + sam), got: {body}");

    let name0 = docs[0]["name"].as_str().expect("docs[0].name");
    let name1 = docs[1]["name"].as_str().expect("docs[1].name");
    assert_eq!(name0, "ada", "first doc (age 36) should be ada");
    assert_eq!(name1, "sam", "second doc (age 41) should be sam");

    // Each document should have a 24-char _id.
    let id0 = docs[0]["_id"].as_str().expect("docs[0]._id");
    assert_eq!(id0.len(), 24, "expected 24-char _id, got '{id0}'");
}

#[tokio::test]
async fn second_insert_reuses_existing_table() {
    let app = app().await;
    // First insert creates the table.
    let (s1, _) = call(
        &app,
        "POST",
        "/collections/reuse/insert",
        Some(json!({ "documents": [{ "n": 1 }] })),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);

    // Second insert must NOT fail with "table already exists".
    let (s2, body) = call(
        &app,
        "POST",
        "/collections/reuse/insert",
        Some(json!({ "documents": [{ "n": 2 }] })),
    )
    .await;
    assert_eq!(s2, StatusCode::OK, "second insert failed: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 1);
}

// ---------------------------------------------------------------------------
// createIndex tests
// ---------------------------------------------------------------------------

/// Implicit collection create via insert, then createIndex, then insert-after-
/// index, then find by the indexed field — all on the GlueSQL fast path (no
/// seal needed because the derived column is a real indexed column).
#[tokio::test]
async fn create_index_then_find_by_indexed_field_is_fresh() {
    let app = app().await;

    // Step 1: create the collection implicitly with one insert.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/users/insert",
        Some(json!({ "documents": [{ "name": "pre", "status": "old" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "first insert: {body}");

    // Step 2: create a JSON-path index on "status".
    let (s, body) = call(
        &app,
        "POST",
        "/collections/users/createIndex",
        Some(json!({ "keys": { "status": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");
    let index_name = body["name"].as_str().expect("name in response");
    assert!(index_name.contains("status"), "expected status in index name, got {index_name}");

    // Step 3: insert two more docs after the index exists.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/users/insert",
        Some(json!({ "documents": [
            { "name": "ada", "status": "active" },
            { "name": "lin", "status": "idle" }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert after index: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 2);

    // Step 4: find by indexed field — must return exactly the "active" doc.
    // No seal_now needed: the index-backed find uses the GlueSQL fast path.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/users/find",
        Some(json!({ "filter": { "status": "active" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc, got: {body}");
    assert_eq!(docs[0]["name"].as_str().unwrap(), "ada");
    let id = docs[0]["_id"].as_str().expect("_id in doc");
    assert_eq!(id.len(), 24, "expected 24-char _id, got '{id}'");
}

/// createIndex on a non-empty collection backfills docs that were inserted
/// BEFORE the index was created, so a subsequent find by that field still
/// returns them.
#[tokio::test]
async fn create_index_backfills_pre_existing_docs() {
    let app = app().await;

    // Insert a doc first, BEFORE the index exists.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/things/insert",
        Some(json!({ "documents": [{ "name": "widget", "kind": "tool" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "pre-index insert: {body}");

    // Create the index — this must backfill the doc above.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/things/createIndex",
        Some(json!({ "keys": { "kind": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // find by kind — the pre-existing doc must be found on the fast path.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/things/find",
        Some(json!({ "filter": { "kind": "tool" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 backfilled doc, got: {body}");
    assert_eq!(docs[0]["name"].as_str().unwrap(), "widget");
}

/// createIndex with a malformed path (DDL-injection attempt) is rejected with a
/// 4xx and leaves the collection intact: inserts still work after the rejection.
#[tokio::test]
async fn create_index_invalid_path_is_rejected_and_collection_intact() {
    let app = app().await;

    // Create the collection first.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "documents": [{ "name": "ada" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert before injection attempt: {body}");

    // Attempt the injection — must be rejected.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/people/createIndex",
        Some(json!({ "keys": { "x) TEXT; DROP TABLE people; --": 1 } })),
    )
    .await;
    assert!(s.is_client_error(), "expected 4xx for injection path, got {s}: {body}");

    // The collection is intact: inserting another document must still succeed,
    // proving that no DROP TABLE was executed.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/people/insert",
        Some(json!({ "documents": [{ "name": "lin" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert after injection attempt should succeed: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 1);
}

// ---------------------------------------------------------------------------
// update / delete tests
// ---------------------------------------------------------------------------

/// Basic update + delete round-trip: insert, $set a field, find to confirm
/// the field changed, delete, confirm gone.
#[tokio::test]
async fn update_set_then_find_reflects_change() {
    let app = app().await;

    // Insert.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/peeps/insert",
        Some(json!({ "documents": [{ "name": "ada", "age": 36 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");
    let id = body["insertedIds"][0].as_str().unwrap().to_string();

    // Update age.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/peeps/update",
        Some(json!({ "filter": { "_id": id }, "update": { "$set": { "age": 37 } } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "update: {body}");
    assert_eq!(body["matchedCount"].as_i64().unwrap(), 1);
    assert_eq!(body["modifiedCount"].as_i64().unwrap(), 1);
    assert!(body["upsertedId"].is_null());

    // Find — must see age 37.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/peeps/find",
        Some(json!({ "filter": { "_id": id } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find after update: {body}");
    let docs = body["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["age"].as_i64().unwrap(), 37);

    // Delete.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/peeps/delete",
        Some(json!({ "filter": { "_id": id } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "delete: {body}");
    assert_eq!(body["deletedCount"].as_i64().unwrap(), 1);

    // Find again — must return 0 docs.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/peeps/find",
        Some(json!({ "filter": { "_id": id } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find after delete: {body}");
    assert_eq!(body["documents"].as_array().unwrap().len(), 0);
}

/// upsert: update with no match + upsert:true inserts a new document and
/// returns its id; a subsequent find by the upserted field returns it.
#[tokio::test]
async fn update_with_upsert_inserts_when_no_match() {
    let app = app().await;

    // Upsert into an empty collection.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/contacts/update",
        Some(json!({
            "filter": { "email": "x@y.z" },
            "update": { "$set": { "name": "new", "email": "x@y.z" } },
            "upsert": true
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "upsert: {body}");
    assert_eq!(body["matchedCount"].as_i64().unwrap(), 0);
    assert_eq!(body["modifiedCount"].as_i64().unwrap(), 0);
    let upserted_id = body["upsertedId"].as_str().expect("upsertedId should be a string");
    assert_eq!(upserted_id.len(), 24, "expected 24-char id, got '{upserted_id}'");

    // find by the field set during upsert.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/contacts/find",
        Some(json!({ "filter": { "_id": upserted_id } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find after upsert: {body}");
    let docs = body["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 1, "expected 1 upserted doc: {body}");
    assert_eq!(docs[0]["name"].as_str().unwrap(), "new");
}

/// Index-consistency: createIndex, insert, update a field that has an index,
/// then find by both old and new values to prove __cidx_* was rewritten.
#[tokio::test]
async fn update_keeps_index_consistent() {
    let app = app().await;

    // Create an index on "status" before any data.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/workers/createIndex",
        Some(json!({ "keys": { "status": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert a doc.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/workers/insert",
        Some(json!({ "documents": [{ "name": "ada", "status": "active" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");
    let id = body["insertedIds"][0].as_str().unwrap().to_string();

    // Update status from "active" to "idle".
    let (s, body) = call(
        &app,
        "POST",
        "/collections/workers/update",
        Some(json!({
            "filter": { "_id": id },
            "update": { "$set": { "status": "idle" } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "update: {body}");
    assert_eq!(body["modifiedCount"].as_i64().unwrap(), 1);

    // find by new value "idle" — must return exactly 1 doc (proves __cidx_status updated).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/workers/find",
        Some(json!({ "filter": { "status": "idle" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find idle: {body}");
    let docs = body["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 1, "expected 1 idle doc; got: {body}");
    assert_eq!(docs[0]["name"].as_str().unwrap(), "ada");

    // find by old value "active" — must return 0 docs (stale index would return 1).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/workers/find",
        Some(json!({ "filter": { "status": "active" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find active: {body}");
    let docs = body["documents"].as_array().unwrap();
    assert_eq!(docs.len(), 0, "expected 0 active docs; stale index? got: {body}");
}

/// createIndex is idempotent — calling it twice on the same field succeeds and
/// the find still works.
#[tokio::test]
async fn create_index_idempotent_column_add() {
    let app = app().await;

    let (s, _) = call(
        &app,
        "POST",
        "/collections/items/insert",
        Some(json!({ "documents": [{ "color": "red" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // Create the same index twice.
    for _ in 0..2 {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/items/createIndex",
            Some(json!({ "keys": { "color": 1 } })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "createIndex attempt: {body}");
    }

    let (s, body) = call(
        &app,
        "POST",
        "/collections/items/find",
        Some(json!({ "filter": { "color": "red" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc: {body}");
}

// ---------------------------------------------------------------------------
// aggregate (MongoDB aggregation pipeline via DataFusion)
// ---------------------------------------------------------------------------

/// `$group` + `$sum` + `$sort`: insert three sales rows, seal so the Iceberg
/// mirror has the data (the aggregation reads via DataFusion), then group by
/// region summing `amt`, sorted by `_id`.
#[tokio::test]
async fn aggregate_group_sum_sorted_by_id() {
    let (app, state) = app_with_state().await;

    for d in [
        json!({"region": "EU", "amt": 10}),
        json!({"region": "EU", "amt": 5}),
        json!({"region": "US", "amt": 7}),
    ] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/sales/insert",
            Some(json!({ "documents": [d] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert: {body}");
    }

    // Seal so the DataFusion-over-Iceberg read sees the rows.
    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/sales/aggregate",
        Some(json!({
            "pipeline": [
                { "$group": { "_id": "$region", "total": { "$sum": "$amt" } } },
                { "$sort": { "_id": 1 } }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 2, "expected 2 groups (EU, US), got: {body}");

    assert_eq!(docs[0]["_id"].as_str().unwrap(), "EU");
    assert_eq!(docs[0]["total"].as_f64().unwrap(), 15.0, "EU total: {body}");
    assert_eq!(docs[1]["_id"].as_str().unwrap(), "US");
    assert_eq!(docs[1]["total"].as_f64().unwrap(), 7.0, "US total: {body}");
}

/// `$match` (string field) + `$sort` + `$limit`: filter to one region, sort by a
/// string field ascending, cap the count.
#[tokio::test]
async fn aggregate_match_sort_limit_over_string_field() {
    let (app, state) = app_with_state().await;

    for d in [
        json!({"region": "EU", "name": "delta"}),
        json!({"region": "EU", "name": "alpha"}),
        json!({"region": "EU", "name": "charlie"}),
        json!({"region": "US", "name": "zeta"}),
    ] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/teams/insert",
            Some(json!({ "documents": [d] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert: {body}");
    }

    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/teams/aggregate",
        Some(json!({
            "pipeline": [
                { "$match": { "region": "EU" } },
                { "$sort": { "name": 1 } },
                { "$limit": 2 }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 2, "expected 2 (limited from 3 EU), got: {body}");

    // Sorted ascending by name: alpha, charlie (delta is dropped by the limit).
    let n0 = json_get_field(&docs[0], "name");
    let n1 = json_get_field(&docs[1], "name");
    assert_eq!(n0, "alpha", "first by name asc: {body}");
    assert_eq!(n1, "charlie", "second by name asc: {body}");
}

/// `$count`: count the documents matching a `$match` over a string field.
#[tokio::test]
async fn aggregate_match_then_count() {
    let (app, state) = app_with_state().await;

    for d in [
        json!({"region": "EU"}),
        json!({"region": "EU"}),
        json!({"region": "US"}),
    ] {
        let (s, _) = call(
            &app,
            "POST",
            "/collections/regions/insert",
            Some(json!({ "documents": [d] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
    }
    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/regions/aggregate",
        Some(json!({
            "pipeline": [
                { "$match": { "region": "EU" } },
                { "$count": "n" }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "count yields one row: {body}");
    assert_eq!(docs[0]["n"].as_i64().unwrap(), 2, "EU count: {body}");
}

/// FIX 1: createIndex on a numeric field must not cause the indexed query to
/// silently return empty. The derived column is TEXT but the filter value is
/// numeric, so `to_sql_indexed` must fall back to the JSON accessor for numeric
/// comparisons. A string-indexed field must still work on the fast path.
#[tokio::test]
async fn indexed_numeric_field_find_returns_correct_results() {
    let (app, state) = app_with_state().await;

    // Create index on "age" (numeric) and "name" (string) before insert.
    for field in ["age", "name"] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/agetest/createIndex",
            Some(json!({ "keys": { field: 1 } })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "createIndex {field}: {body}");
    }

    // Insert a doc with age=36.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/agetest/insert",
        Some(json!({ "documents": [{ "name": "ada", "age": 36 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // Seal so the analytical engine can serve the query if it routes there.
    state.seal_now().await.expect("seal");

    // find by numeric age=36 — must return exactly 1 doc "ada" (not 0).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/agetest/find",
        Some(json!({ "filter": { "age": 36 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find age=36: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "indexed numeric field must return 1 doc for age=36, got: {body}");
    assert_eq!(
        docs[0]["name"].as_str().unwrap_or(""),
        "ada",
        "expected ada: {body}"
    );

    // find by string name="ada" (indexed string field) — must still work on the fast path.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/agetest/find",
        Some(json!({ "filter": { "name": "ada" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find name=ada: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "string-indexed field must return 1 doc, got: {body}");
}

/// An unknown aggregation stage is a 400 with the PARSE_ERROR code.
#[tokio::test]
async fn aggregate_unknown_stage_is_400() {
    let (app, _state) = app_with_state().await;

    // Create the collection so the table resolves (the stage check still fires).
    let (s, _) = call(
        &app,
        "POST",
        "/collections/widgets/insert",
        Some(json!({ "documents": [{ "x": 1 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    _state.seal_now().await.expect("seal");

    let (status, _body) = call(
        &app,
        "POST",
        "/collections/widgets/aggregate",
        Some(json!({ "pipeline": [ { "$bogus": {} } ] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "unknown stage should be 400");
}

// ---------------------------------------------------------------------------
// count tests
// ---------------------------------------------------------------------------

/// `POST /collections/{coll}/count` with a filter and with an empty filter.
/// The `v >= 20` filter on a non-indexed field routes through DataFusion, so
/// a seal is required after insert (same pattern as `find_by_id_and_by_field`).
#[tokio::test]
async fn count_with_filter() {
    let (app, state) = app_with_state().await;

    // Insert {v:10}, {v:20}, {v:30}.
    for v in [10i64, 20, 30] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/c/insert",
            Some(json!({ "documents": [{ "v": v }] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert v={v}: {body}");
    }

    // Seal so the DataFusion/Iceberg path sees the data.
    state.seal_now().await.expect("seal");

    // count with filter v >= 20  → 2 (v=20 and v=30).
    let (status, body) = call(
        &app,
        "POST",
        "/collections/c/count",
        Some(json!({ "filter": { "v": { "$gte": 20 } } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "count filtered: {body}");
    let n = body["count"].as_i64().expect("count field missing");
    assert_eq!(n, 2, "expected 2 docs with v>=20, got: {body}");

    // count with empty filter {} → 3 (all docs).
    let (status, body) = call(
        &app,
        "POST",
        "/collections/c/count",
        Some(json!({ "filter": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "count all: {body}");
    let n_all = body["count"].as_i64().expect("count field missing");
    assert_eq!(n_all, 3, "expected 3 total docs, got: {body}");
}

// ---------------------------------------------------------------------------
// MongoDB-shaped error tests
// ---------------------------------------------------------------------------

/// Inserting a document with a duplicate `_id` returns a MongoDB-shaped
/// `{ok:0, code:11000, codeName:"DuplicateKey"}` body, not the generic
/// bluedb `{error, code}` shape.
#[tokio::test]
async fn duplicate_id_returns_mongo_duplicate_key() {
    let app = app().await;
    let body = json!({ "documents": [{ "_id": "dup", "x": 1 }] });

    // First insert — must succeed.
    let (s1, b1) = call(&app, "POST", "/collections/dup_test/insert", Some(body.clone())).await;
    assert_eq!(s1, StatusCode::OK, "first insert failed: {b1}");

    // Second insert of the same _id — must error with DuplicateKey.
    let (s2, b2) = call(&app, "POST", "/collections/dup_test/insert", Some(body)).await;
    assert!(s2.is_client_error() || s2.is_server_error(), "expected error, got {s2}: {b2}");
    assert_eq!(b2["ok"].as_i64().unwrap_or(1), 0, "expected ok:0, got: {b2}");
    assert_eq!(b2["code"].as_i64().unwrap_or(0), 11000, "expected code 11000, got: {b2}");
    assert_eq!(b2["codeName"].as_str().unwrap_or(""), "DuplicateKey", "expected DuplicateKey, got: {b2}");
}

/// Sending an unsupported MQL operator in a filter returns a MongoDB-shaped
/// `{ok:0, code:2, codeName:"BadValue"}` body.
#[tokio::test]
async fn unsupported_operator_is_mongo_bad_value() {
    let app = app().await;
    // Insert one doc so the collection exists (filter parse fires before the scan).
    let (s, _) = call(
        &app,
        "POST",
        "/collections/bad_op/insert",
        Some(json!({ "documents": [{ "x": 1 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (status, body) = call(
        &app,
        "POST",
        "/collections/bad_op/find",
        Some(json!({ "filter": { "x": { "$where": "1" } } })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400, got {status}: {body}");
    assert_eq!(body["ok"].as_i64().unwrap_or(1), 0, "expected ok:0, got: {body}");
    assert_eq!(body["codeName"].as_str().unwrap_or(""), "BadValue", "expected BadValue, got: {body}");
}

// ---------------------------------------------------------------------------
// Typed index tests (numeric / bool fast path, no seal needed)
// ---------------------------------------------------------------------------

/// createIndex on a numeric field (inferred type), insert a doc, find by
/// numeric equality **without sealing** — proves the FLOAT derived column is
/// used on the GlueSQL fast path (read-your-writes).
/// Also tests `{age: {"$gte": 30}}` to exercise the range fast path.
#[tokio::test]
async fn numeric_indexed_field_is_fast_and_fresh() {
    let app = app().await;

    // Create a numeric index on "age" (type inferred from empty collection → Text,
    // but we pass an explicit hint so it is always FLOAT regardless of order).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/numidx/createIndex",
        Some(json!({ "keys": { "age": 1 }, "options": { "type": "number" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert without sealing.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/numidx/insert",
        Some(json!({ "documents": [{ "name": "ada", "age": 36 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // find {age: 36} — no seal needed (fast path via FLOAT column).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/numidx/find",
        Some(json!({ "filter": { "age": 36 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find age=36: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "numeric fast path: expected 1 doc for age=36 (no seal), got: {body}"
    );
    assert_eq!(docs[0]["name"].as_str().unwrap(), "ada");

    // find {age: {$gte: 30}} — range on FLOAT column, still fast path.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/numidx/find",
        Some(json!({ "filter": { "age": { "$gte": 30 } } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find age>=30: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "numeric range fast path: expected 1 doc for age>=30, got: {body}"
    );
}

/// createIndex on a boolean field (`{type:"bool"}`), insert, find without sealing.
#[tokio::test]
async fn bool_indexed_field_is_fast() {
    let app = app().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/boolidx/createIndex",
        Some(json!({ "keys": { "active": 1 }, "options": { "type": "bool" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/collections/boolidx/insert",
        Some(json!({ "documents": [{ "active": true, "name": "ada" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // find {active: true} — no seal (fast path via BOOLEAN column).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/boolidx/find",
        Some(json!({ "filter": { "active": true } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find active=true: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "bool fast path: expected 1 doc for active=true (no seal), got: {body}"
    );
    assert_eq!(docs[0]["name"].as_str().unwrap(), "ada");
}

/// A numeric-indexed field queried with a **string** value must not error —
/// it falls back to the JSON accessor path and returns a sensible result
/// (may be empty or analytical; the point is no crash and string index unaffected).
#[tokio::test]
async fn numeric_index_queried_with_string_does_not_error() {
    let (app, state) = app_with_state().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/mixidx/createIndex",
        Some(json!({ "keys": { "score": 1 }, "options": { "type": "number" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/collections/mixidx/insert",
        Some(json!({ "documents": [{ "score": 99 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // Seal so the analytical fallback can also see the row.
    state.seal_now().await.expect("seal");

    // Query with a string value on a numeric-indexed field — must not panic or 500.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/mixidx/find",
        Some(json!({ "filter": { "score": "99" } })),
    )
    .await;
    assert!(
        !s.is_server_error(),
        "string query on numeric index must not 500, got {s}: {body}"
    );
    // Result may be 0 or 1 depending on routing; we only require no crash.
    assert!(body["documents"].is_array(), "response must have documents array");
}

/// Helper: read a string field that may have arrived as a JSON object's member
/// or directly. The aggregate result projects `_id` plus extracted columns; a
/// `$match`+`$sort` pipeline with no `$project` returns the base table columns
/// (`_id`, `doc`), so `name` lives inside the re-inflated `doc` text or, when the
/// row carries a top-level `name` column, directly. Handle both.
fn json_get_field(doc: &Value, field: &str) -> String {
    if let Some(s) = doc.get(field).and_then(Value::as_str) {
        return s.to_string();
    }
    // Fall back to the JSON `doc` column (string or object).
    match doc.get("doc") {
        Some(Value::String(s)) => serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v.get(field).and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default(),
        Some(Value::Object(_)) => doc["doc"]
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default(),
        _ => String::new(),
    }
}
