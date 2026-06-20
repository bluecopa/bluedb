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

/// Regression for UAT-COLL-007: a `$group` output referenced by a later
/// `$sort` (and a `$project` output by a later `$addFields`) must read the
/// computed column, not the now-absent `doc` JSON accessor. Before the fix,
/// `$sort {total: -1}` after `$group` errored "No field named <coll>.doc".
#[tokio::test]
async fn aggregate_sort_on_group_output_and_chained_project() {
    let (app, state) = app_with_state().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/agg7/insert",
        Some(json!({ "documents": [
            { "_id": "a1", "region": "EU", "amount": 10, "kind": "retail" },
            { "_id": "a2", "region": "EU", "amount": 30, "kind": "retail" },
            { "_id": "a3", "region": "US", "amount": 20, "kind": "enterprise" },
            { "_id": "a4", "region": "US", "amount": 40, "kind": "retail" }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");
    state.seal_now().await.expect("seal");

    // $group then $sort on the `total` accumulator output (desc), $limit 2.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/agg7/aggregate",
        Some(json!({ "pipeline": [
            { "$match": { "kind": "retail" } },
            { "$group": { "_id": "$region", "total": { "$sum": "$amount" }, "count": { "$count": {} } } },
            { "$sort": { "total": -1 } },
            { "$limit": 2 }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "group+sort+limit: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 2, "expected 2 groups, got: {body}");
    // Both groups sum to 40; sort is stable enough that EU (first inserted) leads.
    assert_eq!(docs[0]["total"].as_f64().unwrap(), 40.0, "first total: {body}");

    // $project then $addFields referencing the projected field `who`.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/agg7/aggregate",
        Some(json!({ "pipeline": [
            { "$match": { "region": "EU" } },
            { "$project": { "who": "$region", "amount": 1 } },
            { "$addFields": { "tag": "$who" } }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "project+addFields: {body}");
    for doc in body["documents"].as_array().expect("documents array") {
        assert_eq!(doc["who"].as_str().unwrap(), "EU", "who: {body}");
        assert_eq!(doc["tag"].as_str().unwrap(), "EU", "tag mirrors who: {body}");
    }
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

/// `$unwind` of a JSON array field: the array `tags` is materialized into a real
/// Arrow list column and unnested to one row per element, then grouped+counted.
/// Two docs `{tags:[x,y]}` and `{tags:[y,z]}` yield element counts x:1, y:2, z:1.
/// This proves (a) `$unwind` works over a JSON document field (not a real column),
/// and (b) the downstream `$group {_id:"$tags"}` reads the materialized `tags`
/// column rather than re-extracting the original array from `doc`.
#[tokio::test]
async fn aggregate_unwind_array_field_then_group() {
    let (app, state) = app_with_state().await;

    for d in [
        json!({"name": "a", "tags": ["x", "y"]}),
        json!({"name": "b", "tags": ["y", "z"]}),
    ] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/tags_coll/insert",
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
        "/collections/tags_coll/aggregate",
        Some(json!({
            "pipeline": [
                { "$unwind": "$tags" },
                { "$group": { "_id": "$tags", "n": { "$sum": 1 } } },
                { "$sort": { "_id": 1 } }
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 3, "expected 3 distinct tags (x,y,z), got: {body}");

    // Sorted ascending by _id: x:1, y:2, z:1.
    assert_eq!(docs[0]["_id"].as_str().unwrap(), "x");
    assert_eq!(docs[0]["n"].as_i64().unwrap(), 1, "x count: {body}");
    assert_eq!(docs[1]["_id"].as_str().unwrap(), "y");
    assert_eq!(docs[1]["n"].as_i64().unwrap(), 2, "y appears in both docs: {body}");
    assert_eq!(docs[2]["_id"].as_str().unwrap(), "z");
    assert_eq!(docs[2]["n"].as_i64().unwrap(), 1, "z count: {body}");
}

/// `$lookup` joining `orders` to `customers` on `cust == _id`. The matched foreign
/// document is nested into the `as` array (`customer`), matching MongoDB: exactly
/// one output row per left document, and `customer` is a JSON **array** of the
/// matched foreign documents as objects (here a single-element array `[{…}]`).
#[tokio::test]
async fn lookup_nests_matched_docs_as_array() {
    let (app, state) = app_with_state().await;

    // orders: one order referencing customer c1.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/orders/insert",
        Some(json!({ "documents": [{ "_id": "o1", "cust": "c1" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert order: {body}");

    // customers: c1 = Ada.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/customers/insert",
        Some(json!({ "documents": [{ "_id": "c1", "name": "Ada" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert customer: {body}");

    // Seal both collections so the DataFusion join sees both tables.
    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/orders/aggregate",
        Some(json!({
            "pipeline": [
                { "$lookup": {
                    "from": "customers",
                    "localField": "cust",
                    "foreignField": "_id",
                    "as": "customer"
                }}
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected exactly the one o1 row, got: {body}");

    // The o1 row carries its own _id plus the matched customer under `customer`.
    assert_eq!(docs[0]["_id"].as_str().unwrap(), "o1", "left _id: {body}");

    // `customer` is a JSON ARRAY of matched foreign documents (objects).
    let customer = docs[0]["customer"]
        .as_array()
        .unwrap_or_else(|| panic!("`customer` must be a JSON array, got: {body}"));
    assert_eq!(customer.len(), 1, "one matched customer: {body}");
    assert!(customer[0].is_object(), "array element must be an object: {body}");
    assert_eq!(
        customer[0]["name"].as_str(),
        Some("Ada"),
        "matched customer name: {body}"
    );
}

/// `$lookup` where the left document references a non-existent foreign key: the
/// `as` array must be an **empty array** `[]`, not `[null]` and not a NULL/string.
#[tokio::test]
async fn lookup_no_match_is_empty_array() {
    let (app, state) = app_with_state().await;

    // orders: one order referencing a customer that does not exist.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/orders/insert",
        Some(json!({ "documents": [{ "_id": "o1", "cust": "missing" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert order: {body}");

    // customers: a single unrelated customer (so the table exists and seals).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/customers/insert",
        Some(json!({ "documents": [{ "_id": "c1", "name": "Ada" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert customer: {body}");

    // Seal both collections so the DataFusion join sees both tables.
    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/orders/aggregate",
        Some(json!({
            "pipeline": [
                { "$lookup": {
                    "from": "customers",
                    "localField": "cust",
                    "foreignField": "_id",
                    "as": "customer"
                }}
            ]
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "aggregate failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected the one o1 row, got: {body}");
    assert_eq!(docs[0]["_id"].as_str().unwrap(), "o1", "left _id: {body}");

    // No-match → empty array `[]`.
    let customer = docs[0]["customer"]
        .as_array()
        .unwrap_or_else(|| panic!("`customer` must be a JSON array, got: {body}"));
    assert!(
        customer.is_empty(),
        "no-match `customer` must be an empty array, got: {body}"
    );
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

/// Numeric index unifies integer and whole-float encoding.
///
/// A doc stored with `{"qty": 5}` (integer) and one with `{"qty": 6.0}` (whole
/// float) must both be findable by `{qty:5}` and `{qty:6}` respectively on the
/// GlueSQL fast path — no seal required, because the index column is used.
#[tokio::test]
async fn numeric_index_unifies_int_and_float_encoding() {
    let app = app().await;

    // Create an explicit number index on "qty".
    let (s, body) = call(
        &app,
        "POST",
        "/collections/qtyidx/createIndex",
        Some(json!({ "keys": { "qty": 1 }, "options": { "type": "number" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert one doc with integer qty=5 and another with float qty=6.0.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/qtyidx/insert",
        Some(json!({ "documents": [
            { "label": "int-five",   "qty": 5   },
            { "label": "float-six",  "qty": 6.0 }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // find {qty:5} — must find the integer-5 doc on the fast path (no seal).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/qtyidx/find",
        Some(json!({ "filter": { "qty": 5 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find qty=5: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc for qty=5, got: {body}");
    assert_eq!(docs[0]["label"].as_str().unwrap(), "int-five");

    // find {qty:6} — must find the float-6.0 doc on the fast path.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/qtyidx/find",
        Some(json!({ "filter": { "qty": 6 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find qty=6: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "float-6.0 doc must be found by integer query {{qty:6}}, got: {body}"
    );
    assert_eq!(docs[0]["label"].as_str().unwrap(), "float-six");
}

/// Fractional field find is correct — routes to analytical path, not silently empty.
///
/// A doc stored with `{"price": 3.14}` must be findable by `{price: 3.14}`.
/// Because 3.14 is fractional the index column stores NULL for it; the query
/// must route to the JSON accessor (analytical path after seal) and still
/// return the doc.  `{price: 2}` must return 0 docs.
#[tokio::test]
async fn fractional_field_find_is_correct() {
    let (app, state) = app_with_state().await;

    // Create an explicit number index on "price" (will store NULL for 3.14).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/priceidx/createIndex",
        Some(json!({ "keys": { "price": 1 }, "options": { "type": "number" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert a doc with a fractional price.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/priceidx/insert",
        Some(json!({ "documents": [{ "label": "pi-price", "price": 3.14 }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // Seal so the analytical path (DataFusion/Iceberg) can serve the query.
    state.seal_now().await.expect("seal");

    // find {price: 3.14} — must return 1 doc via the analytical path (not 0).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/priceidx/find",
        Some(json!({ "filter": { "price": 3.14 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find price=3.14: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "fractional price query must return 1 doc (not silently empty), got: {body}"
    );
    assert_eq!(docs[0]["label"].as_str().unwrap(), "pi-price");

    // find {price: 2} — must return 0 docs.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/priceidx/find",
        Some(json!({ "filter": { "price": 2 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find price=2: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 0, "price=2 must return 0 docs, got: {body}");
}

// ---------------------------------------------------------------------------
// Compound index tests
// ---------------------------------------------------------------------------

/// `createIndex({region:1, status:1})` — compound equality fast path.
///
/// Insert three docs without sealing.  A `find {region:"EU", status:"active"}`
/// must return exactly the one matching doc (n==1), proving:
/// - the compound `__cidxm_region__status` column is populated on insert,
/// - the find routes through the compound index (GlueSQL fast path, no DataFusion
///   round-trip → read-your-writes without a seal).
#[tokio::test]
async fn compound_index_equality_is_fast_and_fresh() {
    let app = app().await;

    // Create compound index before any data.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/cidx_eq/createIndex",
        Some(json!({ "keys": { "region": 1, "status": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex compound: {body}");
    let name = body["name"].as_str().expect("name in response");
    assert!(
        name.contains("cidxm") && name.contains("region") && name.contains("status"),
        "expected compound index name, got: {name}"
    );

    // Insert three docs — no seal.
    for (region, status, n) in [("EU", "active", 1), ("EU", "idle", 2), ("US", "active", 3)] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/cidx_eq/insert",
            Some(json!({ "documents": [{ "region": region, "status": status, "n": n }] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert: {body}");
    }

    // find {region:"EU", status:"active"} — must return exactly 1 doc (n==1).
    // No seal_now — compound fast path (GlueSQL, read-your-writes).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/cidx_eq/find",
        Some(json!({ "filter": { "region": "EU", "status": "active" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find compound eq: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected exactly 1 doc (EU/active), got: {body}");
    assert_eq!(
        docs[0]["n"].as_i64().unwrap_or(-1),
        1,
        "expected n==1 (EU/active), got: {body}"
    );
}

/// `find {region:"EU"}` (prefix-only) on a compound index must still return
/// the correct 2 docs.  This does NOT use the compound key (single-field
/// routing or DataFusion fallback); we seal to make the analytical path work.
#[tokio::test]
async fn compound_index_partial_filter_still_correct() {
    let (app, state) = app_with_state().await;

    // Create compound index.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/cidx_partial/createIndex",
        Some(json!({ "keys": { "region": 1, "status": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert three docs.
    for (region, status, n) in [("EU", "active", 1), ("EU", "idle", 2), ("US", "active", 3)] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/cidx_partial/insert",
            Some(json!({ "documents": [{ "region": region, "status": status, "n": n }] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert: {body}");
    }

    // Seal so the DataFusion/Iceberg path can serve the non-compound query.
    state.seal_now().await.expect("seal");

    // find {region:"EU"} — only the prefix, no compound match.
    // Must return exactly 2 docs (EU/active + EU/idle), correctness not compromised.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/cidx_partial/find",
        Some(json!({ "filter": { "region": "EU" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find region-only: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 2, "expected 2 docs for region=EU only, got: {body}");
}

/// Sorting on a nested document path (e.g. `{"addr.city": 1}`) must resolve the
/// chained accessor `(doc->'addr'->>'city')`, not the broken single-level
/// `(doc->>'addr.city')`.  A sort on a non-indexed nested path routes to the
/// analytical engine, so a seal is required after insert.
#[tokio::test]
async fn find_sorts_by_nested_path() {
    let (app, state) = app_with_state().await;

    for d in [
        serde_json::json!({"name": "a", "addr": {"city": "Zurich"}}),
        serde_json::json!({"name": "b", "addr": {"city": "Austin"}}),
        serde_json::json!({"name": "c", "addr": {"city": "Madrid"}}),
    ] {
        let (s, body) = call(
            &app,
            "POST",
            "/collections/nested_sort/insert",
            Some(serde_json::json!({ "documents": [d] })),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "insert failed: {body}");
    }

    // Seal so the Iceberg mirror has the data (analytical path required for
    // a non-indexed nested sort).
    state.seal_now().await.expect("seal");

    let (status, body) = call(
        &app,
        "POST",
        "/collections/nested_sort/find",
        Some(serde_json::json!({
            "filter": {},
            "sort": { "addr.city": 1 }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "find failed: {body}");

    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 3, "expected 3 docs, got: {body}");

    // Ascending by city: Austin(b) < Madrid(c) < Zurich(a).
    let names: Vec<&str> = docs
        .iter()
        .map(|d| d["name"].as_str().expect("name"))
        .collect();
    assert_eq!(
        names,
        vec!["b", "c", "a"],
        "expected sort order Austin→Madrid→Zurich (b,c,a), got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// TTL index tests
// ---------------------------------------------------------------------------

/// Verify that `sweep_ttl` deletes expired documents and leaves live ones.
///
/// Setup: collection "sessions" with two docs —
///   {_id:"old", created: 1000}  — expires at epoch 1100 (1000 + 100)
///   {_id:"new", created: 1_000_000}  — expires at epoch 1_000_100
///
/// Sweep with now_epoch = 2000, expireAfterSeconds = 100.
///   "old": 1000 + 100 = 1100 ≤ 2000 → deleted
///   "new": 1_000_000 + 100 = 1_000_100 > 2000 → survives
///
/// After sweep, find {} must return exactly one document: "new".
#[tokio::test]
async fn ttl_sweep_deletes_expired_docs() {
    let (app, state) = app_with_state().await;

    // Insert the two documents.
    let (status, body) = call(
        &app,
        "POST",
        "/collections/sessions/insert",
        Some(json!({
            "documents": [
                { "_id": "old", "created": 1000 },
                { "_id": "new", "created": 1_000_000_i64 }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "insert failed: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 2);

    // Call createIndex with expireAfterSeconds so the TTL registry row is written.
    let (status, body) = call(
        &app,
        "POST",
        "/collections/sessions/createIndex",
        Some(json!({
            "keys": { "created": 1 },
            "options": { "expireAfterSeconds": 100, "type": "number" }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "createIndex failed: {body}");

    // Sweep with deterministic now_epoch = 2000 (100 s past "old"'s expiry).
    let deleted = state
        .sweep_ttl_for_test("_", "sessions", "created", 100, 2000)
        .await
        .expect("sweep_ttl failed");
    assert_eq!(deleted, 1, "expected 1 doc deleted, got {deleted}");

    // find {} — only "new" must remain.
    let (status, body) = call(
        &app,
        "POST",
        "/collections/sessions/find",
        Some(json!({ "filter": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "find after sweep failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc remaining after sweep, got: {body}");
    let remaining_id = docs[0].get("_id").and_then(Value::as_str).unwrap_or("");
    assert_eq!(remaining_id, "new", "expected 'new' to survive, got: {docs:?}");
}

/// Verify that a document with an ISO-8601 string timestamp is also swept.
#[tokio::test]
async fn ttl_sweep_handles_iso8601_string_field() {
    let (app, state) = app_with_state().await;

    // 2000-01-01T00:00:00Z = epoch 946684800
    let (status, body) = call(
        &app,
        "POST",
        "/collections/iso_sessions/insert",
        Some(json!({
            "documents": [
                { "_id": "expired_iso", "ts": "2000-01-01T00:00:00Z" },
                { "_id": "future_iso",  "ts": "2099-01-01T00:00:00Z" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "insert failed: {body}");

    // now_epoch = 946684900 (100 s after the expired doc, well before the future one).
    // expireAfterSeconds = 0 → the doc is expired iff its ts ≤ now.
    let deleted = state
        .sweep_ttl_for_test("_", "iso_sessions", "ts", 0, 946_684_900)
        .await
        .expect("sweep_ttl (iso8601) failed");
    assert_eq!(deleted, 1, "expected 1 iso8601 doc deleted, got {deleted}");
}

/// Verify that `createIndex` with a negative expireAfterSeconds is rejected.
#[tokio::test]
async fn ttl_create_index_rejects_negative_seconds() {
    let app = app().await;

    let (status, body) = call(
        &app,
        "POST",
        "/collections/neg_ttl/createIndex",
        Some(json!({
            "keys": { "ts": 1 },
            "options": { "expireAfterSeconds": -1 }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400 for negative expireAfterSeconds, got: {body}");
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

// ---------------------------------------------------------------------------
// Multikey (array-field) index tests
// ---------------------------------------------------------------------------

/// Basic multikey index: createIndex auto-detects array field, find by element
/// membership returns the correct subset on FRESH, unsealed data.
///
/// The element-membership rewrite is `_id IN (SELECT _id FROM {side} WHERE
/// val = $1)`. The side table carries `INDEX(val)`, and the guardrail accepts
/// this PK-IN-(indexed subquery) shape as index-served, so the read stays on the
/// GlueSQL transactional fast path — it never routes to DataFusion (which would
/// require a prior seal). The correctness assertions here run on data that was
/// just inserted and never sealed, so a pass means the fast path served them.
/// (The formal "accepted, not rejected" proof is the `bluedb_sql::guardrail`
/// unit test `pk_in_indexed_subquery_is_accepted`.)
#[tokio::test]
async fn multikey_find_matches_array_element() {
    let app = app().await;

    // Insert two docs with an array field `tags`.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/articles/insert",
        Some(json!({ "documents": [
            { "name": "a", "tags": ["x", "y"] },
            { "name": "b", "tags": ["y", "z"] }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // Create index on `tags` — auto-detected as multikey because existing values are arrays.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/articles/createIndex",
        Some(json!({ "keys": { "tags": 1 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");
    let index_name = body["name"].as_str().expect("name in response");
    assert!(
        index_name.contains("mk_") || index_name.contains("tags"),
        "expected multikey index name, got {index_name}"
    );

    // find {tags:"y"} → 2 docs (both a and b have "y").
    let (s, body) = call(
        &app,
        "POST",
        "/collections/articles/find",
        Some(json!({ "filter": { "tags": "y" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find y: {body}");
    let docs = body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "expected 2 docs for tags:y, got: {body}");

    // find {tags:"x"} → 1 doc (only a has "x").
    let (s, body) = call(
        &app,
        "POST",
        "/collections/articles/find",
        Some(json!({ "filter": { "tags": "x" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find x: {body}");
    let docs = body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 1, "expected 1 doc for tags:x, got: {body}");
    assert_eq!(
        docs[0]["name"].as_str().unwrap_or(""),
        "a",
        "expected doc a for tags:x"
    );

    // find {tags:"q"} → 0 docs.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/articles/find",
        Some(json!({ "filter": { "tags": "q" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find q: {body}");
    let docs = body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 0, "expected 0 docs for tags:q, got: {body}");
}

/// Multikey index stays consistent after update and delete:
/// - update doc a's tags → old element no longer matches, new element does.
/// - delete doc b → old element in doc b no longer matches.
#[tokio::test]
async fn multikey_stays_consistent_on_update_and_delete() {
    let app = app().await;

    // Insert two docs.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/insert",
        Some(json!({ "documents": [
            { "_id": "doc_a", "tags": ["x", "y"] },
            { "_id": "doc_b", "tags": ["y", "z"] }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // Create multikey index (explicit opt-in to avoid auto-detect ambiguity).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/createIndex",
        Some(json!({ "keys": { "tags": 1 }, "options": { "multikey": true } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Sanity: find {tags:"x"} → 1 (doc_a).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/find",
        Some(json!({ "filter": { "tags": "x" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find x before update: {body}");
    assert_eq!(
        body["documents"].as_array().unwrap().len(),
        1,
        "expected 1 doc for x before update, got: {body}"
    );

    // Update doc_a: replace tags with ["z"] (removes x and y, adds z).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/update",
        Some(json!({
            "filter": { "_id": "doc_a" },
            "update": { "$set": { "tags": ["z"] } }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "update: {body}");
    assert_eq!(body["modifiedCount"].as_i64().unwrap(), 1, "modifiedCount: {body}");

    // After update: find {tags:"x"} → 0 (doc_a no longer has x).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/find",
        Some(json!({ "filter": { "tags": "x" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find x after update: {body}");
    assert_eq!(
        body["documents"].as_array().unwrap().len(),
        0,
        "expected 0 docs for x after doc_a update, got: {body}"
    );

    // After update: find {tags:"z"} → 2 (doc_a now has z, doc_b still has z).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/find",
        Some(json!({ "filter": { "tags": "z" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find z after update: {body}");
    assert_eq!(
        body["documents"].as_array().unwrap().len(),
        2,
        "expected 2 docs for z after update, got: {body}"
    );

    // Delete doc_b.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/delete",
        Some(json!({ "filter": { "_id": "doc_b" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "delete: {body}");
    assert_eq!(body["deletedCount"].as_i64().unwrap(), 1, "deletedCount: {body}");

    // After delete: find {tags:"y"} → 0 (doc_a no longer has y, doc_b deleted).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/find",
        Some(json!({ "filter": { "tags": "y" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find y after delete: {body}");
    assert_eq!(
        body["documents"].as_array().unwrap().len(),
        0,
        "expected 0 docs for y after update+delete, got: {body}"
    );

    // find {tags:"z"} → 1 (only doc_a remains with z).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/posts/find",
        Some(json!({ "filter": { "tags": "z" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find z after delete: {body}");
    assert_eq!(
        body["documents"].as_array().unwrap().len(),
        1,
        "expected 1 doc for z after doc_b delete, got: {body}"
    );
    assert_eq!(
        body["documents"][0]["_id"].as_str().unwrap_or(""),
        "doc_a",
        "expected doc_a for tags:z"
    );
}

// ---------------------------------------------------------------------------
// FIX C1: whole-float query literal on a numeric index (find {qty:6.0})
// ---------------------------------------------------------------------------

/// FIX C1: querying a numeric-indexed field with a whole-valued float literal
/// (`6.0`) must not error with a 400; it must find the document stored with that
/// value. Previously the float was passed raw to the INT index encoder, which
/// rejected it with "FLOAT data type cannot be converted to Big-Endian bytes".
#[tokio::test]
async fn numeric_index_whole_float_query_does_not_error() {
    let app = app().await;

    // Create an explicit number index on "qty".
    let (s, body) = call(
        &app,
        "POST",
        "/collections/floatq/createIndex",
        Some(json!({ "keys": { "qty": 1 }, "options": { "type": "number" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert one doc with integer qty=5 and another stored with whole-float qty=6.0.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/floatq/insert",
        Some(json!({ "documents": [
            { "label": "int-five",   "qty": 5   },
            { "label": "float-six",  "qty": 6.0 }
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert: {body}");

    // find {qty:6.0} (float literal) — must return the float-six doc (not 400).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/floatq/find",
        Some(json!({ "filter": { "qty": 6.0 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find qty=6.0 must not error: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc for qty=6.0, got: {body}");
    assert_eq!(docs[0]["label"].as_str().unwrap(), "float-six");

    // find {qty:5} — must return the int-five doc.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/floatq/find",
        Some(json!({ "filter": { "qty": 5 } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find qty=5: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc for qty=5, got: {body}");
    assert_eq!(docs[0]["label"].as_str().unwrap(), "int-five");

    // find {qty:{$gte:6.0}} (float range) — must return at least the float-six doc.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/floatq/find",
        Some(json!({ "filter": { "qty": { "$gte": 6.0 } } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find qty>=6.0 must not error: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert!(
        docs.len() >= 1,
        "find qty>=6.0 must return at least the float-six doc, got: {body}"
    );
    let labels: Vec<&str> = docs.iter().filter_map(|d| d["label"].as_str()).collect();
    assert!(
        labels.contains(&"float-six"),
        "float-six must be in qty>=6.0 results, got: {labels:?}"
    );
}

// ---------------------------------------------------------------------------
// Multi-tenant TTL sweep
// ---------------------------------------------------------------------------

/// TTL sweep works on a non-default tenant.
///
/// Setup: collection "acme_sessions" in tenant "acme" with two docs —
///   {_id:"old", created: 1000}  — expires at epoch 1100 (1000 + 100)
///   {_id:"new", created: 1_000_000}  — expires at epoch 1_000_100
///
/// Sweep with now_epoch = 2000, expireAfterSeconds = 100.
///   "old": 1000 + 100 = 1100 ≤ 2000 → deleted
///   "new": 1_000_000 + 100 = 1_000_100 > 2000 → survives
#[tokio::test]
async fn ttl_sweep_works_on_non_default_tenant() {
    let (app, state) = app_with_state().await;

    // Insert the two documents under tenant "acme".
    let request = Request::builder()
        .method("POST")
        .uri("/collections/acme_sessions/insert")
        .header("content-type", "application/json")
        .header("X-Bluedb-Tenant", "acme")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "documents": [
                    { "_id": "old", "created": 1000 },
                    { "_id": "new", "created": 1_000_000_i64 }
                ]
            }))
            .unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "insert failed");

    // createIndex with expireAfterSeconds on non-default tenant "acme" — must succeed.
    let request = Request::builder()
        .method("POST")
        .uri("/collections/acme_sessions/createIndex")
        .header("content-type", "application/json")
        .header("X-Bluedb-Tenant", "acme")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "keys": { "created": 1 },
                "options": { "expireAfterSeconds": 100, "type": "number" }
            }))
            .unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    assert_eq!(status, StatusCode::OK, "createIndex on non-default tenant failed: {body}");

    // Sweep using sweep_ttl_for_test on the "acme" tenant with now_epoch = 2000.
    let deleted = state
        .sweep_ttl_for_test("acme", "acme_sessions", "created", 100, 2000)
        .await
        .expect("sweep_ttl failed");
    assert_eq!(deleted, 1, "expected 1 doc deleted, got {deleted}");

    // find {} on "acme" — only "new" must remain.
    let request = Request::builder()
        .method("POST")
        .uri("/collections/acme_sessions/find")
        .header("content-type", "application/json")
        .header("X-Bluedb-Tenant", "acme")
        .body(Body::from(
            serde_json::to_vec(&json!({ "filter": {} })).unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    assert_eq!(status, StatusCode::OK, "find after sweep failed: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 1, "expected 1 doc remaining after sweep, got: {body}");
    let remaining_id = docs[0].get("_id").and_then(Value::as_str).unwrap_or("");
    assert_eq!(remaining_id, "new", "expected 'new' to survive, got: {docs:?}");
}

/// TTL createIndex on a non-default tenant is now accepted and registers the
/// tenant in the global TTL registry.
#[tokio::test]
async fn ttl_create_index_accepted_for_non_default_tenant() {
    let app = app().await;

    let request = Request::builder()
        .method("POST")
        .uri("/collections/reg_coll/createIndex")
        .header("content-type", "application/json")
        .header("X-Bluedb-Tenant", "reg_tenant")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "keys": { "created": 1 },
                "options": { "expireAfterSeconds": 60, "type": "number" }
            }))
            .unwrap(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);

    assert_eq!(
        status,
        StatusCode::OK,
        "TTL createIndex on non-default tenant must now succeed, got: {body}"
    );
}

// ---------------------------------------------------------------------------
// FIX I4: compound/multikey indexes reject dotted (nested) paths
// ---------------------------------------------------------------------------

/// FIX I4: createIndex on a compound key with a dotted component path must
/// return 4xx — the column-name round-trip conflates "a.b" with "a_b".
#[tokio::test]
async fn compound_index_rejects_dotted_path() {
    let app = app().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/ci_dotted/createIndex",
        Some(json!({ "keys": { "a.b": 1, "c": 1 } })),
    )
    .await;
    assert!(
        s.is_client_error(),
        "compound index with dotted path must be rejected (4xx), got: {s} body: {body}"
    );
    let errmsg = body["errmsg"].as_str().unwrap_or("");
    assert!(
        errmsg.contains("dotted") || errmsg.contains("nested") || errmsg.contains("not supported"),
        "error must mention dotted/nested paths, got: {errmsg}"
    );
}

/// FIX I4: createIndex (multikey) on a dotted path must return 4xx.
#[tokio::test]
async fn multikey_index_rejects_dotted_path() {
    let app = app().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/mk_dotted/createIndex",
        Some(json!({ "keys": { "a.b": 1 }, "options": { "multikey": true } })),
    )
    .await;
    assert!(
        s.is_client_error(),
        "multikey index with dotted path must be rejected (4xx), got: {s} body: {body}"
    );
    let errmsg = body["errmsg"].as_str().unwrap_or("");
    assert!(
        errmsg.contains("dotted") || errmsg.contains("nested") || errmsg.contains("not supported"),
        "error must mention dotted/nested paths, got: {errmsg}"
    );
}

// ---------------------------------------------------------------------------
// FIX M5: scalar value in a multikey field is indexed
// ---------------------------------------------------------------------------

/// FIX M5: when a multikey-indexed field holds a scalar (not an array), it
/// must still be indexed as a single entry so `find {tags:"x"}` returns the doc.
#[tokio::test]
async fn multikey_scalar_field_is_indexed() {
    let app = app().await;

    // Create multikey index on `tags` (explicit opt-in — there are no existing
    // docs to auto-detect from, and we want to force multikey even for a scalar).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/scalartags/createIndex",
        Some(json!({ "keys": { "tags": 1 }, "options": { "multikey": true } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    // Insert a doc with a scalar (non-array) tags field.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/scalartags/insert",
        Some(json!({ "documents": [{ "_id": "doc1", "tags": "x" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "insert scalar tags: {body}");

    // find {tags:"x"} — must return the scalar-tags doc (not 0).
    let (s, body) = call(
        &app,
        "POST",
        "/collections/scalartags/find",
        Some(json!({ "filter": { "tags": "x" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find tags=x: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(
        docs.len(), 1,
        "scalar 'tags:x' must be found by find {{tags:'x'}}, got: {body}"
    );

    // find {tags:"y"} — must return 0 (scalar "x" does not match "y").
    let (s, body) = call(
        &app,
        "POST",
        "/collections/scalartags/find",
        Some(json!({ "filter": { "tags": "y" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "find tags=y: {body}");
    let docs = body["documents"].as_array().expect("documents array");
    assert_eq!(docs.len(), 0, "tags:y must match 0 docs for scalar x, got: {body}");
}

/// Regression for UAT-COLL-003: a unique single-field collection index must
/// reject a duplicate field value. GlueSQL silently drops the UNIQUE keyword
/// from `CREATE UNIQUE INDEX`, so uniqueness is enforced as a column-level
/// constraint on the derived `__cidx_*` column. The duplicate surfaces as the
/// Mongo-shaped `DuplicateKey` (code 11000) error over HTTP 409.
#[tokio::test]
async fn unique_single_field_index_rejects_duplicate() {
    let app = app().await;

    let (s, body) = call(
        &app,
        "POST",
        "/collections/prod/createIndex",
        Some(json!({ "keys": { "sku": 1 }, "options": { "unique": true, "type": "string" } })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "createIndex: {body}");

    let (s, body) = call(
        &app,
        "POST",
        "/collections/prod/insert",
        Some(json!({ "documents": [{ "_id": "p1", "sku": "A-1", "tags": ["blue", "sale"] }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "first insert: {body}");

    // Same sku, different _id → must be rejected.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/prod/insert",
        Some(json!({ "documents": [{ "_id": "p2", "sku": "A-1", "tags": ["red"] }] })),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "duplicate should be 409, got {s}: {body}");
    assert_eq!(body["code"].as_i64().unwrap(), 11000, "DuplicateKey code: {body}");
    assert_eq!(body["codeName"].as_str().unwrap(), "DuplicateKey", "codeName: {body}");

    // A distinct sku still inserts cleanly — uniqueness, not a blanket block.
    let (s, body) = call(
        &app,
        "POST",
        "/collections/prod/insert",
        Some(json!({ "documents": [{ "_id": "p3", "sku": "B-2" }] })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "distinct sku insert: {body}");
    assert_eq!(body["insertedCount"].as_i64().unwrap(), 1);
}
