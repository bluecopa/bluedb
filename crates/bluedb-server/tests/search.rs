//! Embedded-server integration tests for the Elasticsearch-shaped `/collections`
//! search surface (Tasks 9–14). Drives the real axum router via
//! `tower::ServiceExt::oneshot` — no socket — against an in-memory store on the
//! active writer, so search reflects just-written docs with NO seal step
//! (read-your-writes).
//!
//! Each test is independent: a fresh app and/or a fresh collection name, then
//! `searchIndex` (declare mapping) → `insert`/`update`/`delete` → `search`, with
//! SPECIFIC assertions (counts, ids, ordering, field presence).
//!
//! Errors come back in bluedb's STANDARD error body (an object with an `error`
//! field) with the right STATUS — NOT the ES error envelope (a documented v1
//! limitation) — so error tests assert on STATUS (+ that `error` is present).

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
    let writer = Arc::new(WriterController::new(
        node_id,
        lease,
        Arc::new(SystemClock),
        TTL,
        MARGIN,
    ));
    AppState::new(store, "bluedb", writer)
}

/// A fresh in-memory app, promoted to the active writer (search writes +
/// `searchIndex` require the active writer; the read path uses the same bound
/// substrate, which is what gives us read-your-writes with no seal).
async fn app() -> Router {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = node("test-node", store, Arc::new(LocalLeaseProvider::new()));
    state.promote().await.expect("promote");
    build_app(state)
}

/// Send a request with an optional JSON body and optional `X-Bluedb-Tenant`,
/// returning `(status, json_body)`.
async fn call_tenant(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
    tenant: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        builder = builder.header("x-bluedb-tenant", t);
    }
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

/// The common, default-tenant case.
async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    json_body: Option<Value>,
) -> (StatusCode, Value) {
    call_tenant(app, method, uri, json_body, None).await
}

// ---- small helpers --------------------------------------------------------

/// Declare (or replace) a collection's search mapping. Asserts a 200 ack.
async fn declare_index(app: &Router, coll: &str, mapping: Value) {
    let (status, body) = call(
        app,
        "POST",
        &format!("/collections/{coll}/searchIndex"),
        Some(mapping),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "searchIndex [{coll}] failed: {body}"
    );
    assert_eq!(body["acknowledged"], json!(true), "searchIndex ack: {body}");
}

/// Insert a batch of documents into a collection. Asserts a 200 + the count.
async fn insert_docs(app: &Router, coll: &str, docs: Value) {
    let n = docs.as_array().map(|a| a.len()).unwrap_or(0);
    let (status, body) = call(
        app,
        "POST",
        &format!("/collections/{coll}/insert"),
        Some(json!({ "documents": docs })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "insert into [{coll}] failed: {body}"
    );
    assert_eq!(
        body["insertedCount"].as_i64().unwrap() as usize,
        n,
        "insertedCount: {body}"
    );
}

/// `POST /collections/{coll}/search` with the given ES body → `(status, body)`.
async fn search(app: &Router, coll: &str, body: Value) -> (StatusCode, Value) {
    call(
        app,
        "POST",
        &format!("/collections/{coll}/search"),
        Some(body),
    )
    .await
}

/// The hit array from a search response.
fn hits(body: &Value) -> &Vec<Value> {
    body["hits"]["hits"].as_array().expect("hits.hits array")
}

/// `hits.total.value` as i64.
fn total(body: &Value) -> i64 {
    body["hits"]["total"]["value"]
        .as_i64()
        .expect("hits.total.value")
}

/// The `_source.<field>` string of a hit.
fn src_str<'a>(hit: &'a Value, field: &str) -> &'a str {
    hit["_source"][field]
        .as_str()
        .unwrap_or_else(|| panic!("hit._source.{field} not a string: {hit}"))
}

/// A standard mapping used by several tests: english title/body, keyword tag,
/// integer year.
fn standard_mapping() -> Value {
    json!({
        "fields": {
            "title": { "analyzer": "english" },
            "body":  { "analyzer": "english" },
            "tag":   { "type": "keyword" },
            "year":  { "type": "integer" }
        }
    })
}

// ---------------------------------------------------------------------------
// 1. declare mapping → insert → match (read-your-writes; NO seal)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn declare_index_then_search_match() {
    let app = app().await;
    declare_index(&app, "articles", standard_mapping()).await;

    insert_docs(
        &app,
        "articles",
        json!([
            { "title": "Dogs", "body": "dogs are loyal companions", "tag": "pets", "year": 2021 },
            { "title": "Cats", "body": "cats are independent", "tag": "pets", "year": 2022 },
            { "title": "Cars", "body": "engines and wheels", "tag": "tech", "year": 2019 }
        ]),
    )
    .await;

    // Read-your-writes: search immediately, no seal.
    let (status, body) = search(
        &app,
        "articles",
        json!({ "query": { "match": { "body": "dogs" } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(total(&body), 1, "exactly the dogs doc: {body}");
    let hs = hits(&body);
    assert_eq!(hs.len(), 1, "one hit: {body}");

    let id = hs[0]["_id"].as_str().expect("hit._id");
    assert_eq!(id.len(), 24, "_id should be a 24-char hex, got '{id}'");
    assert!(
        id.chars().all(|c| c.is_ascii_hexdigit()),
        "_id should be hex: '{id}'"
    );

    assert_eq!(
        src_str(&hs[0], "title"),
        "Dogs",
        "the dogs doc's title: {body}"
    );
    assert_eq!(
        hs[0]["_index"],
        json!("articles"),
        "_index is the collection: {body}"
    );
    assert!(
        hs[0]["_score"].as_f64().unwrap_or(0.0) > 0.0,
        "scored hit: {body}"
    );
}

// ---------------------------------------------------------------------------
// 2. bool must/should/must_not
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bool_must_should_must_not() {
    let app = app().await;
    declare_index(&app, "zoo", standard_mapping()).await;

    insert_docs(
        &app,
        "zoo",
        json!([
            // matches "cats" but is tagged animals → excluded by must_not.
            { "title": "Wild", "body": "cats roam the savanna", "tag": "animals", "year": 2020 },
            // matches "cats" and is NOT tagged animals → the only survivor.
            { "title": "Home", "body": "house cats nap a lot", "tag": "pets", "year": 2021 },
            // doesn't match "cats".
            { "title": "Birds", "body": "parrots can talk", "tag": "pets", "year": 2022 }
        ]),
    )
    .await;

    let q = json!({
        "query": { "bool": {
            "must":     [{ "match": { "body": "cats" } }],
            "must_not": [{ "term":  { "tag": "animals" } }]
        }}
    });
    let (status, body) = search(&app, "zoo", q).await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(total(&body), 1, "only the non-animals cats doc: {body}");
    let hs = hits(&body);
    assert_eq!(hs.len(), 1);
    assert_eq!(
        src_str(&hs[0], "title"),
        "Home",
        "the surviving doc: {body}"
    );
    assert_eq!(src_str(&hs[0], "tag"), "pets");
}

// ---------------------------------------------------------------------------
// 3. range on an integer field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn range_on_integer() {
    let app = app().await;
    declare_index(&app, "years", standard_mapping()).await;

    insert_docs(
        &app,
        "years",
        json!([
            { "title": "old",   "body": "x", "tag": "a", "year": 2018 },
            { "title": "edge",  "body": "x", "tag": "a", "year": 2020 },
            { "title": "new",   "body": "x", "tag": "a", "year": 2023 }
        ]),
    )
    .await;

    let (status, body) = search(
        &app,
        "years",
        json!({ "query": { "range": { "year": { "gte": 2020 } } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(total(&body), 2, "two docs with year >= 2020: {body}");
    let titles: Vec<&str> = hits(&body).iter().map(|h| src_str(h, "title")).collect();
    assert!(
        titles.contains(&"edge"),
        "2020 (gte boundary) included: {titles:?}"
    );
    assert!(titles.contains(&"new"), "2023 included: {titles:?}");
    assert!(!titles.contains(&"old"), "2018 excluded: {titles:?}");
}

// ---------------------------------------------------------------------------
// 4. match_phrase (order-sensitive)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn match_phrase() {
    let app = app().await;
    declare_index(&app, "phrases", standard_mapping()).await;

    insert_docs(
        &app,
        "phrases",
        json!([{ "title": "fox", "body": "the quick brown fox", "tag": "a", "year": 2020 }]),
    )
    .await;

    // In-order phrase → matches.
    let (status, body) = search(
        &app,
        "phrases",
        json!({ "query": { "match_phrase": { "body": "quick brown" } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    assert_eq!(total(&body), 1, "in-order phrase matches: {body}");

    // Reversed phrase → no match (the words exist but not adjacent in order).
    let (status, body) = search(
        &app,
        "phrases",
        json!({ "query": { "match_phrase": { "body": "brown quick" } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    assert_eq!(total(&body), 0, "reversed phrase does NOT match: {body}");
    assert!(
        hits(&body).is_empty(),
        "no hits for reversed phrase: {body}"
    );
}

// ---------------------------------------------------------------------------
// 5. term on a keyword field (exact)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn term_keyword_exact() {
    let app = app().await;
    declare_index(&app, "kw", standard_mapping()).await;

    insert_docs(
        &app,
        "kw",
        json!([
            { "title": "a", "body": "x", "tag": "pets",   "year": 2020 },
            { "title": "b", "body": "x", "tag": "pets",   "year": 2021 },
            { "title": "c", "body": "x", "tag": "tech",   "year": 2022 }
        ]),
    )
    .await;

    let (status, body) = search(
        &app,
        "kw",
        json!({ "query": { "term": { "tag": "pets" } }, "size": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(total(&body), 2, "two pets docs: {body}");
    assert!(
        hits(&body).iter().all(|h| src_str(h, "tag") == "pets"),
        "all hits are pets: {body}"
    );
}

// ---------------------------------------------------------------------------
// 6. exists query excludes docs missing the field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exists_query() {
    let app = app().await;
    declare_index(&app, "maybe_year", standard_mapping()).await;

    insert_docs(
        &app,
        "maybe_year",
        json!([
            { "title": "has", "body": "x", "tag": "a", "year": 2020 },
            // omits `year` entirely.
            { "title": "missing", "body": "x", "tag": "a" }
        ]),
    )
    .await;

    let (status, body) = search(
        &app,
        "maybe_year",
        json!({ "query": { "exists": { "field": "year" } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(total(&body), 1, "only the doc that has `year`: {body}");
    let hs = hits(&body);
    assert_eq!(hs.len(), 1);
    assert_eq!(
        src_str(&hs[0], "title"),
        "has",
        "the doc with a year: {body}"
    );
}

// ---------------------------------------------------------------------------
// 7. from/size pagination — page is bounded, total is the full match count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn from_size_pagination() {
    let app = app().await;
    declare_index(&app, "paged", standard_mapping()).await;

    insert_docs(
        &app,
        "paged",
        json!([
            { "title": "p0", "body": "x", "tag": "page", "year": 2020 },
            { "title": "p1", "body": "x", "tag": "page", "year": 2021 },
            { "title": "p2", "body": "x", "tag": "page", "year": 2022 },
            { "title": "p3", "body": "x", "tag": "page", "year": 2023 },
            { "title": "p4", "body": "x", "tag": "page", "year": 2024 }
        ]),
    )
    .await;

    let (status, body) = search(
        &app,
        "paged",
        json!({ "query": { "term": { "tag": "page" } }, "from": 2, "size": 2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    assert_eq!(
        total(&body),
        5,
        "total reflects ALL matches, not the page: {body}"
    );
    assert_eq!(
        hits(&body).len(),
        2,
        "the page holds exactly `size` hits: {body}"
    );
}

// ---------------------------------------------------------------------------
// 8. _source: false (no _source) and _source: [list] (trimmed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn source_false_and_field_list() {
    let app = app().await;
    declare_index(&app, "src", standard_mapping()).await;

    insert_docs(
        &app,
        "src",
        json!([{ "title": "Hello", "body": "dogs bark", "tag": "pets", "year": 2020 }]),
    )
    .await;

    // _source: false → the hit carries NO _source at all.
    let (status, body) = search(
        &app,
        "src",
        json!({ "query": { "match": { "body": "dogs" } }, "_source": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let hs = hits(&body);
    assert_eq!(hs.len(), 1, "{body}");
    assert!(
        hs[0].get("_source").is_none(),
        "_source:false → no _source key: {body}"
    );

    // _source: ["title"] → _source is present but has only `title`.
    let (status, body) = search(
        &app,
        "src",
        json!({ "query": { "match": { "body": "dogs" } }, "_source": ["title"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let hs = hits(&body);
    assert_eq!(hs.len(), 1, "{body}");
    let source = hs[0]["_source"].as_object().expect("_source object");
    assert_eq!(source.len(), 1, "_source trimmed to one field: {body}");
    assert_eq!(
        source.get("title"),
        Some(&json!("Hello")),
        "kept title: {body}"
    );
    assert!(source.get("body").is_none(), "dropped body: {body}");
    assert!(source.get("tag").is_none(), "dropped tag: {body}");
}

// ---------------------------------------------------------------------------
// 9. highlight wraps matched terms in <em>…</em>
// ---------------------------------------------------------------------------

#[tokio::test]
async fn highlight_wraps_terms() {
    let app = app().await;
    // `standard` analyzer (lowercase, no stemming) so the literal "dogs" token
    // matches the source word "dogs" verbatim → exact <em>dogs</em>.
    declare_index(
        &app,
        "hl",
        json!({ "fields": { "body": { "analyzer": "standard" }, "tag": { "type": "keyword" } } }),
    )
    .await;

    insert_docs(
        &app,
        "hl",
        json!([{ "body": "the dogs ran fast", "tag": "a" }]),
    )
    .await;

    let (status, body) = search(
        &app,
        "hl",
        json!({
            "query": { "match": { "body": "dogs" } },
            "highlight": { "fields": { "body": {} } }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    let hs = hits(&body);
    assert_eq!(hs.len(), 1, "{body}");
    let snippet = hs[0]["highlight"]["body"][0]
        .as_str()
        .unwrap_or_else(|| panic!("highlight.body[0] missing: {body}"));
    assert!(
        snippet.contains("<em>"),
        "snippet should wrap a term: '{snippet}'"
    );
    assert!(
        snippet.contains("<em>dogs</em>"),
        "the matched term is wrapped: '{snippet}'"
    );
}

// ---------------------------------------------------------------------------
// 10. update is reflected in search (re-index on the active writer)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_reflected_in_search() {
    let app = app().await;
    declare_index(
        &app,
        "mut",
        json!({ "fields": { "body": { "analyzer": "english" } } }),
    )
    .await;

    insert_docs(&app, "mut", json!([{ "body": "alpha" }])).await;

    // The id we just inserted, fetched back via a find on `_id`-less filter.
    let (status, body) = search(
        &app,
        "mut",
        json!({ "query": { "match": { "body": "alpha" } } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(total(&body), 1, "alpha hits before update: {body}");
    let id = hits(&body)[0]["_id"].as_str().expect("hit _id").to_string();

    // Update body → "beta".
    let (status, body) = call(
        &app,
        "POST",
        "/collections/mut/update",
        Some(json!({ "filter": { "_id": id }, "update": { "$set": { "body": "beta" } } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update failed: {body}");
    assert_eq!(
        body["modifiedCount"].as_i64().unwrap(),
        1,
        "one doc modified: {body}"
    );

    // "alpha" no longer matches; "beta" now matches.
    let (_, body) = search(
        &app,
        "mut",
        json!({ "query": { "match": { "body": "alpha" } } }),
    )
    .await;
    assert_eq!(
        total(&body),
        0,
        "alpha no longer matches after update: {body}"
    );

    let (_, body) = search(
        &app,
        "mut",
        json!({ "query": { "match": { "body": "beta" } } }),
    )
    .await;
    assert_eq!(total(&body), 1, "beta matches after update: {body}");
}

// ---------------------------------------------------------------------------
// 11. delete is reflected in search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_reflected_in_search() {
    let app = app().await;
    declare_index(
        &app,
        "del",
        json!({ "fields": { "body": { "analyzer": "english" } } }),
    )
    .await;

    insert_docs(&app, "del", json!([{ "body": "ephemeral" }])).await;

    let (_, body) = search(
        &app,
        "del",
        json!({ "query": { "match": { "body": "ephemeral" } } }),
    )
    .await;
    assert_eq!(total(&body), 1, "hits before delete: {body}");
    let id = hits(&body)[0]["_id"].as_str().expect("hit _id").to_string();

    let (status, body) = call(
        &app,
        "POST",
        "/collections/del/delete",
        Some(json!({ "filter": { "_id": id } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete failed: {body}");
    assert_eq!(
        body["deletedCount"].as_i64().unwrap(),
        1,
        "one doc deleted: {body}"
    );

    let (_, body) = search(
        &app,
        "del",
        json!({ "query": { "match": { "body": "ephemeral" } } }),
    )
    .await;
    assert_eq!(total(&body), 0, "no hits after delete: {body}");
    assert!(hits(&body).is_empty(), "{body}");
}

// ---------------------------------------------------------------------------
// 12. numeric sort (ascending by an integer field)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn numeric_sort() {
    let app = app().await;
    declare_index(&app, "sorted", standard_mapping()).await;

    insert_docs(
        &app,
        "sorted",
        json!([
            { "title": "mid",   "body": "x", "tag": "s", "year": 2021 },
            { "title": "first", "body": "x", "tag": "s", "year": 2019 },
            { "title": "last",  "body": "x", "tag": "s", "year": 2024 }
        ]),
    )
    .await;

    let (status, body) = search(
        &app,
        "sorted",
        json!({ "query": { "term": { "tag": "s" } }, "sort": [{ "year": "asc" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    let hs = hits(&body);
    assert_eq!(hs.len(), 3, "all three docs: {body}");
    let years: Vec<i64> = hs
        .iter()
        .map(|h| h["_source"]["year"].as_i64().unwrap())
        .collect();
    assert_eq!(years, vec![2019, 2021, 2024], "ascending by year: {body}");
    assert_eq!(
        src_str(&hs[0], "title"),
        "first",
        "smallest year first: {body}"
    );
}

// ---------------------------------------------------------------------------
// 13. a query on an unmapped field is a client error (400)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unmapped_field_errors() {
    let app = app().await;
    declare_index(&app, "strict", standard_mapping()).await;
    insert_docs(
        &app,
        "strict",
        json!([{ "title": "a", "body": "x", "tag": "t", "year": 2020 }]),
    )
    .await;

    let (status, body) = search(
        &app,
        "strict",
        json!({ "query": { "match": { "nope": "x" } } }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unmapped field → 400: {body}"
    );
    // bluedb's STANDARD error body (an object with an `error` field) — NOT the ES
    // error envelope (documented v1 limitation).
    assert!(body.get("error").is_some(), "error body present: {body}");
}

// ---------------------------------------------------------------------------
// 14. tenant isolation — t1's mapping + docs are invisible to t2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenant_isolation() {
    let app = app().await;

    // Declare + insert under tenant t1.
    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/searchIndex",
        Some(standard_mapping()),
        Some("t1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "t1 searchIndex: {body}");

    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/insert",
        Some(json!({ "documents": [{ "title": "secret", "body": "t1 only dogs", "tag": "x", "year": 2020 }] })),
        Some("t1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "t1 insert: {body}");

    // t1 can search and see its doc (read-your-writes).
    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/search",
        Some(json!({ "query": { "match": { "body": "dogs" } } })),
        Some("t1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "t1 search: {body}");
    assert_eq!(total(&body), 1, "t1 sees its own doc: {body}");

    // t2 has NO mapping for the same collection name → 404, and never t1's docs.
    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/search",
        Some(json!({ "query": { "match": { "body": "dogs" } } })),
        Some("t2"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "t2 has no mapping → 404: {body}"
    );
    assert!(
        body.get("error").is_some() || body.get("code").is_some(),
        "error body: {body}"
    );

    // Even after t2 declares its OWN mapping, it sees zero of t1's docs.
    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/searchIndex",
        Some(standard_mapping()),
        Some("t2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "t2 searchIndex: {body}");
    assert_eq!(
        body["backfilled"],
        json!(0),
        "t2 has no docs to backfill: {body}"
    );

    let (status, body) = call_tenant(
        &app,
        "POST",
        "/collections/shared/search",
        Some(json!({ "query": { "match": { "body": "dogs" } } })),
        Some("t2"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "t2 search after own mapping: {body}"
    );
    assert_eq!(total(&body), 0, "t2 never sees t1's docs: {body}");
}

// ---------------------------------------------------------------------------
// 15. searching a collection with no search mapping is a 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_without_mapping_404() {
    let app = app().await;
    // A collection that exists (has docs) but was never declared for search.
    insert_docs(&app, "unindexed", json!([{ "body": "hello" }])).await;

    let (status, body) = search(&app, "unindexed", json!({ "query": { "match_all": {} } })).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "no mapping → 404: {body}");
    assert!(
        body.get("error").is_some() || body.get("code").is_some(),
        "error body: {body}"
    );

    // A collection that doesn't exist at all is also a 404.
    let (status, body) = search(&app, "ghost", json!({ "query": { "match_all": {} } })).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "missing collection → 404: {body}"
    );
}

// ---------------------------------------------------------------------------
// 16. an invalid collection identifier is rejected (400) on the search surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejects_invalid_collection_name() {
    let app = app().await;
    // `1bad` starts with a digit → rejected by `ident` (^[A-Za-z_][A-Za-z0-9_]*$),
    // but is URL-safe so it reaches the handler (where the validation lives).
    // Must be a 400 — NOT a 200, and NOT a 500 from interpolating it into SQL.
    let (status, body) = search(&app, "1bad", json!({ "query": { "match_all": {} } })).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "search bad name → 400: {body}"
    );

    let (status, body) = call(
        &app,
        "POST",
        "/collections/1bad/searchIndex",
        Some(standard_mapping()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "searchIndex bad name → 400: {body}"
    );
}

// ---------------------------------------------------------------------------
// 17. an oversized result window (from + size) is rejected (400)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejects_oversized_window() {
    let app = app().await;
    declare_index(&app, "window", standard_mapping()).await;
    insert_docs(
        &app,
        "window",
        json!([{ "title": "a", "body": "x", "tag": "t", "year": 2020 }]),
    )
    .await;

    let (status, body) = search(
        &app,
        "window",
        json!({ "query": { "match_all": {} }, "size": 20000 }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "size beyond max window → 400: {body}"
    );
}

// ---------------------------------------------------------------------------
// 18. a bare-field sort string defaults to ascending (ES fidelity)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bare_field_sort_defaults_ascending() {
    let app = app().await;
    declare_index(&app, "bare_sort", standard_mapping()).await;

    insert_docs(
        &app,
        "bare_sort",
        json!([
            { "title": "y2024", "body": "x", "tag": "s", "year": 2024 },
            { "title": "y2019", "body": "x", "tag": "s", "year": 2019 },
            { "title": "y2021", "body": "x", "tag": "s", "year": 2021 }
        ]),
    )
    .await;

    // Bare field name (no explicit order) → ascending, like Elasticsearch.
    let (status, body) = search(
        &app,
        "bare_sort",
        json!({ "query": { "match_all": {} }, "sort": ["year"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");

    let hs = hits(&body);
    assert_eq!(hs.len(), 3, "all three docs: {body}");
    assert_eq!(
        hs[0]["_source"]["year"].as_i64().unwrap(),
        2019,
        "bare-field sort is ascending → smallest year first: {body}"
    );
}
