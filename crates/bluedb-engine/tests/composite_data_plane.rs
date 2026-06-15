//! Composite primary keys on the parameterized surfaces: `$N` params on `/sql`,
//! the PostgREST-style `/tables` data plane, and cross-surface key consistency
//! (a key inserted one way is found the other way — the encoding is coerced to
//! the column type, so it's surface-independent).

use std::sync::Arc;

use bluedb_engine::rest_sql;
use bluedb_rest::{InsertRequest, Param};
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("composite-dp", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    rest_sql::execute_sql(
        &mut g,
        "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
        &[],
        true,
    )
    .await
    .unwrap();
    g
}

fn rows(payloads: Vec<Payload>) -> Vec<Vec<Value>> {
    match payloads.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn parameterized_sql_insert_and_select() {
    let mut g = glue().await;
    // PK components supplied as bound $N params (not inline literals).
    rest_sql::execute_sql(
        &mut g,
        "INSERT INTO t (a, b, payload) VALUES ($1, $2, $3)",
        &[Param::Int(1), Param::Str("x".into()), Param::Str("p1".into())],
        false,
    )
    .await
    .unwrap();

    let out = rest_sql::execute_sql(
        &mut g,
        "SELECT payload FROM t WHERE a = $1 AND b = $2",
        &[Param::Int(1), Param::Str("x".into())],
        false,
    )
    .await
    .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Str("p1".into())]]);
}

#[tokio::test]
async fn data_plane_insert_and_query() {
    let mut g = glue().await;
    // PostgREST-style insert (values are strings, typed by content at render).
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["a".into(), "b".into(), "payload".into()],
        rows: vec![
            vec!["1".into(), "x".into(), "p1".into()],
            vec!["2".into(), "y".into(), "p2".into()],
        ],
    };
    rest_sql::execute_insert(&mut g, &req).await.unwrap();

    // GET /tables/t?a=eq.1&b=eq.x → composite predicate rewrite over params.
    let out = rest_sql::execute_query_str(&mut g, "t", "select=payload&a=eq.1&b=eq.x")
        .await
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Str("p1".into())]]);
}

#[tokio::test]
async fn cross_surface_key_consistency() {
    let mut g = glue().await;
    // Insert via the data plane; find it via inline /sql — the surrogate encodes
    // identically regardless of how the key value arrived.
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["a".into(), "b".into(), "payload".into()],
        rows: vec![vec!["1".into(), "x".into(), "dp".into()]],
    };
    rest_sql::execute_insert(&mut g, &req).await.unwrap();
    let out = rest_sql::execute_sql(&mut g, "SELECT payload FROM t WHERE a = 1 AND b = 'x'", &[], false)
        .await
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Str("dp".into())]], "data-plane key not found via inline SQL");

    // And the reverse: insert inline, find it via the data plane.
    rest_sql::execute_sql(&mut g, "INSERT INTO t (a, b, payload) VALUES (2, 'y', 'sql')", &[], false)
        .await
        .unwrap();
    let out = rest_sql::execute_query_str(&mut g, "t", "select=payload&a=eq.2&b=eq.y")
        .await
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Str("sql".into())]], "inline key not found via data plane");

    // Numeric-spelling canonicalization on a numeric column: `5` and `5` agree
    // whether inline or a data-plane param (both coerce to the INTEGER column).
    rest_sql::execute_sql(&mut g, "INSERT INTO t (a, b, payload) VALUES (5, 'k', 'n')", &[], false)
        .await
        .unwrap();
    let out = rest_sql::execute_query_str(&mut g, "t", "select=payload&a=eq.5&b=eq.k")
        .await
        .unwrap();
    assert_eq!(rows(out), vec![vec![Value::Str("n".into())]]);
}
