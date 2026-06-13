//! End-to-end REST → SQL execution through `bluedb-engine`.

use std::sync::Arc;

use bluedb_engine::{rest_sql, EngineError, SlateDbStorage};
use bluedb_rest::{DeleteRequest, Filter, InsertRequest, Operator, UpdateRequest};
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue() -> Glue<SlateDbStorage> {
    let db = Db::open("engine-rest-test", Arc::new(InMemory::new()))
        .await
        .expect("open slatedb");
    Glue::new(SlateDbStorage::new(Arc::new(db)))
}

fn select_rows(payloads: Vec<Payload>) -> Vec<Vec<Value>> {
    match payloads.into_iter().next().expect("one payload") {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn rest_select_translates_and_executes() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
        .await
        .unwrap();
    glue.execute("INSERT INTO users VALUES (1,'alice',30),(2,'bob',25),(3,'carol',40);")
        .await
        .unwrap();

    // GET /users?select=name&age=gt.28&order=name.asc
    let payloads = rest_sql::execute_query_str(&mut glue, "users", "select=name&age=gt.28&order=name.asc")
        .await
        .expect("rest query");
    assert_eq!(
        select_rows(payloads),
        vec![
            vec![Value::Str("alice".to_owned())], // age 30
            vec![Value::Str("carol".to_owned())], // age 40
        ],
        "age > 28, projected to name, ordered ascending"
    );
}

#[tokio::test]
async fn rest_insert_update_delete_round_trip() {
    let mut glue = new_glue().await;
    glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);").await.unwrap();

    rest_sql::execute_insert(
        &mut glue,
        &InsertRequest {
            table: "t".into(),
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec!["1".into(), "alice".into()],
                vec!["2".into(), "bob".into()],
            ],
        },
    )
    .await
    .expect("insert");

    rest_sql::execute_update(
        &mut glue,
        &UpdateRequest {
            table: "t".into(),
            assignments: vec![("name".into(), "ALICE".into())],
            filters: vec![Filter::new("id", Operator::Eq, "1")],
        },
    )
    .await
    .expect("update");

    rest_sql::execute_delete(
        &mut glue,
        &DeleteRequest {
            table: "t".into(),
            filters: vec![Filter::new("id", Operator::Eq, "2")],
        },
    )
    .await
    .expect("delete");

    let rows = select_rows(glue.execute("SELECT id, name FROM t ORDER BY id;").await.unwrap());
    assert_eq!(rows, vec![vec![Value::I64(1), Value::Str("ALICE".to_owned())]]);
}

#[tokio::test]
async fn rest_translation_error_is_distinct_from_sql_error() {
    let mut glue = new_glue().await;
    // A malicious table identifier is rejected by the REST translator (identifier
    // allow-list) — surfaced as EngineError::Rest, never reaching the SQL engine.
    let err = rest_sql::execute_query_str(&mut glue, "t; DROP TABLE x; --", "id=eq.1")
        .await
        .expect_err("must reject bad identifier");
    assert!(matches!(err, EngineError::Rest(_)), "got {err:?}");
}
