//! Parameterized data-plane execution + injection-proofing.
use std::sync::Arc;

use bluedb_engine::rest_sql;
use bluedb_rest::{DeleteRequest, Filter, InsertRequest, Operator, RestQuery};
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("t", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    g.execute("CREATE TABLE docs (id INTEGER, body TEXT);").await.unwrap();
    g
}

fn rows(p: Payload) -> Vec<Vec<Value>> {
    match p { Payload::Select { rows, .. } => rows, other => panic!("not select: {other:?}") }
}

#[tokio::test]
async fn insert_batch_then_query_with_params() {
    let mut g = glue().await;
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), "alpha".into()], vec!["2".into(), "beta".into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();

    let q = RestQuery { table: "docs".into(), filters: vec![Filter::new("id", Operator::Eq, "2")], ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    let r = rows(out.into_iter().next().unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Str("beta".into()));
}

#[tokio::test]
async fn injection_payload_is_stored_as_data_not_executed() {
    let mut g = glue().await;
    let payload = "'); DROP TABLE docs; --";
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), payload.into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();

    let q = RestQuery { table: "docs".into(), ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    let r = rows(out.into_iter().next().unwrap());
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Str(payload.into()));
}

#[tokio::test]
async fn delete_with_param() {
    let mut g = glue().await;
    let req = InsertRequest {
        table: "docs".into(),
        columns: vec!["id".into(), "body".into()],
        rows: vec![vec!["1".into(), "x".into()], vec!["2".into(), "y".into()]],
    };
    rest_sql::execute_insert_batch(&mut g, &req).await.unwrap();
    let d = DeleteRequest { table: "docs".into(), filters: vec![Filter::new("id", Operator::Eq, "1")] };
    rest_sql::execute_delete(&mut g, &d).await.unwrap();
    let q = RestQuery { table: "docs".into(), ..Default::default() };
    let out = rest_sql::execute_query(&mut g, &q).await.unwrap();
    assert_eq!(rows(out.into_iter().next().unwrap()).len(), 1);
}
