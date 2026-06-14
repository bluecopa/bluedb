use std::sync::Arc;
use bluedb_engine::rest_sql;
use bluedb_rest::Param;
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("t", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    g.execute("CREATE TABLE t (id INTEGER, body TEXT);").await.unwrap();
    g.execute("INSERT INTO t (id, body) VALUES (1, 'a'), (2, 'b');").await.unwrap();
    g
}

#[tokio::test]
async fn sql_surface_runs_single_param_select() {
    let mut g = glue().await;
    let out = rest_sql::execute_sql(&mut g, "SELECT body FROM t WHERE id = $1", &[Param::Int(2)], false).await.unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert_eq!(rows[0][0], Value::Str("b".into())),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn sql_surface_rejects_ddl() {
    let mut g = glue().await;
    assert!(rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], false).await.is_err());
}

#[tokio::test]
async fn sql_surface_rejects_multi_statement() {
    let mut g = glue().await;
    assert!(rest_sql::execute_sql(&mut g, "SELECT 1; SELECT 2;", &[], false).await.is_err());
}

#[tokio::test]
async fn admin_allows_ddl_when_arbitrary() {
    let mut g = glue().await;
    rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], true).await.unwrap();
}
