use bluedb_engine::rest_sql;
use bluedb_rest::Param;
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::sync::Arc;

async fn glue() -> Glue<SlateDbStorage> {
    let db = Db::open("t", Arc::new(InMemory::new())).await.unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));
    g.execute("CREATE TABLE t (id INTEGER, body TEXT);")
        .await
        .unwrap();
    g.execute("INSERT INTO t (id, body) VALUES (1, 'a'), (2, 'b');")
        .await
        .unwrap();
    g
}

#[tokio::test]
async fn sql_surface_runs_single_param_select() {
    let mut g = glue().await;
    let out = rest_sql::execute_sql(
        &mut g,
        "SELECT body FROM t WHERE id = $1",
        &[Param::Int(2)],
        false,
    )
    .await
    .unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert_eq!(rows[0][0], Value::Str("b".into())),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn sql_surface_rejects_ddl() {
    let mut g = glue().await;
    assert!(
        rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sql_surface_rejects_multi_statement() {
    let mut g = glue().await;
    assert!(
        rest_sql::execute_sql(&mut g, "SELECT 1; SELECT 2;", &[], false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn admin_allows_ddl_when_arbitrary() {
    let mut g = glue().await;
    rest_sql::execute_sql(&mut g, "CREATE TABLE x (a INTEGER)", &[], true)
        .await
        .unwrap();
}

#[tokio::test]
async fn composite_primary_key_through_the_execute_sql_chokepoint() {
    // Proves the wiring: execute_sql applies the composite-PK rewrite for DDL
    // (admin), INSERT, and SELECT — the real server path.
    let db = Db::open("composite", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let mut g = Glue::new(SlateDbStorage::new(Arc::new(db)));

    // Composite DDL goes through the arbitrary (admin) surface.
    rest_sql::execute_sql(
        &mut g,
        "CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))",
        &[],
        true,
    )
    .await
    .unwrap();

    // INSERT + SELECT go through the /sql (non-arbitrary) surface, inline literals.
    rest_sql::execute_sql(
        &mut g,
        "INSERT INTO t (a, b, payload) VALUES (1, 'x', 'p1')",
        &[],
        false,
    )
    .await
    .unwrap();
    rest_sql::execute_sql(
        &mut g,
        "INSERT INTO t (a, b, payload) VALUES (1, 'y', 'p2')",
        &[],
        false,
    )
    .await
    .unwrap();

    let out = rest_sql::execute_sql(
        &mut g,
        "SELECT payload FROM t WHERE a = 1 AND b = 'x'",
        &[],
        false,
    )
    .await
    .unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => assert_eq!(rows[0][0], Value::Str("p1".into())),
        other => panic!("{other:?}"),
    }

    // SELECT * hides the surrogate (three user columns).
    let out = rest_sql::execute_sql(
        &mut g,
        "SELECT * FROM t WHERE a = 1 AND b = 'y'",
        &[],
        false,
    )
    .await
    .unwrap();
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => {
            assert_eq!(rows[0].len(), 3, "__bluedb_pk must be hidden from SELECT *");
        }
        other => panic!("{other:?}"),
    }
}
