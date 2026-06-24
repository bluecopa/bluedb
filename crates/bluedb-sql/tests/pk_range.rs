//! Spike: primary-key **range** pushdown.
//!
//! gluesql plans a PK *equality* as a point `fetch_data` but leaves PK *ranges*
//! to a full `scan_data` + filter. bluedb injects a clustered-PK pseudo-index
//! (`__bluedb_pk`) so `plan_index` routes PK ranges to a bounded scan over the
//! pk-ordered data keyspace. These tests retire risk R2 from the composite-PK
//! design: (1) the routing actually happens (planner emits the pseudo-index for
//! ranges, still a point lookup for equality), and (2) the bounded scan returns
//! the correct, ordered rows for ranges, BETWEEN, keyset pagination, and DESC.

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use gluesql_core::ast::{IndexItem, IndexOperator, SetExpr, Statement, TableFactor};
use gluesql_core::prelude::{Glue, Payload, Value};
use gluesql_core::store::Planner;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn open_db() -> Arc<Db> {
    let store = Arc::new(InMemory::new());
    Arc::new(
        Db::open("pk-range-test", store)
            .await
            .expect("open slatedb"),
    )
}

fn select_rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select payload, got {other:?}"),
    }
}

async fn exec(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Payload {
    let mut payloads = glue.execute(sql).await.expect("execute sql");
    payloads.pop().unwrap()
}

/// The planner's chosen index for the single table in a `SELECT`'s `FROM`.
fn planned_index(stmt: Statement) -> Option<IndexItem> {
    match stmt {
        Statement::Query(query) => match query.body {
            SetExpr::Select(select) => match select.from.relation {
                TableFactor::Table { index, .. } => index,
                other => panic!("expected a table relation, got {other:?}"),
            },
            other => panic!("expected a SELECT body, got {other:?}"),
        },
        other => panic!("expected a query, got {other:?}"),
    }
}

async fn plan_for(db: &Arc<Db>, sql: &str) -> Option<IndexItem> {
    let conn = SlateDbStorage::new(Arc::clone(db));
    let parsed = gluesql_core::parse_sql::parse(sql).expect("parse");
    let stmt = gluesql_core::translate::translate(&parsed[0]).expect("translate");
    planned_index(conn.plan(stmt).await.expect("plan"))
}

#[tokio::test]
async fn pk_range_routes_to_clustered_pseudo_index() {
    let db = open_db().await;
    {
        let mut glue = Glue::new(SlateDbStorage::new(Arc::clone(&db)));
        exec(
            &mut glue,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .await;
    }

    // A PK *range* is routed to the clustered-PK pseudo-index (bounded scan).
    match plan_for(&db, "SELECT * FROM t WHERE id > 5;").await {
        Some(IndexItem::NonClustered { name, cmp_expr, .. }) => {
            assert_eq!(name, "__bluedb_pk");
            assert!(matches!(cmp_expr, Some((IndexOperator::Gt, _))));
        }
        other => panic!("expected NonClustered pseudo-index for a PK range, got {other:?}"),
    }

    // A PK *equality* is still a point lookup, not the pseudo-index.
    match plan_for(&db, "SELECT * FROM t WHERE id = 5;").await {
        Some(IndexItem::PrimaryKey(_)) => {}
        other => panic!("expected a point PrimaryKey lookup for PK equality, got {other:?}"),
    }
}

#[tokio::test]
async fn pk_range_keyset_and_order_return_correct_rows() {
    let db = open_db().await;
    let mut glue = Glue::new(SlateDbStorage::new(Arc::clone(&db)));
    exec(
        &mut glue,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .await;
    for i in 1..=10 {
        exec(&mut glue, &format!("INSERT INTO t VALUES ({i}, 'r{i}');")).await;
    }

    let ids = |rows: Vec<Vec<Value>>| -> Vec<i64> {
        rows.into_iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected i64 id, got {other:?}"),
            })
            .collect()
    };

    // Open range.
    let rows = select_rows(exec(&mut glue, "SELECT id FROM t WHERE id > 7 ORDER BY id;").await);
    assert_eq!(ids(rows), vec![8, 9, 10]);

    // Closed range (BETWEEN → id >= 3 AND id <= 5).
    let rows = select_rows(
        exec(
            &mut glue,
            "SELECT id FROM t WHERE id BETWEEN 3 AND 5 ORDER BY id;",
        )
        .await,
    );
    assert_eq!(ids(rows), vec![3, 4, 5]);

    // Keyset pagination: strictly-greater cursor, bounded page.
    let rows = select_rows(
        exec(
            &mut glue,
            "SELECT id FROM t WHERE id > 4 ORDER BY id LIMIT 3;",
        )
        .await,
    );
    assert_eq!(ids(rows), vec![5, 6, 7]);

    // Descending order over a range.
    let rows = select_rows(
        exec(
            &mut glue,
            "SELECT id FROM t WHERE id >= 8 ORDER BY id DESC;",
        )
        .await,
    );
    assert_eq!(ids(rows), vec![10, 9, 8]);

    // Equality still works (point lookup).
    let rows = select_rows(exec(&mut glue, "SELECT name FROM t WHERE id = 6;").await);
    assert_eq!(rows, vec![vec![Value::Str("r6".to_owned())]]);
}
