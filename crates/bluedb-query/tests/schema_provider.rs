//! Spike: `BluedbSchemaProvider` — automatic multi-table resolution. A query can
//! JOIN tables WITHOUT explicit `register_table`; DataFusion resolves each name
//! through the schema provider, which mints a `BluedbTableProvider` on demand
//! (each one the streaming Iceberg ∪ unsealed-tail union). This is the idiomatic
//! catalog seam the front-door flip needs so joins / CTEs / subqueries resolve
//! all their tables uniformly.

use std::sync::Arc;

use arrow_array::Int64Array;
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::{session_with_catalog, BluedbSchemaProvider};
use bluedb_sql::{CdcConfig, Database};
use datafusion::prelude::SessionContext;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

async fn make_engine() -> (Database, CdcConfig, Arc<LakehouseEngine>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Database::new(Arc::new(Db::open("bluedb", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();
    let file_io = object_store_file_io(store.clone(), "");
    let eng = LakehouseEngine::reopen(file_io, "lakehouse", "_", db.clone(), cdc.clone())
        .await
        .unwrap();
    (db, cdc, Arc::new(eng))
}

async fn ddl(db: &Database, sql: &str) {
    let mut g = Glue::new(db.connection_serialized());
    g.execute(sql).await.unwrap();
}

async fn dml(db: &Database, cdc: &CdcConfig, sql: &str) {
    let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
    g.execute(sql).await.unwrap();
}

/// A JOIN resolves both tables through the schema provider — no explicit
/// `register_table` — and carries post-seal (tail) freshness through the join.
#[tokio::test]
async fn schema_provider_resolves_join_without_explicit_registration() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("orders").await.unwrap();
    eng.enable_table("customers").await.unwrap();
    ddl(&db, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);").await;
    ddl(&db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO customers VALUES (1, 'alice'), (2, 'bob');").await;
    dml(&db, &cdc, "INSERT INTO orders VALUES (1, 100), (2, 200);").await;
    eng.seal().await.unwrap();
    // Post-seal (tail-only) update — the resolved provider must surface it.
    dml(&db, &cdc, "UPDATE orders SET amount = 250 WHERE id = 2;").await;

    let ctx = SessionContext::new();
    ctx.catalog("datafusion")
        .expect("default catalog")
        .register_schema("public", Arc::new(BluedbSchemaProvider::new(eng.clone())))
        .expect("register schema");

    // No register_table calls — resolution is entirely via the schema provider.
    let batches = ctx
        .sql(
            "SELECT c.name, o.amount \
             FROM orders o JOIN customers c ON o.id = c.id \
             ORDER BY o.amount",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let mut out = Vec::new();
    for b in &batches {
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let amts = b
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((names.value(i).to_string(), amts.value(i)));
        }
    }
    assert_eq!(
        out,
        vec![("alice".to_string(), 100), ("bob".to_string(), 250)],
        "join resolved via the schema provider, with tail freshness: {out:?}"
    );
}

/// [`session_with_catalog`] returns a context where `ctx.table(name)` resolves
/// to a `DataFrame` without any explicit `register_table`. This is the
/// DataFrame-ready entry point for the MongoDB aggregation pipeline (Task 16).
#[tokio::test]
async fn session_with_catalog_table_resolves_to_dataframe() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("orders").await.unwrap();
    ddl(&db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);").await;
    dml(&db, &cdc, "INSERT INTO orders VALUES (1, 100), (2, 200);").await;
    eng.seal().await.unwrap();

    let ctx = session_with_catalog(eng).await.unwrap();
    // ctx.table("orders") must succeed — the collection is resolvable as a DataFrame.
    let df = ctx.table("orders").await.unwrap();
    let batches = df.limit(0, Some(1)).unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "limit(1) returned one row from 'orders' DataFrame");
}
