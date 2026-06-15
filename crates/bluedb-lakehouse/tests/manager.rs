//! Multi-tenant manager: each tenant mirrors into its own Iceberg namespace,
//! tenants are isolated, and the tenant set is restored on a fresh manager
//! (the promote/failover path).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use bluedb_lakehouse::{object_store_file_io, LakehouseConfig, LakehouseEngine, LakehouseManager};
use bluedb_sql::{CdcConfig, Database};
use futures::TryStreamExt;
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

/// Background loops idle far longer than the test runs, so sealing is driven
/// explicitly via `seal_all` for determinism.
fn cfg() -> LakehouseConfig {
    LakehouseConfig {
        seal_debounce: Duration::from_secs(30),
        seal_max_interval: Duration::from_secs(30),
        compaction_interval: Duration::from_secs(30),
        max_data_files: 1024,
        max_delete_files: 1024,
    }
}

/// Create table `docs` and insert rows under `tenant`, with CDC on.
async fn write_docs(db: &Database, cdc: &CdcConfig, tenant: &str, rows: &[(i64, &str)]) {
    {
        let mut g = Glue::new(db.connection_for_tenant(tenant).serialize_writes());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    let mut g = Glue::new(
        db.connection_for_tenant(tenant)
            .serialize_writes()
            .with_cdc(cdc.clone()),
    );
    for (id, body) in rows {
        g.execute(&format!("INSERT INTO docs VALUES ({id}, '{body}');"))
            .await
            .unwrap();
    }
}

/// Read a `(id BIGINT, body TEXT)` mirror back as id -> body (columns by index).
async fn read_docs(engine: &LakehouseEngine) -> BTreeMap<i64, String> {
    let schema = engine.fetch_schema("docs").await.unwrap();
    let writer = engine.writer_for("docs", &schema, &[]).await.unwrap();
    let table = writer.to_table().unwrap();
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bodies = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.insert(ids.value(i), bodies.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn tenants_mirror_into_isolated_namespaces() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Database::new(Arc::new(Db::open("bluedb", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();
    let mgr = LakehouseManager::open(
        object_store_file_io(store.clone(), ""),
        "lakehouse",
        db.clone(),
        cdc.clone(),
        cfg(),
    )
    .await
    .unwrap();

    // Two tenants each mirror their own `docs` with different data.
    mgr.engine_for("acme")
        .await
        .unwrap()
        .enable_table("docs")
        .await
        .unwrap();
    mgr.engine_for("globex")
        .await
        .unwrap()
        .enable_table("docs")
        .await
        .unwrap();
    write_docs(&db, &cdc, "acme", &[(1, "acme-a"), (2, "acme-b")]).await;
    write_docs(&db, &cdc, "globex", &[(1, "globex-x")]).await;
    mgr.seal_all().await.unwrap();

    // Each namespace exists and holds only its own tenant's rows.
    assert_eq!(
        mgr.namespaces().await,
        vec!["acme".to_string(), "globex".to_string()]
    );

    let acme = read_docs(&mgr.engine_for("acme").await.unwrap()).await;
    assert_eq!(acme.len(), 2);
    assert_eq!(acme.get(&1).map(String::as_str), Some("acme-a"));

    let globex = read_docs(&mgr.engine_for("globex").await.unwrap()).await;
    assert_eq!(
        globex.len(),
        1,
        "globex sees only its own row, not acme's id=2"
    );
    assert_eq!(globex.get(&1).map(String::as_str), Some("globex-x"));
}

#[tokio::test]
async fn tenant_set_is_restored_on_a_fresh_manager() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Database::new(Arc::new(Db::open("bluedb", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();

    {
        let mgr = LakehouseManager::open(
            object_store_file_io(store.clone(), ""),
            "lakehouse",
            db.clone(),
            cdc.clone(),
            cfg(),
        )
        .await
        .unwrap();
        mgr.engine_for("acme")
            .await
            .unwrap()
            .enable_table("docs")
            .await
            .unwrap();
        write_docs(&db, &cdc, "acme", &[(1, "a")]).await;
        mgr.seal_all().await.unwrap();
        mgr.shutdown();
    }

    // A fresh manager (the promote path) over the same store restores the tenant
    // index → acme's namespace and mirror are back without any PRAGMA replay.
    let cdc2 = CdcConfig::default();
    let mgr2 = LakehouseManager::open(
        object_store_file_io(store.clone(), ""),
        "lakehouse",
        db.clone(),
        cdc2.clone(),
        cfg(),
    )
    .await
    .unwrap();
    assert_eq!(mgr2.namespaces().await, vec!["acme".to_string()]);
    let eng = mgr2
        .engine_for_namespace("acme")
        .await
        .expect("acme restored");
    assert!(eng.is_mirrored("docs"), "registry restored for the tenant");
    assert_eq!(read_docs(&eng).await.get(&1).map(String::as_str), Some("a"));
}
