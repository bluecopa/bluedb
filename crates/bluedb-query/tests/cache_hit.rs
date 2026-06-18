//! Integration test: analytical queries (query_sql) are served from the foyer
//! cache on the second call — CacheStats::hits() increases, proving the inner
//! object store was NOT called again.
//!
//! Architecture under test:
//!
//! ```text
//!   query_sql (DataFusion → IcebergStaticTableProvider)
//!        │  get / get_range
//!        ▼
//!   CachingObjectStore  ─── foyer HybridCache (DRAM)
//!        │ miss only
//!        ▼
//!   InMemory object store (inner; bytes stay here after the first query)
//! ```
//!
//! Proof of caching: CacheStats::misses() > 0 after the first query (cold),
//! CacheStats::hits() > 0 after the second query (warm).

use std::sync::Arc;

use bluedb_cache::CachingObjectStoreBuilder;
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::query_sql;
use bluedb_sql::{CdcConfig, Database};
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/// Build `(Database, CdcConfig, LakehouseEngine)` where the engine's file I/O
/// is backed by a `CachingObjectStore(InMemory)`.
///
/// Returns `(db, cdc, engine, cache_stats)`.
async fn make_cached_engine() -> (
    Database,
    CdcConfig,
    LakehouseEngine,
    Arc<bluedb_cache::CacheStats>,
) {
    // SlateDB gets a plain InMemory (it has its own block cache).
    let slatedb_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // The lakehouse/Iceberg path gets a CachingObjectStore wrapping its own
    // InMemory.  This means sealed Parquet files land here and are cached.
    let inner_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let caching = CachingObjectStoreBuilder::new(inner_store)
        .dram_bytes(16 * 1024 * 1024) // 16 MiB — plenty for test Parquet files
        .build()
        .await
        .expect("build CachingObjectStore");
    let stats = caching.stats();
    let caching_arc: Arc<dyn ObjectStore> = Arc::new(caching);

    let db = Database::new(Arc::new(
        Db::open("bluedb", slatedb_store).await.unwrap(),
    ));
    let cdc = CdcConfig::default();

    // LakehouseEngine uses the CachingObjectStore for all Iceberg I/O.
    let file_io = object_store_file_io(caching_arc, "");
    let eng = LakehouseEngine::reopen(file_io, "lakehouse", "_", db.clone(), cdc.clone())
        .await
        .unwrap();

    (db, cdc, eng, stats)
}

/// Seed a tiny `items` table and seal it so a snapshot exists for query_sql.
async fn seed_items(db: &Database, cdc: &CdcConfig, eng: &LakehouseEngine) {
    eng.enable_table("items").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute(
            "CREATE TABLE items (\
                id   INTEGER PRIMARY KEY,\
                name TEXT\
            );",
        )
        .await
        .unwrap();
    }

    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute(
            "INSERT INTO items VALUES \
                (1, 'alpha'),\
                (2, 'beta'),\
                (3, 'gamma');",
        )
        .await
        .unwrap();
    }

    eng.seal().await.unwrap();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Run the same `query_sql` twice against a sealed Iceberg snapshot.
///
/// After the FIRST call the cache will have seen misses (cold path: every
/// Parquet/metadata byte fetched from the inner InMemory and inserted into
/// foyer).  After the SECOND call the cache must record hits (warm path: every
/// read served from foyer, inner store not called again).
#[tokio::test]
async fn repeated_analytical_query_served_from_cache() {
    let (db, cdc, eng, stats) = make_cached_engine().await;
    seed_items(&db, &cdc, &eng).await;

    // --- first query: cold (cache misses for every Iceberg/Parquet byte) -----
    let batches1 = query_sql(&eng, "items", "SELECT id, name FROM items ORDER BY id")
        .await
        .expect("first query_sql should succeed");
    let total_rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows1, 3, "first query: all 3 rows returned");

    let misses_after_first = stats.misses();
    let hits_after_first = stats.hits();
    // Sanity: cold query must have caused at least one cache miss.
    assert!(
        misses_after_first > 0,
        "expected cache misses > 0 on the first (cold) query; got 0 \
         (CachingObjectStore may not be wired into the lakehouse)"
    );

    // --- second query: warm (all bytes already in foyer) ---------------------
    let batches2 = query_sql(&eng, "items", "SELECT id, name FROM items ORDER BY id")
        .await
        .expect("second query_sql should succeed");
    let total_rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows2, 3, "second query: all 3 rows returned");

    let hits_after_second = stats.hits();
    let misses_after_second = stats.misses();

    // Hits must have increased (foyer served at least one read).
    assert!(
        hits_after_second > hits_after_first,
        "expected cache hits to increase on the second (warm) query; \
         hits_before={hits_after_first}, hits_after={hits_after_second}"
    );

    // Misses must NOT have increased (no new cold fetches to the inner store).
    assert_eq!(
        misses_after_second, misses_after_first,
        "cache misses should not increase on the second (warm) query; \
         the inner store was called again (cache is not serving the read)"
    );
}
