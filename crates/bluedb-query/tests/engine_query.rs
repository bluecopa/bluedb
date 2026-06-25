//! Integration tests for [`bluedb_query::query_sql`].
//!
//! Builds a table via the lakehouse engine (with DECIMAL + DATE columns, to
//! exercise the recently-added seal writer type support), seals it, then
//! asserts that DataFusion returns correct results for an arbitrary-column
//! filter + sort query AND an aggregate query.

use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use bluedb_lakehouse::{object_store_file_io, LakehouseEngine};
use bluedb_query::query_sql;
use bluedb_sql::{CdcConfig, Database};
use gluesql_core::prelude::Glue;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

/// Construct an in-memory `Database` + a lakehouse engine over a fresh
/// `InMemory` object store so no temp-dir or file-system I/O is required.
async fn make_engine() -> (Database, CdcConfig, LakehouseEngine) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Database::new(Arc::new(Db::open("bluedb", store.clone()).await.unwrap()));
    let cdc = CdcConfig::default();
    let file_io = object_store_file_io(store.clone(), "");
    let eng = LakehouseEngine::reopen(file_io, "lakehouse", "_", db.clone(), cdc.clone())
        .await
        .unwrap();
    (db, cdc, eng)
}

/// Build and seal a `products` table with:
/// - `id INTEGER PRIMARY KEY`
/// - `name TEXT`
/// - `category TEXT`
/// - `price DECIMAL`
/// - `listed DATE`
///
/// The DECIMAL and DATE columns exercise the type completeness fix.
async fn seed_products(db: &Database, cdc: &CdcConfig, eng: &LakehouseEngine) {
    eng.enable_table("products").await.unwrap();

    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute(
            "CREATE TABLE products (\
                id       INTEGER  PRIMARY KEY,\
                name     TEXT,\
                category TEXT,\
                price    DECIMAL,\
                listed   DATE\
            );",
        )
        .await
        .unwrap();
    }

    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        // 'a%' names: apple, apricot, artichoke; others: banana, cherry.
        // Two categories: fruit (apple, banana, apricot, cherry), veg (artichoke).
        // listed = days-since-epoch: 19000 = 2022-01-18, 19001 = 2022-01-19, etc.
        g.execute(
            "INSERT INTO products VALUES \
                (1, 'apple',     'fruit', 1.50,  DATE '2022-01-18'),\
                (2, 'banana',    'fruit', 0.75,  DATE '2022-01-19'),\
                (3, 'apricot',   'fruit', 2.00,  DATE '2022-01-20'),\
                (4, 'cherry',    'fruit', 3.25,  DATE '2022-01-21'),\
                (5, 'artichoke', 'veg',   4.99,  DATE '2022-01-22');",
        )
        .await
        .unwrap();
    }

    eng.seal().await.unwrap();
}

/// Collect `(id, name)` pairs from a batch slice that has columns in unknown
/// order — look up by name.
fn collect_id_name(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let names = b
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out
}

/// Collect `(category, count)` pairs from a batch slice with columns
/// `category TEXT` and `COUNT(*) BIGINT`.
fn collect_category_counts(batches: &[arrow_array::RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let cats = b
            .column_by_name("category")
            .expect("category column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category is Utf8");
        // DataFusion names COUNT(*) as "count(*)"
        let col_name = b
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().to_lowercase().starts_with("count"))
            .map(|f| f.name().clone())
            .expect("count column");
        let counts = b
            .column_by_name(&col_name)
            .expect("count column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64");
        for i in 0..b.num_rows() {
            out.push((cats.value(i).to_string(), counts.value(i)));
        }
    }
    out.sort();
    out
}

/// [`query_sql`] — filter + sort on non-PK columns over sealed Iceberg table.
///
/// Query shape: `SELECT id, name FROM products WHERE name LIKE 'a%' ORDER BY name`
/// Expected rows: apple(1), apricot(3), artichoke(5) — sorted alphabetically.
#[tokio::test]
async fn filter_and_sort_query_returns_correct_rows() {
    let (db, cdc, eng) = make_engine().await;
    seed_products(&db, &cdc, &eng).await;

    let batches = query_sql(
        &eng,
        "products",
        "SELECT id, name FROM products WHERE name LIKE 'a%' ORDER BY name",
    )
    .await
    .expect("query_sql should succeed");

    let rows = collect_id_name(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "apple".to_string()),
            (3, "apricot".to_string()),
            (5, "artichoke".to_string()),
        ],
        "only 'a%' rows, alphabetically sorted: {rows:?}"
    );
}

/// [`query_sql`] — aggregate (GROUP BY + COUNT) over sealed Iceberg table.
///
/// Query shape: `SELECT category, COUNT(*) FROM products GROUP BY category`
/// Expected: fruit → 4, veg → 1.
#[tokio::test]
async fn aggregate_query_returns_correct_counts() {
    let (db, cdc, eng) = make_engine().await;
    seed_products(&db, &cdc, &eng).await;

    let batches = query_sql(
        &eng,
        "products",
        "SELECT category, COUNT(*) FROM products GROUP BY category",
    )
    .await
    .expect("query_sql should succeed");

    let mut counts = collect_category_counts(&batches);
    counts.sort();
    assert_eq!(
        counts,
        vec![("fruit".to_string(), 4), ("veg".to_string(), 1),],
        "fruit=4, veg=1: {counts:?}"
    );
}

/// [`query_sql`] returns an error when the table has never been sealed.
#[tokio::test]
async fn error_on_unsealed_table() {
    let (db, cdc, eng) = make_engine().await;
    eng.enable_table("empty").await.unwrap();
    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE empty (id INTEGER PRIMARY KEY, x TEXT);")
            .await
            .unwrap();
    }
    // We insert one row but do NOT seal.
    {
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute("INSERT INTO empty VALUES (1, 'x');")
            .await
            .unwrap();
    }

    let result = query_sql(&eng, "empty", "SELECT * FROM empty").await;
    assert!(
        result.is_err(),
        "expected error for unsealed table, got {:?}",
        result
    );
}

/// [`query_sql`] — DECIMAL and DATE columns are accessible and return correct
/// Arrow types (Decimal128 / Date32).
#[tokio::test]
async fn decimal_and_date_columns_are_accessible() {
    use arrow_array::{Date32Array, Decimal128Array};

    let (db, cdc, eng) = make_engine().await;
    seed_products(&db, &cdc, &eng).await;

    // Query by the DECIMAL column and order by DATE — exercises type completeness.
    let batches = query_sql(
        &eng,
        "products",
        "SELECT id, price, listed FROM products ORDER BY id",
    )
    .await
    .expect("query_sql should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "all 5 rows returned");

    // Check the Arrow types of price and listed in the first batch.
    let b = &batches[0];

    let price_col = b.column_by_name("price").expect("price column");
    assert!(
        price_col
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .is_some(),
        "price must be Decimal128, got {:?}",
        price_col.data_type()
    );

    let listed_col = b.column_by_name("listed").expect("listed column");
    assert!(
        listed_col.as_any().downcast_ref::<Date32Array>().is_some(),
        "listed must be Date32, got {:?}",
        listed_col.data_type()
    );
}
