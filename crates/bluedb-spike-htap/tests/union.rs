//! THROWAWAY SPIKE proof (slice 1 of the HTAP unified-query design).
//!
//! Load-bearing claim under test: **DataFusion can query a real Iceberg table
//! (read via `iceberg-datafusion`) UNIONed with an in-memory Arrow table,
//! applying an arbitrary-column filter + ORDER BY, returning correct merged +
//! sorted rows with clean types** — the "historical tier ∪ fresh tier" union the
//! whole architecture rests on.

use std::sync::Arc;

use arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use iceberg_datafusion::IcebergStaticTableProvider;

use bluedb_spike_htap::{
    create_and_populate, fresh_batch, hist_arrow_schema, make_batch, memory_catalog, DEC_PRECISION,
    DEC_SCALE,
};

/// PRIORITY 2: DataFusion reads a real Iceberg table through
/// `iceberg-datafusion`'s static table provider (`SELECT * FROM hist`).
#[tokio::test]
async fn datafusion_reads_iceberg_table() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = memory_catalog(dir.path().to_str().unwrap()).await;

    // Historical rows: two start with 'a' (apple, anchovy), one does not.
    let hist = make_batch(
        vec![1, 2, 3],
        vec!["apple", "cherry", "anchovy"],
        vec![1050, 2075, 333],
        vec![19000, 19001, 19002],
    );
    let table = create_and_populate(&catalog, hist).await;

    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .expect("iceberg static table provider");

    let ctx = SessionContext::new();
    ctx.register_table("hist", Arc::new(provider)).unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM hist ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "apple".to_string()),
            (2, "cherry".to_string()),
            (3, "anchovy".to_string()),
        ],
        "DataFusion must read all three Iceberg rows via iceberg-datafusion"
    );
}

/// PRIORITY 3 (the real claim): Iceberg(`hist`) UNION ALL Arrow-MemTable(`fresh`),
/// filtered on a NON-id column (`name LIKE 'a%'`) and ORDER BY a non-id column
/// (`name`). Asserts the merged set is correct AND sorted.
#[tokio::test]
async fn union_iceberg_and_fresh_with_filter_and_order() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = memory_catalog(dir.path().to_str().unwrap()).await;

    // hist: apple (a), cherry, anchovy (a)
    let hist = make_batch(
        vec![1, 2, 3],
        vec!["apple", "cherry", "anchovy"],
        vec![1050, 2075, 333],
        vec![19000, 19001, 19002],
    );
    let table = create_and_populate(&catalog, hist).await;
    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .expect("iceberg static table provider");

    let ctx = SessionContext::new();
    ctx.register_table("hist", Arc::new(provider)).unwrap();

    // fresh: avocado (a), banana, apricot (a)  — see fresh_batch()
    let mem = MemTable::try_new(hist_arrow_schema(), vec![vec![fresh_batch()]]).unwrap();
    ctx.register_table("fresh", Arc::new(mem)).unwrap();

    // The guardrail-rejected shape: arbitrary-column filter + sort over the union.
    let sql = "SELECT id, name FROM \
               (SELECT * FROM hist UNION ALL SELECT * FROM fresh) \
               WHERE name LIKE 'a%' ORDER BY name";
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    let rows = collect_id_name(&batches);

    // 'a%' matches: apple, anchovy (hist) + avocado, apricot (fresh) = 4 rows.
    // cherry + banana excluded. Sorted by name ascending.
    let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(
        names,
        vec!["anchovy", "apple", "apricot", "avocado"],
        "merged result must be the 4 'a%' rows from BOTH tiers, sorted by name"
    );
    // Spot-check the id↔name pairing survived the union (no column smear).
    assert!(rows.contains(&(3, "anchovy".to_string())));
    assert!(rows.contains(&(1, "apple".to_string())));
    assert!(rows.contains(&(102, "apricot".to_string())));
    assert!(rows.contains(&(100, "avocado".to_string())));
}

/// PRIORITY 3 fidelity tail: the `decimal(12,2)` and `date` columns round-trip
/// through iceberg-datafusion → DataFusion with the RIGHT Arrow types and values
/// (not garbled / not silently widened).
#[tokio::test]
async fn decimal_and_date_round_trip_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = memory_catalog(dir.path().to_str().unwrap()).await;

    let hist = make_batch(
        vec![1, 2, 3],
        vec!["apple", "cherry", "anchovy"],
        vec![1050, 2075, 333], // 10.50, 20.75, 3.33
        vec![19000, 19001, 19002],
    );
    let table = create_and_populate(&catalog, hist).await;
    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .expect("iceberg static table provider");

    let ctx = SessionContext::new();
    ctx.register_table("hist", Arc::new(provider)).unwrap();

    let batches = ctx
        .sql("SELECT id, amount, created FROM hist ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let b = &batches[0];

    // --- Type fidelity: schema came back as Decimal128(12,2) + Date32 ---
    let amount_field = b.schema().field_with_name("amount").unwrap().clone();
    assert_eq!(
        amount_field.data_type(),
        &DataType::Decimal128(DEC_PRECISION as u8, DEC_SCALE as i8),
        "amount must round-trip as Decimal128(12,2), not garbled/widened"
    );
    let created_field = b.schema().field_with_name("created").unwrap().clone();
    assert_eq!(
        created_field.data_type(),
        &DataType::Date32,
        "created must round-trip as Date32"
    );

    // --- Value fidelity ---
    let ids = b
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let amounts = b
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let created = b
        .column_by_name("created")
        .unwrap()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();

    assert_eq!(ids.value(0), 1);
    assert_eq!(amounts.value(0), 1050); // raw i128 at scale 2 => 10.50
    assert_eq!(amounts.value(1), 2075);
    assert_eq!(created.value(0), 19000);
}

/// Collect `(id, name)` rows from a batch slice carrying Int64 `id` + Utf8 `name`.
fn collect_id_name(batches: &[arrow::array::RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out
}
