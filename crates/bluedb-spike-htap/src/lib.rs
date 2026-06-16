//! THROWAWAY SPIKE — helpers for the HTAP unified-query proof (slice 1).
//!
//! The proof itself lives in `tests/union.rs`. This module holds the fiddly
//! iceberg-rust 0.9.1 plumbing so the test reads as a narrative:
//!   1. [`memory_catalog`] — a `MemoryCatalog` over a temp-dir warehouse.
//!   2. [`hist_schema`] / [`hist_arrow_schema`] — the shared `(id, name, amount,
//!      created)` schema, exercising a `decimal(12,2)` + a `date` for type
//!      fidelity.
//!   3. [`create_and_populate`] — create the Iceberg table in the catalog and
//!      `fast_append` one Parquet data file via a `Transaction`.
//!   4. [`fresh_batch`] — an in-memory Arrow `RecordBatch` of the SAME schema.
//!
//! Key API facts learned from the 0.9.1 sources (crates.io tarballs):
//! - iceberg 0.9.1 ships the memory catalog in-crate: `iceberg::memory::{
//!   MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE}` (NO standalone
//!   `iceberg-catalog-memory` 0.9.1 exists — it's a 0.0.0 placeholder).
//! - The catalog defaults to **in-memory** storage (`MemoryStorageFactory`),
//!   so the warehouse "location" is just a path-prefix string; data files must
//!   be written through the table's own `FileIO` (`table.file_io()`) so they
//!   land in the same store the reader uses.
//! - Write path = `Transaction::new(&table).fast_append().add_data_files(..)
//!   .apply(tx)?` then `tx.commit(&catalog).await` (the `.apply` method is on
//!   the `ApplyTransactionAction` trait — must be imported).
//! - Read side = `iceberg_datafusion::IcebergStaticTableProvider
//!   ::try_new_from_table(table)` — takes a `Table` directly, no catalog ref
//!   needed (the catalog-backed `IcebergTableProvider::try_new` is `pub(crate)`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::file::properties::WriterProperties;

/// `decimal(P, S)` used for the amount column — the type-fidelity probe.
pub const DEC_PRECISION: u32 = 12;
pub const DEC_SCALE: u32 = 2;

/// The shared Iceberg schema for both tiers:
/// `id BIGINT (required, PK), name STRING, amount DECIMAL(12,2), created DATE`.
pub fn hist_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", IcebergType::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(
                3,
                "amount",
                IcebergType::Primitive(PrimitiveType::Decimal {
                    precision: DEC_PRECISION,
                    scale: DEC_SCALE,
                }),
            )
            .into(),
            NestedField::optional(4, "created", IcebergType::Primitive(PrimitiveType::Date)).into(),
        ])
        .build()
        .expect("valid iceberg schema")
}

/// The Arrow projection of [`hist_schema`] (field-ids carried in field metadata).
/// This is the exact schema the Iceberg writer expects for input batches AND
/// the schema the in-memory `fresh` table must match for `UNION ALL` to typecheck.
pub fn hist_arrow_schema() -> ArrowSchemaRef {
    Arc::new(
        iceberg::arrow::schema_to_arrow_schema(&hist_schema()).expect("arrow schema conversion"),
    )
}

/// A `MemoryCatalog` over a fresh temp-dir warehouse (default in-memory storage).
/// `_warehouse` path is just a prefix string under the bundled `MemoryStorageFactory`.
pub async fn memory_catalog(warehouse: &str) -> impl Catalog {
    MemoryCatalogBuilder::default()
        .load(
            "spike",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.to_string(),
            )]),
        )
        .await
        .expect("build memory catalog")
}

/// Create namespace `db` + table `db.hist` in `catalog`, append one Parquet data
/// file holding `rows`, and return the committed [`Table`].
pub async fn create_and_populate(catalog: &dyn Catalog, rows: RecordBatch) -> Table {
    let ns = NamespaceIdent::from_strs(["db"]).unwrap();
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("create namespace");

    let table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("hist".to_string())
                .schema(hist_schema())
                .build(),
        )
        .await
        .expect("create table");

    // Write a Parquet data file through the TABLE's FileIO so it lands in the
    // same (in-memory) store the reader will use.
    let data_file = {
        let parquet = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(hist_schema()),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata().clone()).expect("location generator"),
            DefaultFileNameGenerator::new(
                "data".to_string(),
                Some("0".to_string()),
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .expect("build data-file writer");
        writer.write(rows).await.expect("write batch");
        writer
            .close()
            .await
            .expect("close writer")
            .into_iter()
            .next()
            .expect("one data file")
    };

    // fast_append + commit through the catalog → new table snapshot.
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
    let tx = action.apply(tx).expect("apply fast-append action");
    tx.commit(catalog).await.expect("commit transaction")
}

/// Build a `RecordBatch` matching [`hist_arrow_schema`] from parallel column data.
/// `created` values are days-since-epoch (Arrow `Date32`).
pub fn make_batch(
    ids: Vec<i64>,
    names: Vec<&str>,
    amounts: Vec<i128>,
    created: Vec<i32>,
) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    let amount: ArrayRef = Arc::new(
        Decimal128Array::from(amounts)
            .with_precision_and_scale(DEC_PRECISION as u8, DEC_SCALE as i8)
            .expect("decimal precision/scale"),
    );
    let created: ArrayRef = Arc::new(Date32Array::from(created));
    RecordBatch::try_new(hist_arrow_schema(), vec![id, name, amount, created])
        .expect("record batch matches schema")
}

/// A few "fresh tier" rows (would come from the writer's memtable in prod).
pub fn fresh_batch() -> RecordBatch {
    // amounts are scaled by 10^2 (scale=2): 1234 = 12.34, etc.
    make_batch(
        vec![100, 101, 102],
        vec!["avocado", "banana", "apricot"],
        vec![1234, 5000, 9999],
        vec![20000, 20001, 20002],
    )
}
