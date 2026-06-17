//! [`LakehouseWriter`] — writes gluesql rows into an Iceberg table and
//! **self-authors** the snapshot commit (spec §5.2).
//!
//! We host our own catalog: a table is a directory in object storage with
//! `metadata/v{N}.metadata.json` files and a `metadata/version-hint.text`
//! pointing at the current version. Because we own that pointer we apply the
//! `AddSnapshot`/`SetSnapshotRef` table updates to
//! [`TableMetadata::into_builder`] ourselves and write the next `metadata.json`
//! — we never touch iceberg-rust's non-constructible `TableCommit` /
//! `Catalog::update_table`. The commit pipeline mirrors iceberg-rust's own
//! `fast_append` (write data file → data manifest → manifest list → snapshot →
//! metadata), extended with equality-delete manifests for full CRUD.
//!
//! Full-CRUD merge-on-read works because each seal commit is its own snapshot
//! with a strictly larger sequence number: an equality-delete on a primary key,
//! committed in snapshot *N*, removes that key's rows from all prior snapshots
//! (sequence `< N`) but not the new row written in *N* itself. Upsert and delete
//! both fall out of that.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, TimeUnit};
use bytes::Bytes;
use chrono::Timelike;
use gluesql_core::data::{Key, Value};
use gluesql_core::store::DataRow;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestWriterBuilder, NullOrder, Operation, Schema as IcebergSchema,
    SchemaRef, Snapshot, SnapshotReference, SnapshotRetention, SortDirection, SortField, SortOrder,
    Summary, TableMetadata, Transform, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;

use crate::{LakehouseError, Result};

/// Snapshot summary property carrying the CDC watermark the snapshot durably
/// reflects — the highest CDC sequence published into it. The seal loop reads it
/// back via [`LakehouseWriter::current_watermark`] to resume exactly-once.
pub const WATERMARK_PROP: &str = "bluedb.cdc_watermark";

/// Writes one Iceberg table for the lakehouse mirror, owning its metadata
/// pointer (self-hosted catalog). Built/loaded via [`LakehouseWriter::open`].
pub struct LakehouseWriter {
    file_io: FileIO,
    /// `<root>/<namespace>/<table>` — the table's directory in object storage.
    table_root: String,
    table_ident: TableIdent,
    /// The Iceberg table schema (field-ids align with bluedb-sql field-ids).
    schema: SchemaRef,
    /// Field-id of the primary key, used as the equality-delete identifier.
    pk_field_id: i32,
    /// Current in-memory table metadata (the authority between commits).
    metadata: TableMetadata,
    /// Current metadata version `N` (file `metadata/v{N}.metadata.json`).
    version: u64,
    /// Data files written since the last commit.
    pending_data: Vec<DataFile>,
    /// Equality-delete files written since the last commit.
    pending_deletes: Vec<DataFile>,
}

impl LakehouseWriter {
    /// A local-filesystem-backed writer (tests + single-node dev). `root` is an
    /// absolute path; object-store-backed construction lands in Phase 5.
    pub async fn open_local(
        root: &str,
        namespace: &str,
        table: &str,
        schema: IcebergSchema,
        pk_field_id: i32,
    ) -> Result<Self> {
        let file_io = FileIO::new_with_fs();
        Self::open(file_io, root, namespace, table, schema, pk_field_id, &[pk_field_id]).await
    }

    /// Open the table at `<root>/<namespace>/<table>`: load it from
    /// `version-hint.text` if present, else create it (writing `v0.metadata.json`).
    ///
    /// On creation the table declares an Iceberg **sort order** on
    /// `sort_field_ids` (ascending). The seal writes rows in primary-key order
    /// (the `BTreeMap` collapse), so the data genuinely is sorted by these
    /// columns — for a composite key, the user component columns; otherwise the
    /// single PK. This lets warehouses prune files on those columns. An empty
    /// slice declares no sort order.
    pub async fn open(
        file_io: FileIO,
        root: &str,
        namespace: &str,
        table: &str,
        schema: IcebergSchema,
        pk_field_id: i32,
        sort_field_ids: &[i32],
    ) -> Result<Self> {
        let table_root = format!("{root}/{namespace}/{table}");
        let table_ident = TableIdent::from_strs([namespace, table])?;
        let hint_path = format!("{table_root}/metadata/version-hint.text");

        if file_io.exists(&hint_path).await? {
            let raw = file_io.new_input(&hint_path)?.read().await?;
            let version: u64 = String::from_utf8_lossy(&raw)
                .trim()
                .parse()
                .map_err(|e| LakehouseError::Iceberg(format!("bad version-hint: {e}")))?;
            let md_path = format!("{table_root}/metadata/v{version}.metadata.json");
            let bytes = file_io.new_input(&md_path)?.read().await?;
            let metadata: TableMetadata = serde_json::from_slice(&bytes)?;
            let mut writer = Self {
                file_io,
                table_root,
                table_ident,
                schema: metadata.current_schema().clone(),
                pk_field_id,
                metadata,
                version,
                pending_data: Vec::new(),
                pending_deletes: Vec::new(),
            };
            // Reconcile any ALTER since the last seal into the Iceberg schema,
            // self-authoring a schema-only metadata commit. No-op when unchanged
            // (the common path), so an untouched table pays only a comparison.
            writer.evolve_schema_to(&schema).await?;
            Ok(writer)
        } else {
            let creation = TableCreation::builder()
                .name(table.to_string())
                .location(table_root.clone())
                .schema(schema)
                .sort_order(sort_order_on(sort_field_ids)?)
                .build();
            let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)?
                .build()?
                .metadata;
            let schema = metadata.current_schema().clone();
            let writer = Self {
                file_io,
                table_root,
                table_ident,
                schema,
                pk_field_id,
                metadata,
                version: 0,
                pending_data: Vec::new(),
                pending_deletes: Vec::new(),
            };
            writer.write_metadata(0).await?;
            Ok(writer)
        }
    }

    /// If `desired` differs from the current Iceberg schema, evolve to it by
    /// self-authoring a schema-only metadata version (`add_current_schema`) and
    /// adopt the evolved schema for subsequent writes. No-op when equal — the
    /// common (never-altered) path, so an unchanged table pays only a comparison.
    ///
    /// The evolved schema's field order is the current logical column order, so
    /// the positional record-batch construction in [`Self::rows_to_record_batch`]
    /// stays correct after the ALTER.
    async fn evolve_schema_to(&mut self, desired: &IcebergSchema) -> Result<()> {
        let Some(evolved) = reconcile_schema(self.metadata.current_schema(), desired)? else {
            return Ok(());
        };
        let current_md_loc = format!("{}/v{}.metadata.json", self.metadata_dir(), self.version);
        let result = self
            .metadata
            .clone()
            .into_builder(Some(current_md_loc))
            .add_current_schema(evolved)?
            .build()?;
        self.metadata = result.metadata;
        self.version += 1;
        self.schema = self.metadata.current_schema().clone();
        self.write_metadata(self.version).await?;
        Ok(())
    }

    /// Stage an upsert of `rows` (insert or new version of an existing key).
    /// The data file is written immediately; equality-deletes that retire the
    /// prior versions of these keys are staged too (so the merge-on-read result
    /// is last-writer-wins after the next [`Self::commit_snapshot`]).
    pub async fn upsert(&mut self, rows: &[(Key, DataRow)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = self.rows_to_record_batch(rows)?;
        let data_file = self.write_data_file(batch).await?;
        self.pending_data.push(data_file);

        let keys: Vec<&Key> = rows.iter().map(|(k, _)| k).collect();
        self.stage_deletes(&keys).await?;
        Ok(())
    }

    /// Stage deletion of `keys` via equality-deletes on the primary key.
    pub async fn delete(&mut self, keys: &[Key]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let refs: Vec<&Key> = keys.iter().collect();
        self.stage_deletes(&refs).await?;
        Ok(())
    }

    /// The watermark the current snapshot durably reflects, if any.
    pub fn current_watermark(&self) -> Option<i64> {
        self.metadata
            .current_snapshot()?
            .summary()
            .additional_properties
            .get(WATERMARK_PROP)
            .and_then(|v| v.parse().ok())
    }

    /// Build an iceberg [`Table`] view over the current metadata, for reading
    /// the mirror back (used by the seal loop's verification and tests).
    pub fn to_table(&self) -> Result<Table> {
        Ok(Table::builder()
            .identifier(self.table_ident.clone())
            .file_io(self.file_io.clone())
            .metadata(self.metadata.clone())
            .build()?)
    }

    /// The Arrow schema of this table (Iceberg field-ids carried via
    /// [`schema_to_arrow_schema`]), shared by the seal path's row conversion
    /// ([`Self::rows_to_record_batch`]) and the Iceberg read-back so merged
    /// batches from both sides align for concatenation.
    pub fn arrow_schema(&self) -> Result<arrow_schema::SchemaRef> {
        Ok(Arc::new(schema_to_arrow_schema(&self.schema)?))
    }

    // --- internals ----------------------------------------------------------

    fn metadata_dir(&self) -> String {
        format!("{}/metadata", self.table_root)
    }

    /// Write `metadata/v{version}.metadata.json` and point `version-hint.text` at
    /// it. The hint write is the atomic publish (single-key PUT); bluedb has one
    /// active writer, so there is no concurrent-writer race on the pointer.
    async fn write_metadata(&self, version: u64) -> Result<()> {
        let md_path = format!("{}/v{version}.metadata.json", self.metadata_dir());
        let bytes = serde_json::to_vec(&self.metadata)?;
        self.file_io
            .new_output(&md_path)?
            .write(Bytes::from(bytes))
            .await?;
        let hint_path = format!("{}/version-hint.text", self.metadata_dir());
        self.file_io
            .new_output(&hint_path)?
            .write(Bytes::from(version.to_string().into_bytes()))
            .await?;
        Ok(())
    }

    /// Convert gluesql rows to an Arrow [`RecordBatch`] matching the table's
    /// Arrow schema (field-ids carried via [`schema_to_arrow_schema`]).
    ///
    /// Public so the fresh writer-local analytical read (`bluedb-query`) can
    /// reuse the exact gluesql-`Value`→Arrow conversion the seal path uses,
    /// guaranteeing identical typing/rendering across the OLTP and analytical
    /// tiers. (`LakehouseEngine::current_record_batch` is the caller.)
    pub fn rows_to_record_batch(&self, rows: &[(Key, DataRow)]) -> Result<RecordBatch> {
        let arrow_schema = Arc::new(schema_to_arrow_schema(&self.schema)?);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());
        for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
            let cells: Vec<&Value> = rows
                .iter()
                .map(|(_, row)| match row {
                    DataRow::Vec(values) => values.get(col_idx).unwrap_or(&Value::Null),
                    DataRow::Map(_) => &Value::Null,
                })
                .collect();
            columns.push(build_arrow_column(field.data_type(), &cells)?);
        }
        RecordBatch::try_new(arrow_schema, columns)
            .map_err(|e| LakehouseError::Iceberg(format!("record batch: {e}")))
    }

    /// A fresh rolling data-file writer (caps each output file at the target
    /// size). Used both for a single upsert batch and for streaming compaction.
    async fn new_data_file_writer(&self) -> Result<impl IcebergWriter> {
        let parquet =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), self.schema.clone());
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            self.file_io.clone(),
            DefaultLocationGenerator::with_data_location(format!("{}/data", self.table_root)),
            DefaultFileNameGenerator::new(
                "data".to_string(),
                Some(self.unique_suffix()),
                DataFileFormat::Parquet,
            ),
        );
        Ok(DataFileWriterBuilder::new(rolling).build(None).await?)
    }

    /// Write one Parquet data file from `batch`, returning its [`DataFile`].
    async fn write_data_file(&self, batch: RecordBatch) -> Result<DataFile> {
        let mut writer = self.new_data_file_writer().await?;
        writer.write(batch).await?;
        let files = writer.close().await?;
        files
            .into_iter()
            .next()
            .ok_or_else(|| LakehouseError::Iceberg("data writer produced no file".into()))
    }

    /// Write one Parquet equality-delete file holding the primary keys in
    /// `keys`, appended to `pending_deletes` (content = equality-deletes).
    async fn stage_deletes(&mut self, keys: &[&Key]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // The equality-delete writer projects the input (full-schema) batch down
        // to the identifier field, so its inner Parquet writer must carry the
        // *projected* schema (just the PK), not the full table schema.
        let batch = self.keys_to_delete_batch(keys)?;
        let config = EqualityDeleteWriterConfig::new(vec![self.pk_field_id], self.schema.clone())?;
        let parquet =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), self.projected_pk_schema()?);
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            self.file_io.clone(),
            DefaultLocationGenerator::with_data_location(format!("{}/data", self.table_root)),
            DefaultFileNameGenerator::new(
                "eq-del".to_string(),
                Some(self.unique_suffix()),
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
            .build(None)
            .await?;
        writer.write(batch).await?;
        self.pending_deletes.extend(writer.close().await?);
        Ok(())
    }

    /// An Iceberg schema containing only the primary-key field, for the
    /// equality-delete file's Parquet writer.
    fn projected_pk_schema(&self) -> Result<SchemaRef> {
        let pk_field = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.id == self.pk_field_id)
            .ok_or_else(|| LakehouseError::Schema("pk field not in schema".into()))?
            .clone();
        Ok(Arc::new(
            IcebergSchema::builder()
                .with_schema_id(self.schema.schema_id())
                .with_identifier_field_ids(vec![self.pk_field_id])
                .with_fields(vec![pk_field])
                .build()?,
        ))
    }

    /// Build a full-table-schema RecordBatch carrying the primary keys (other
    /// columns null). The equality-delete writer projects it down to the PK.
    ///
    /// Non-PK fields are forced **nullable** in this batch's Arrow schema: they
    /// are filled with NULL placeholders (the writer projects them away), so a
    /// `NOT NULL` non-PK column — e.g. a composite-key component — would
    /// otherwise fail `RecordBatch` validation even though those values are
    /// discarded.
    pub fn keys_to_delete_batch(&self, keys: &[&Key]) -> Result<RecordBatch> {
        let full = schema_to_arrow_schema(&self.schema)?;
        let pk_values: Vec<Value> = keys.iter().map(|k| key_to_value(k)).collect();
        let mut fields = Vec::with_capacity(full.fields().len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(full.fields().len());
        for field in full.fields() {
            let is_pk = field
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .and_then(|v| v.parse::<i32>().ok())
                == Some(self.pk_field_id);
            let cells: Vec<&Value> = if is_pk {
                pk_values.iter().collect()
            } else {
                vec![&Value::Null; keys.len()]
            };
            // Keep the PK field as-is; relax every other field to nullable.
            fields.push(if is_pk {
                field.clone()
            } else {
                Arc::new(field.as_ref().clone().with_nullable(true))
            });
            columns.push(build_arrow_column(field.data_type(), &cells)?);
        }
        let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            full.metadata().clone(),
        ));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| LakehouseError::Iceberg(format!("delete batch: {e}")))
    }

    /// Self-author a snapshot publishing all pending data/delete files, tagging
    /// it with `watermark`. No-op when nothing is pending. The new snapshot
    /// carries forward the parent's manifests (incremental merge-on-read).
    pub async fn commit_snapshot(&mut self, watermark: i64) -> Result<()> {
        if self.pending_data.is_empty() && self.pending_deletes.is_empty() {
            return Ok(());
        }
        self.commit_internal(watermark, false).await
    }

    /// Number of **live data files** in the current table state (across the
    /// current snapshot's manifests). Used to decide whether compaction is worth
    /// running and to assert it reduced file count.
    pub async fn data_file_count(&self) -> Result<usize> {
        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(0);
        };
        let metadata_ref = Arc::new(self.metadata.clone());
        let list = snapshot.load_manifest_list(&self.file_io, &metadata_ref).await?;
        let mut count = 0;
        for mf in list.entries() {
            let manifest = mf.load_manifest(&self.file_io).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Scan a transient snapshot that references only `manifests` — a hand-picked
    /// subset of the table's live manifest files (e.g. a compaction cohort's data
    /// manifests plus the delete manifests) — returning the merged-on-read rows.
    ///
    /// This reuses iceberg-rust's own reader, so equality deletes are applied
    /// (sequence-aware, from the carried-forward manifest entries) to just the
    /// referenced data files. The only durable side effect is a throwaway
    /// manifest-list `.avro`; the real table metadata is untouched. It is the
    /// building block for incremental compaction: rewrite a cohort's *current*
    /// rows without scanning (or disturbing) the rest of the table.
    async fn scan_manifest_subset(
        &self,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<RecordBatch>> {
        use futures::TryStreamExt;

        let snapshot_id = fresh_snapshot_id(&self.metadata);
        let next_seq = self.metadata.next_sequence_number();
        let parent_id = self.metadata.current_snapshot_id();

        let manifest_list_path = format!("{}/scoped-{snapshot_id}.avro", self.metadata_dir());
        let mut mlw = ManifestListWriter::v2(
            self.file_io.new_output(&manifest_list_path)?,
            snapshot_id,
            parent_id,
            next_seq,
        );
        mlw.add_manifests(manifests.into_iter())?;
        mlw.close().await?;

        let summary = Summary {
            operation: Operation::Replace,
            additional_properties: std::collections::HashMap::new(),
        };
        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_id)
            .with_sequence_number(next_seq)
            .with_summary(summary)
            .with_schema_id(self.metadata.current_schema_id())
            .with_timestamp_ms(now_ms())
            .build();

        let current_md_loc = format!("{}/v{}.metadata.json", self.metadata_dir(), self.version);
        let result = self
            .metadata
            .clone()
            .into_builder(Some(current_md_loc))
            .add_snapshot(snapshot)?
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )?
            .build()?;
        let table = Table::builder()
            .identifier(self.table_ident.clone())
            .file_io(self.file_io.clone())
            .metadata(result.metadata)
            .build()?;
        let batches = table
            .scan()
            .build()?
            .to_arrow()
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(batches)
    }

    /// The current snapshot's live manifest files, split into (data, delete) by
    /// [`ManifestContentType`]. Empty when the table has no snapshot yet.
    async fn live_manifest_files(&self) -> Result<(Vec<ManifestFile>, Vec<ManifestFile>)> {
        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok((Vec::new(), Vec::new()));
        };
        let metadata_ref = Arc::new(self.metadata.clone());
        let list = snapshot.load_manifest_list(&self.file_io, &metadata_ref).await?;
        let mut data = Vec::new();
        let mut deletes = Vec::new();
        for mf in list.entries() {
            match mf.content {
                ManifestContentType::Data => data.push(mf.clone()),
                ManifestContentType::Deletes => deletes.push(mf.clone()),
            }
        }
        Ok((data, deletes))
    }

    /// **Compaction** (spec §5.1): re-materialize the table's merged-on-read
    /// state into fresh, larger data files and publish a `Replace` snapshot that
    /// references only those files (old data + equality-delete files become
    /// unreferenced and GC-eligible).
    ///
    /// Memory-bounded **independent of table size**: rows stream out of
    /// iceberg-rust's own reader (which applies equality deletes) one
    /// [`RecordBatch`] at a time, and the rolling writer caps each output file —
    /// so peak memory ≈ one batch + one target-sized output file. (v1 rewrites
    /// the whole table; incremental bin-packing is a future optimization.)
    pub async fn compact(&mut self, watermark: i64) -> Result<()> {
        use futures::TryStreamExt;

        // Nothing to gain only when there is at most one data file AND no delete
        // files: a lone data file with delete files still needs a rewrite to
        // apply (and reclaim) those deletes.
        if self.data_file_count().await? <= 1 && self.delete_file_count().await? == 0 {
            return Ok(());
        }

        // Stream the current merged state and re-write it through one rolling
        // writer (so output rolls into target-sized files automatically).
        let table = self.to_table()?;
        let mut stream = table.scan().build()?.to_arrow().await?;
        let mut rolling = self.new_data_file_writer().await?;
        let mut wrote_any = false;
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            rolling.write(batch).await?;
            wrote_any = true;
        }
        let new_files = rolling.close().await?;
        if !wrote_any {
            return Ok(());
        }

        self.pending_data = new_files;
        self.pending_deletes.clear();
        self.commit_internal(watermark, true).await
    }

    /// Number of live equality-delete files in the current table state. Drives
    /// the worker's choice of major (reclaim deletes) vs minor compaction.
    pub async fn delete_file_count(&self) -> Result<usize> {
        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(0);
        };
        let metadata_ref = Arc::new(self.metadata.clone());
        let list = snapshot.load_manifest_list(&self.file_io, &metadata_ref).await?;
        let mut count = 0;
        for mf in list.entries() {
            let manifest = mf.load_manifest(&self.file_io).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::EqualityDeletes {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// **Incremental (minor) compaction**: bin-pack the table's small data files
    /// into fewer, larger files without rewriting the whole table.
    ///
    /// Selects the per-seal data manifests whose live data bytes are below
    /// `target_bytes` (the bin-pack candidates; already-large files are left
    /// alone), scans just those — with all delete files applied, via
    /// [`Self::scan_manifest_subset`] — and commits an `Overwrite` that carries
    /// the survivor data manifests and **all** delete manifests forward intact
    /// (preserving their sequence numbers) while dropping the compacted cohort.
    /// The rewritten rows take the new (highest) sequence with deletes already
    /// materialized, so merge-on-read is preserved. No-op for <2 candidates.
    ///
    /// Delete files are *kept* (a delete may still target a survivor); the
    /// whole-table [`Self::compact`] reclaims them.
    pub async fn compact_incremental(&mut self, target_bytes: u64, watermark: i64) -> Result<()> {
        let (data_manifests, delete_manifests) = self.live_manifest_files().await?;

        // Partition data manifests into the small-file cohort and the survivors.
        let mut cohort = Vec::new();
        let mut survivors = Vec::new();
        for mf in data_manifests {
            if self.manifest_data_bytes(&mf).await? < target_bytes {
                cohort.push(mf);
            } else {
                survivors.push(mf);
            }
        }
        if cohort.len() < 2 {
            return Ok(()); // nothing worth merging
        }

        // Read the cohort's current rows (deletes applied) and re-write them.
        let mut scoped = cohort.clone();
        scoped.extend(delete_manifests.iter().cloned());
        let batches = self.scan_manifest_subset(scoped).await?;
        let mut rolling = self.new_data_file_writer().await?;
        let mut wrote_any = false;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            rolling.write(batch).await?;
            wrote_any = true;
        }
        let new_files = rolling.close().await?;

        // Keep survivors + all delete manifests; drop the cohort data manifests.
        let mut keep = survivors;
        keep.extend(delete_manifests);
        let snapshot_id = fresh_snapshot_id(&self.metadata);

        if !wrote_any {
            // The cohort merged to nothing (every row deleted): just drop it.
            return self
                .publish_snapshot(snapshot_id, keep, Operation::Replace, watermark)
                .await;
        }

        // Write a new data manifest for the compacted files, then publish.
        let schema = self.metadata.current_schema().clone();
        let partition_spec = self.metadata.default_partition_spec().as_ref().clone();
        let path = format!("{}/{snapshot_id}-data.avro", self.metadata_dir());
        let mut mw = ManifestWriterBuilder::new(
            self.file_io.new_output(&path)?,
            Some(snapshot_id),
            None,
            schema,
            partition_spec,
        )
        .build_v2_data();
        for df in new_files {
            mw.add_file(df, -1)?;
        }
        let mut manifests = keep;
        manifests.push(mw.write_manifest_file().await?);
        self.publish_snapshot(snapshot_id, manifests, Operation::Replace, watermark)
            .await
    }

    /// Total live data bytes referenced by one data manifest (sum of its alive
    /// data files' sizes) — the bin-pack candidacy measure.
    async fn manifest_data_bytes(&self, mf: &ManifestFile) -> Result<u64> {
        let manifest = mf.load_manifest(&self.file_io).await?;
        let mut bytes = 0;
        for entry in manifest.entries() {
            if entry.is_alive() && entry.content_type() == DataContentType::Data {
                bytes += entry.data_file().file_size_in_bytes();
            }
        }
        Ok(bytes)
    }

    /// Author one snapshot from the pending data/delete files. When `replace`,
    /// the manifest list contains ONLY the new files (a compaction `Replace`);
    /// otherwise it carries the parent's manifests forward (incremental
    /// append/overwrite). Applies the table updates to our hosted metadata and
    /// publishes the next `metadata.json`.
    async fn commit_internal(&mut self, watermark: i64, replace: bool) -> Result<()> {
        let snapshot_id = fresh_snapshot_id(&self.metadata);
        let schema = self.metadata.current_schema().clone();
        let partition_spec = self.metadata.default_partition_spec().as_ref().clone();

        let mut manifests: Vec<ManifestFile> = Vec::new();
        // Incremental commits carry forward the parent's manifests; a Replace
        // (compaction) starts fresh, dropping the compacted-away files.
        if !replace {
            if let Some(parent) = self.metadata.current_snapshot() {
                let metadata_ref = Arc::new(self.metadata.clone());
                let list = parent
                    .load_manifest_list(&self.file_io, &metadata_ref)
                    .await?;
                manifests.extend(list.entries().iter().cloned());
            }
        }

        if !self.pending_data.is_empty() {
            let data_files = std::mem::take(&mut self.pending_data);
            let path = format!("{}/{snapshot_id}-data.avro", self.metadata_dir());
            let mut mw = ManifestWriterBuilder::new(
                self.file_io.new_output(&path)?,
                Some(snapshot_id),
                None,
                schema.clone(),
                partition_spec.clone(),
            )
            .build_v2_data();
            for df in data_files {
                // -1 → inherited sequence number: the file takes this snapshot's
                // sequence number at commit.
                mw.add_file(df, -1)?;
            }
            manifests.push(mw.write_manifest_file().await?);
        }

        if !self.pending_deletes.is_empty() {
            let delete_files = std::mem::take(&mut self.pending_deletes);
            let path = format!("{}/{snapshot_id}-deletes.avro", self.metadata_dir());
            let mut mw = ManifestWriterBuilder::new(
                self.file_io.new_output(&path)?,
                Some(snapshot_id),
                None,
                schema.clone(),
                partition_spec.clone(),
            )
            .build_v2_deletes();
            for df in delete_files {
                debug_assert_eq!(df.content_type(), DataContentType::EqualityDeletes);
                // Equality-delete files are ADDED entries in a deletes manifest;
                // -1 → inherited sequence number (this snapshot's), so the delete
                // retires prior versions of the key but not same-commit rows.
                mw.add_file(df, -1)?;
            }
            manifests.push(mw.write_manifest_file().await?);
        }

        let operation = if replace {
            Operation::Replace
        } else if self.metadata.current_snapshot().is_none() {
            Operation::Append
        } else {
            Operation::Overwrite
        };
        self.publish_snapshot(snapshot_id, manifests, operation, watermark)
            .await
    }

    /// Author one snapshot from an explicit, already-built set of `manifests`
    /// (carried-forward + freshly-written manifest files), tag it `operation` +
    /// `watermark`, apply the table updates to our hosted metadata, and publish
    /// the next `metadata.json`. Shared by [`Self::commit_internal`] and the
    /// incremental-compaction commit so both author snapshots identically.
    async fn publish_snapshot(
        &mut self,
        snapshot_id: i64,
        manifests: Vec<ManifestFile>,
        operation: Operation,
        watermark: i64,
    ) -> Result<()> {
        let next_seq = self.metadata.next_sequence_number();
        let parent_id = self.metadata.current_snapshot_id();

        let manifest_list_path = format!("{}/snap-{snapshot_id}.avro", self.metadata_dir());
        let mut mlw = ManifestListWriter::v2(
            self.file_io.new_output(&manifest_list_path)?,
            snapshot_id,
            parent_id,
            next_seq,
        );
        mlw.add_manifests(manifests.into_iter())?;
        mlw.close().await?;

        let summary = Summary {
            operation,
            additional_properties: std::collections::HashMap::from([(
                WATERMARK_PROP.to_string(),
                watermark.to_string(),
            )]),
        };
        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_id)
            .with_sequence_number(next_seq)
            .with_summary(summary)
            .with_schema_id(self.metadata.current_schema_id())
            .with_timestamp_ms(now_ms())
            .build();

        let current_md_loc = format!("{}/v{}.metadata.json", self.metadata_dir(), self.version);
        let result = self
            .metadata
            .clone()
            .into_builder(Some(current_md_loc))
            .add_snapshot(snapshot)?
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )?
            .build()?;
        self.metadata = result.metadata;
        self.version += 1;
        self.write_metadata(self.version).await?;
        Ok(())
    }

    /// A per-file-unique suffix for generated file names.
    fn unique_suffix(&self) -> String {
        format!(
            "{}-{}",
            self.version,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }
}

/// An ascending Iceberg sort order over `field_ids` (identity transform,
/// nulls-first). Empty → the unsorted order.
fn sort_order_on(field_ids: &[i32]) -> Result<SortOrder> {
    if field_ids.is_empty() {
        return Ok(SortOrder::unsorted_order());
    }
    let mut builder = SortOrder::builder();
    for &id in field_ids {
        builder.with_sort_field(SortField {
            source_id: id,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        });
    }
    builder
        .build_unbound()
        .map_err(|e| LakehouseError::Iceberg(format!("building sort order: {e}")))
}

/// Evolve the persisted Iceberg schema `current` toward the freshly-computed
/// `desired` (slot-based field-ids, current logical column order). Reuse-by-id:
/// a desired field whose id already exists in `current` carries the persisted
/// field's *type* forward (preserving nested list/map child ids), changing only
/// the name when it differs; a desired id absent from `current` is a brand-new
/// column (taken verbatim); a current id absent from `desired` is a dropped
/// column (omitted).
///
/// The result's field order is `desired`'s order (the current logical order), so
/// the writer's positional record-batch construction stays correct. Returns
/// `None` when the evolved schema is field-for-field identical to `current` (the
/// never-altered / no-op case), so the caller emits no schema commit.
fn reconcile_schema(
    current: &IcebergSchema,
    desired: &IcebergSchema,
) -> Result<Option<IcebergSchema>> {
    use iceberg::spec::NestedField;

    let evolved: Vec<_> = desired
        .as_struct()
        .fields()
        .iter()
        .map(|d| match current.field_by_id(d.id) {
            // Reuse the persisted field (its type, incl. nested ids); rename if
            // the name changed, and honor the desired requiredness.
            Some(existing) => {
                let mut f = NestedField::new(
                    existing.id,
                    d.name.clone(),
                    existing.field_type.as_ref().clone(),
                    d.required,
                );
                f.doc = existing.doc.clone();
                f.initial_default = existing.initial_default.clone();
                f.write_default = existing.write_default.clone();
                Arc::new(f)
            }
            // Brand-new column: take the desired field verbatim.
            None => d.clone(),
        })
        .collect();

    let rebuilt = IcebergSchema::builder()
        .with_schema_id(current.schema_id())
        .with_identifier_field_ids(desired.identifier_field_ids().collect::<Vec<_>>())
        .with_fields(evolved)
        .build()
        .map_err(|e| LakehouseError::Schema(format!("reconciling schema: {e}")))?;

    if schemas_equivalent(current, &rebuilt) {
        Ok(None)
    } else {
        Ok(Some(rebuilt))
    }
}

/// Two schemas are equivalent for reconcile purposes when their top-level fields
/// match by (id, name, required, type) in the same order. (Schema-id is equal by
/// construction, so we compare the struct fields, not the whole schema.)
fn schemas_equivalent(a: &IcebergSchema, b: &IcebergSchema) -> bool {
    let fa = a.as_struct().fields();
    let fb = b.as_struct().fields();
    fa.len() == fb.len()
        && fa.iter().zip(fb.iter()).all(|(x, y)| {
            x.id == y.id
                && x.name == y.name
                && x.required == y.required
                && x.field_type == y.field_type
        })
}

/// Wall-clock milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A table-unique snapshot id (random-ish from the clock, retried on collision).
fn fresh_snapshot_id(metadata: &TableMetadata) -> i64 {
    let mut id = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(1) as i64)
        & i64::MAX;
    while metadata.snapshots().any(|s| s.snapshot_id() == id) {
        id = id.wrapping_add(1) & i64::MAX;
    }
    id.max(1)
}

/// The gluesql primary [`Key`] as a [`Value`] for the equality-delete column.
fn key_to_value(key: &Key) -> Value {
    match key {
        Key::I8(n) => Value::I8(*n),
        Key::I16(n) => Value::I16(*n),
        Key::I32(n) => Value::I32(*n),
        Key::I64(n) => Value::I64(*n),
        Key::I128(n) => Value::I128(*n),
        Key::U8(n) => Value::U8(*n),
        Key::U16(n) => Value::U16(*n),
        Key::U32(n) => Value::U32(*n),
        Key::U64(n) => Value::U64(*n),
        Key::U128(n) => Value::U128(*n),
        Key::Str(s) => Value::Str(s.clone()),
        Key::Bytea(b) => Value::Bytea(b.clone()),
        Key::Bool(b) => Value::Bool(*b),
        Key::Date(d) => Value::Date(*d),
        Key::Timestamp(t) => Value::Timestamp(*t),
        Key::Time(t) => Value::Time(*t),
        Key::Uuid(u) => Value::Uuid(*u),
        Key::Decimal(d) => Value::Decimal(*d),
        other => Value::Str(format!("{other:?}")),
    }
}

/// Build one Arrow array of `arrow_dt` from gluesql `cells` (scalar types).
fn build_arrow_column(arrow_dt: &ArrowDataType, cells: &[&Value]) -> Result<ArrayRef> {
    let arr: ArrayRef = match arrow_dt {
        ArrowDataType::Boolean => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect::<BooleanArray>(),
        ),
        ArrowDataType::Int32 => {
            Arc::new(cells.iter().map(|v| to_i32(v)).collect::<Int32Array>())
        }
        ArrowDataType::Int64 => {
            Arc::new(cells.iter().map(|v| to_i64(v)).collect::<Int64Array>())
        }
        ArrowDataType::Float32 => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::F32(f) => Some(*f),
                    _ => None,
                })
                .collect::<Float32Array>(),
        ),
        ArrowDataType::Float64 => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::F64(f) => Some(*f),
                    Value::F32(f) => Some(*f as f64),
                    _ => None,
                })
                .collect::<Float64Array>(),
        ),
        ArrowDataType::Utf8 => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::Str(s) => Some(s.clone()),
                    _ => None,
                })
                .collect::<StringArray>(),
        ),
        ArrowDataType::Binary => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::Bytea(b) => Some(b.clone()),
                    _ => None,
                })
                .collect::<BinaryArray>(),
        ),
        // iceberg-rust maps Iceberg `binary` to Arrow `LargeBinary` — used by the
        // composite-PK surrogate column (`__bluedb_pk BYTEA`) and any BYTEA column.
        ArrowDataType::LargeBinary => Arc::new(
            cells
                .iter()
                .map(|v| match v {
                    Value::Bytea(b) => Some(b.clone()),
                    _ => None,
                })
                .collect::<LargeBinaryArray>(),
        ),
        // iceberg-rust maps Iceberg `decimal(p,s)` to Arrow `Decimal128(p,s)`.
        // gluesql stores rust_decimal::Decimal with its own scale; we rescale
        // to the target scale before extracting the i128 mantissa.
        ArrowDataType::Decimal128(precision, scale) => {
            let target_scale = *scale as u32;
            let pow = 10i128.pow(target_scale);
            let raw: Vec<Option<i128>> = cells
                .iter()
                .map(|v| match v {
                    Value::Decimal(d) => {
                        let mut d = *d;
                        d.rescale(target_scale);
                        Some(d.mantissa())
                    }
                    // Integer columns that exceed Long map to decimal(p,0): u64,
                    // i128/u128 (e.g. the ledger's u128 amounts). Their mantissa is
                    // the integer scaled to the target scale (0 in practice).
                    Value::U64(n) => Some((*n as i128) * pow),
                    Value::U128(n) => Some((*n as i128) * pow),
                    Value::I128(n) => Some((*n) * pow),
                    _ => None,
                })
                .collect();
            let arr = raw
                .into_iter()
                .collect::<Decimal128Array>()
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| LakehouseError::Schema(format!("decimal array: {e}")))?;
            Arc::new(arr)
        }
        // iceberg-rust maps Iceberg `date` to Arrow `Date32` (days since epoch).
        // chrono's NaiveDate::signed_duration_since gives a Duration; .num_days()
        // converts to i32.
        ArrowDataType::Date32 => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Arc::new(
                cells
                    .iter()
                    .map(|v| match v {
                        Value::Date(d) => {
                            Some(d.signed_duration_since(epoch).num_days() as i32)
                        }
                        _ => None,
                    })
                    .collect::<Date32Array>(),
            )
        }
        // iceberg-rust maps Iceberg `timestamp` to Arrow `Timestamp(Microsecond, None)`.
        // gluesql stores NaiveDateTime; we compute microseconds since Unix epoch.
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            Arc::new(
                cells
                    .iter()
                    .map(|v| match v {
                        Value::Timestamp(dt) => {
                            let secs = dt.and_utc().timestamp();
                            let subsec_micros = dt.and_utc().timestamp_subsec_micros() as i64;
                            secs.checked_mul(1_000_000)
                                .and_then(|s| s.checked_add(subsec_micros))
                        }
                        _ => None,
                    })
                    .collect::<TimestampMicrosecondArray>(),
            )
        }
        // iceberg-rust maps Iceberg `time` to Arrow `Time64(Microsecond)`.
        // gluesql stores NaiveTime; microseconds since midnight.
        ArrowDataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(
                cells
                    .iter()
                    .map(|v| match v {
                        Value::Time(t) => {
                            let h = t.hour() as i64;
                            let m = t.minute() as i64;
                            let s = t.second() as i64;
                            let micros = t.nanosecond() as i64 / 1_000;
                            Some(
                                h * 3_600_000_000
                                    + m * 60_000_000
                                    + s * 1_000_000
                                    + micros,
                            )
                        }
                        _ => None,
                    })
                    .collect::<Time64MicrosecondArray>(),
            )
        }
        other => {
            return Err(LakehouseError::Schema(format!(
                "arrow column type {other:?} not supported by the v1 writer yet"
            )))
        }
    };
    Ok(arr)
}

/// Coerce a gluesql scalar to `i32` (signed/unsigned ints that fit Int32).
fn to_i32(v: &Value) -> Option<i32> {
    match v {
        Value::I8(n) => Some(*n as i32),
        Value::I16(n) => Some(*n as i32),
        Value::I32(n) => Some(*n),
        Value::U8(n) => Some(*n as i32),
        Value::U16(n) => Some(*n as i32),
        _ => None,
    }
}

/// Coerce a gluesql scalar to `i64` (signed/unsigned ints that fit Long).
fn to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::I8(n) => Some(*n as i64),
        Value::I16(n) => Some(*n as i64),
        Value::I32(n) => Some(*n as i64),
        Value::I64(n) => Some(*n),
        Value::U8(n) => Some(*n as i64),
        Value::U16(n) => Some(*n as i64),
        Value::U32(n) => Some(*n as i64),
        _ => None,
    }
}

#[cfg(test)]
mod scoped_scan_spike {
    //! De-risk spike for incremental compaction: prove we can scan a transient
    //! snapshot that references a hand-picked subset of manifest files, with the
    //! reader applying equality deletes (sequence-aware) to just that subset.
    use super::*;
    use gluesql_core::data::Key;
    use gluesql_core::store::DataRow;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

    fn docs_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "body", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn row(id: i64, body: &str) -> (Key, DataRow) {
        (
            Key::I64(id),
            DataRow::Vec(vec![Value::I64(id), Value::Str(body.to_string())]),
        )
    }

    async fn rows_of(batches: &[RecordBatch]) -> std::collections::BTreeMap<i64, String> {
        use arrow_array::{Int64Array, StringArray};
        let mut out = std::collections::BTreeMap::new();
        for b in batches {
            let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            let bodies = b.column_by_name("body").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.insert(ids.value(i), bodies.value(i).to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn scoped_scan_applies_deletes_to_a_manifest_subset() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1)
            .await
            .unwrap();

        // Three commits → three data manifests + three delete manifests (each
        // upsert stages an equality delete on its key).
        w.upsert(&[row(1, "a")]).await.unwrap(); // seq 1
        w.commit_snapshot(1).await.unwrap();
        w.upsert(&[row(2, "b")]).await.unwrap(); // seq 2
        w.commit_snapshot(2).await.unwrap();
        w.upsert(&[row(1, "A")]).await.unwrap(); // seq 3 — updates key 1
        w.commit_snapshot(3).await.unwrap();

        // Full merged state is {1:"A", 2:"b"}.
        let full = rows_of(&w.scan_manifest_subset({
            let (mut d, mut del) = w.live_manifest_files().await.unwrap();
            d.append(&mut del);
            d
        }).await.unwrap()).await;
        assert_eq!(full.get(&1).map(String::as_str), Some("A"));
        assert_eq!(full.get(&2).map(String::as_str), Some("b"));

        // Cohort = every data manifest EXCEPT the newest (which holds (1,"A")),
        // plus ALL delete manifests. The seq-3 delete on key 1 must still retire
        // the old (1,"a") in the lower-seq data file we keep.
        let (mut data, deletes) = w.live_manifest_files().await.unwrap();
        let newest = data.iter().map(|m| m.sequence_number).max().unwrap();
        data.retain(|m| m.sequence_number != newest); // drop the (1,"A") manifest
        let mut cohort = data;
        cohort.extend(deletes);

        let scoped = rows_of(&w.scan_manifest_subset(cohort).await.unwrap()).await;
        // (1,"A") absent (its manifest omitted); (1,"a") absent (seq-3 delete
        // applied); (2,"b") present. Proves subset scoping AND delete application.
        assert_eq!(scoped.len(), 1, "scoped = {scoped:?}");
        assert_eq!(scoped.get(&2).map(String::as_str), Some("b"));
        assert_eq!(scoped.get(&1), None);
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::reconcile_schema;
    use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema as IcebergSchema, Type};

    fn field(id: i32, name: &str, req: bool) -> NestedField {
        let ty = Type::Primitive(PrimitiveType::String);
        if req {
            NestedField::required(id, name, ty)
        } else {
            NestedField::optional(id, name, ty)
        }
    }

    fn schema(pk: i32, fields: Vec<NestedField>) -> IcebergSchema {
        let fields: Vec<NestedFieldRef> = fields.into_iter().map(|f| f.into()).collect();
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![pk])
            .with_fields(fields)
            .build()
            .unwrap()
    }

    fn names(s: &IcebergSchema) -> Vec<(i32, String)> {
        s.as_struct()
            .fields()
            .iter()
            .map(|f| (f.id, f.name.clone()))
            .collect()
    }

    #[test]
    fn identical_schema_is_noop() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        assert!(reconcile_schema(&cur, &des).unwrap().is_none());
    }

    #[test]
    fn add_column_appends_field() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(
            1,
            vec![field(1, "id", true), field(2, "a", false), field(3, "c", false)],
        );
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(
            names(&out),
            vec![(1, "id".into()), (2, "a".into()), (3, "c".into())]
        );
    }

    #[test]
    fn drop_column_removes_field_keeping_ids() {
        // current [id(1), a(2), b(3)] → desired drops `a` → [id(1), b(3)].
        let cur = schema(
            1,
            vec![field(1, "id", true), field(2, "a", false), field(3, "b", false)],
        );
        let des = schema(1, vec![field(1, "id", true), field(3, "b", false)]);
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(names(&out), vec![(1, "id".into()), (3, "b".into())]);
    }

    #[test]
    fn rename_keeps_id_changes_name() {
        let cur = schema(1, vec![field(1, "id", true), field(2, "a", false)]);
        let des = schema(1, vec![field(1, "id", true), field(2, "alpha", false)]);
        let out = reconcile_schema(&cur, &des).unwrap().unwrap();
        assert_eq!(names(&out), vec![(1, "id".into()), (2, "alpha".into())]);
        // The reused field keeps its id (2) — Iceberg rename, not re-add.
        assert_eq!(out.as_struct().fields()[1].id, 2);
    }
}
