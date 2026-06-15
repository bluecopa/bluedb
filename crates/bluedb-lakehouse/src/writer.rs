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
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, RecordBatch, StringArray,
};
use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use gluesql_core::data::{Key, Value};
use gluesql_core::store::DataRow;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Schema as IcebergSchema, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary, TableMetadata, MAIN_BRANCH,
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
        Self::open(file_io, root, namespace, table, schema, pk_field_id).await
    }

    /// Open the table at `<root>/<namespace>/<table>`: load it from
    /// `version-hint.text` if present, else create it (writing `v0.metadata.json`).
    pub async fn open(
        file_io: FileIO,
        root: &str,
        namespace: &str,
        table: &str,
        schema: IcebergSchema,
        pk_field_id: i32,
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
            let schema = metadata.current_schema().clone();
            Ok(Self {
                file_io,
                table_root,
                table_ident,
                schema,
                pk_field_id,
                metadata,
                version,
                pending_data: Vec::new(),
                pending_deletes: Vec::new(),
            })
        } else {
            let creation = TableCreation::builder()
                .name(table.to_string())
                .location(table_root.clone())
                .schema(schema)
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
    fn rows_to_record_batch(&self, rows: &[(Key, DataRow)]) -> Result<RecordBatch> {
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
    fn keys_to_delete_batch(&self, keys: &[&Key]) -> Result<RecordBatch> {
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

        if self.data_file_count().await? <= 1 {
            return Ok(()); // nothing to gain
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

    /// Author one snapshot from the pending data/delete files. When `replace`,
    /// the manifest list contains ONLY the new files (a compaction `Replace`);
    /// otherwise it carries the parent's manifests forward (incremental
    /// append/overwrite). Applies the table updates to our hosted metadata and
    /// publishes the next `metadata.json`.
    async fn commit_internal(&mut self, watermark: i64, replace: bool) -> Result<()> {
        let snapshot_id = fresh_snapshot_id(&self.metadata);
        let next_seq = self.metadata.next_sequence_number();
        let parent_id = self.metadata.current_snapshot_id();
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

        let manifest_list_path = format!("{}/snap-{snapshot_id}.avro", self.metadata_dir());
        let mut mlw = ManifestListWriter::v2(
            self.file_io.new_output(&manifest_list_path)?,
            snapshot_id,
            parent_id,
            next_seq,
        );
        mlw.add_manifests(manifests.into_iter())?;
        mlw.close().await?;

        let operation = if replace {
            Operation::Replace
        } else if self.metadata.current_snapshot().is_none() {
            Operation::Append
        } else {
            Operation::Overwrite
        };
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
