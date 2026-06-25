//! **HTAP read-latency harness vs REAL GCS — the slice-2 "qualifier" proof.**
//!
//! Slice 1 (`tests/union.rs`) proved DataFusion can *correctly* query
//! Iceberg ∪ fresh-Arrow. The open question for calling the analytical tier
//! "online HTAP" rather than "interactive batch" is **latency**: a query that
//! has to drag Parquet + Iceberg metadata across a WAN to GCS is seconds-slow
//! and region-dependent. The unified-query design's answer is a **foyer
//! read-cache** at the `object_store` seam — Iceberg data/metadata files are
//! immutable, so a cache over them is trivially coherent (evict-only, never
//! invalidate). This harness *measures* what that buys.
//!
//! ## What it does, in ONE run
//!
//! 1. **Build** a `grid (id BIGINT PK, name TEXT, category TEXT, amount
//!    DECIMAL(12,2), created DATE)` Iceberg table on real GCS via the lakehouse
//!    engine, ~15k rows in 3 sealed batches → 3 Parquet data files. (Excluded
//!    from all read timing.)
//! 2. **Cold-load** that table back through a layered object store:
//!    `CachingObjectStore { MeasuredStore { Gcs } }` — foyer in front, an
//!    op-counter in the middle, GCS at the bottom. MeasuredStore therefore only
//!    counts ops that *reach GCS* (cache misses).
//! 3. For each of `Q_agg` / `Q_grid` / `Q_point`, run the SAME query **twice**:
//!    - **MISS** (cold): first execution fetches Parquet/metadata from GCS and
//!      populates foyer.
//!    - **HIT** (warm): re-run; served from foyer; GCS ops should drop to ~0.
//!
//!    Report miss vs hit latency + speedup, GCS-ops miss vs hit, and where the
//!    miss-pass time went (GCS vs foyer vs DataFusion compute).
//!
//! The headline: the warm/foyer-hit latency is **local and region-independent**
//! — that is the HTAP qualifier.
//!
//! `#[ignore]`d + env-gated (no creds committed), like `seal_latency_gcs.rs`:
//!
//! ```text
//! BLUEDB_GCS_TEST_BUCKET=copa-assistant.firebasestorage.app \
//! BLUEDB_GCS_TEST_SA=/path/to/sa.json \
//! BLUEDB_GCS_TEST_PREFIX=bluedb-read-latency-foyer/run-$(date +%s) \
//!   cargo test -p bluedb-spike-htap --test read_latency_gcs -- --ignored --nocapture 2>&1
//! ```
//!
//! Delete `gs://$BUCKET/$PREFIX` afterward (SlateDB + Iceberg both live under it).

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bluedb_lakehouse::{namespace_for_tenant, object_store_file_io, LakehouseEngine};
use bluedb_sql::{CdcConfig, Database};
use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    PsyncIoEngineConfig,
};
use futures::stream::BoxStream;
use gluesql_core::prelude::Glue;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::TableIdent;
use iceberg_datafusion::IcebergStaticTableProvider;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as OsPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use datafusion::prelude::SessionContext;

// ---------------------------------------------------------------------------
// env / store construction
// ---------------------------------------------------------------------------

fn gcs_env() -> Option<(String, String, String)> {
    Some((
        std::env::var("BLUEDB_GCS_TEST_BUCKET").ok()?,
        std::env::var("BLUEDB_GCS_TEST_SA").ok()?,
        std::env::var("BLUEDB_GCS_TEST_PREFIX").ok()?,
    ))
}

fn gcs_store(bucket: &str, sa: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket.to_string())
            .with_service_account_path(sa.to_string())
            .build()
            .expect("build GCS object store"),
    )
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// MeasuredStore — counts ops + bytes + wall time that REACH it. Layered BELOW
// the cache, so on a warm pass these counters barely move: a cache hit is served
// before the call ever descends here.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct OpStats {
    get_calls: AtomicU64,
    get_range_calls: AtomicU64,
    head_calls: AtomicU64,
    list_calls: AtomicU64,
    bytes: AtomicU64,
    nanos: AtomicU64,
}

impl OpStats {
    fn snapshot(&self) -> OpSnapshot {
        OpSnapshot {
            get_calls: self.get_calls.load(Ordering::Relaxed),
            get_range_calls: self.get_range_calls.load(Ordering::Relaxed),
            head_calls: self.head_calls.load(Ordering::Relaxed),
            list_calls: self.list_calls.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            nanos: self.nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
struct OpSnapshot {
    get_calls: u64,
    get_range_calls: u64,
    head_calls: u64,
    list_calls: u64,
    bytes: u64,
    nanos: u64,
}

impl OpSnapshot {
    /// Total GCS read ops (the count that should crater to ~0 on a cache hit).
    fn read_ops(&self) -> u64 {
        self.get_calls + self.get_range_calls + self.head_calls + self.list_calls
    }
    fn since(&self, base: &OpSnapshot) -> OpSnapshot {
        OpSnapshot {
            get_calls: self.get_calls - base.get_calls,
            get_range_calls: self.get_range_calls - base.get_range_calls,
            head_calls: self.head_calls - base.head_calls,
            list_calls: self.list_calls - base.list_calls,
            bytes: self.bytes - base.bytes,
            nanos: self.nanos - base.nanos,
        }
    }
}

struct MeasuredStore {
    inner: Arc<dyn ObjectStore>,
    stats: Arc<OpStats>,
}

impl std::fmt::Debug for MeasuredStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MeasuredStore")
    }
}
impl std::fmt::Display for MeasuredStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MeasuredStore")
    }
}

#[async_trait]
impl ObjectStore for MeasuredStore {
    async fn get(&self, location: &OsPath) -> OsResult<GetResult> {
        self.stats.get_calls.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let res = self.inner.get(location).await;
        self.stats
            .nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Ok(r) = &res {
            self.stats.bytes.fetch_add(r.meta.size, Ordering::Relaxed);
        }
        res
    }

    async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
        // Classify so head/range routed through get_opts are counted correctly.
        if options.head {
            self.stats.head_calls.fetch_add(1, Ordering::Relaxed);
        } else if options.range.is_some() {
            self.stats.get_range_calls.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.get_calls.fetch_add(1, Ordering::Relaxed);
        }
        let t = Instant::now();
        let res = self.inner.get_opts(location, options).await;
        self.stats
            .nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Ok(r) = &res {
            self.stats.bytes.fetch_add(r.meta.size, Ordering::Relaxed);
        }
        res
    }

    async fn get_range(&self, location: &OsPath, range: Range<u64>) -> OsResult<Bytes> {
        self.stats.get_range_calls.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let res = self.inner.get_range(location, range).await;
        self.stats
            .nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Ok(b) = &res {
            self.stats
                .bytes
                .fetch_add(b.len() as u64, Ordering::Relaxed);
        }
        res
    }

    async fn head(&self, location: &OsPath) -> OsResult<ObjectMeta> {
        self.stats.head_calls.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let res = self.inner.head(location).await;
        self.stats
            .nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        res
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.stats.list_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.stats.list_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    // --- writes / mutations: pass through (used only in the build phase) ---
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn delete(&self, location: &OsPath) -> OsResult<()> {
        self.inner.delete(location).await
    }
    async fn copy(&self, from: &OsPath, to: &OsPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &OsPath, to: &OsPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ---------------------------------------------------------------------------
// CachingObjectStore — foyer in front of MeasuredStore. Iceberg data + metadata
// files are IMMUTABLE, so cached entries are coherent forever (evict-only). On a
// hit we return the cached bytes without ever descending to MeasuredStore/GCS.
// ---------------------------------------------------------------------------

/// foyer cache: key = a string describing (path[, byte-range]); value = the
/// bytes for that read. String + Vec<u8> get foyer's serde blanket `Code` impl
/// (SlateDB enables foyer's `serde` feature workspace-wide), so the disk tier
/// (de)serializes them via bincode with no hand-rolled codec.
type ReadCache = HybridCache<String, Vec<u8>>;

#[derive(Default)]
struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    /// Wall time spent inside foyer on the hit path (the "local read" cost).
    hit_nanos: AtomicU64,
    /// Wall time spent inserting fetched bytes into foyer on the miss path.
    insert_nanos: AtomicU64,
}

struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: ReadCache,
    stats: Arc<CacheStats>,
}

impl std::fmt::Debug for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CachingObjectStore")
    }
}
impl std::fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CachingObjectStore")
    }
}

impl CachingObjectStore {
    fn whole_key(location: &OsPath) -> String {
        format!("obj|{location}")
    }
    fn range_key(location: &OsPath, range: &Range<u64>) -> String {
        format!("rng|{location}|{}-{}", range.start, range.end)
    }

    /// Probe foyer. `Some(bytes)` = HIT (records hit + foyer-get time);
    /// `None` = MISS.
    async fn cache_get(&self, key: &str) -> Option<Vec<u8>> {
        let t = Instant::now();
        let got = self.cache.get(key).await;
        match got {
            Ok(Some(entry)) => {
                self.stats
                    .hit_nanos
                    .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value().clone())
            }
            // Miss or any cache error → treat as miss (fall through to inner).
            _ => None,
        }
    }

    fn cache_put(&self, key: String, bytes: &[u8]) {
        let t = Instant::now();
        // `insert` returns an entry handle synchronously; the disk write happens
        // in the background. Dropping the handle is fine.
        let _ = self.cache.insert(key, bytes.to_vec());
        self.stats
            .insert_nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn get(&self, location: &OsPath) -> OsResult<GetResult> {
        let key = Self::whole_key(location);
        if let Some(bytes) = self.cache_get(&key).await {
            return Ok(get_result_from_bytes(location, Bytes::from(bytes)));
        }
        // MISS: fetch whole object from below, cache the bytes, return.
        let res = self.inner.get(location).await?;
        let meta = res.meta.clone();
        let bytes = res.bytes().await?;
        self.cache_put(key, &bytes);
        Ok(get_result_from_meta(meta, bytes))
    }

    async fn get_range(&self, location: &OsPath, range: Range<u64>) -> OsResult<Bytes> {
        let key = Self::range_key(location, &range);
        if let Some(bytes) = self.cache_get(&key).await {
            return Ok(Bytes::from(bytes));
        }
        // MISS: fetch the range from below, cache it, return.
        let bytes = self.inner.get_range(location, range).await?;
        self.cache_put(key, &bytes);
        Ok(bytes)
    }

    async fn get_ranges(&self, location: &OsPath, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        // Per-range cache lookups (each range is its own immutable slice).
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.get_range(location, r.clone()).await?);
        }
        Ok(out)
    }

    async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
        // Route the cacheable shapes through the explicit cached paths; anything
        // exotic (conditional/if-match/etc.) bypasses the cache to stay correct.
        let plain = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none();
        if plain && !options.head {
            match &options.range {
                None => return self.get(location).await,
                Some(_) => { /* fall through: ranged get_opts is rare here */ }
            }
        }
        // Head or non-cacheable: straight to inner (counted by MeasuredStore).
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &OsPath) -> OsResult<ObjectMeta> {
        // HEAD is metadata-only + cheap; pass through so it reaches GCS (and we
        // can SEE any head-only residue on the warm pass).
        self.inner.head(location).await
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    // --- writes: pass through ---
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn delete(&self, location: &OsPath) -> OsResult<()> {
        self.inner.delete(location).await
    }
    async fn copy(&self, from: &OsPath, to: &OsPath) -> OsResult<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &OsPath, to: &OsPath) -> OsResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Rebuild a `GetResult` for the WHOLE object from cached bytes — we never kept
/// the real `ObjectMeta`, so synthesize a plausible one (size is what callers
/// read; the Iceberg reader only uses `meta.size` + the bytes).
fn get_result_from_bytes(location: &OsPath, bytes: Bytes) -> GetResult {
    let meta = ObjectMeta {
        location: location.clone(),
        // Synthetic: the Iceberg reader only consumes `size` + bytes, never this.
        last_modified: Default::default(),
        size: bytes.len() as u64,
        e_tag: None,
        version: None,
    };
    get_result_from_meta(meta, bytes)
}

fn get_result_from_meta(meta: ObjectMeta, bytes: Bytes) -> GetResult {
    let range = 0..bytes.len() as u64;
    GetResult {
        payload: object_store::GetResultPayload::Stream(Box::pin(futures::stream::once(
            async move { Ok(bytes) },
        ))),
        meta,
        range,
        attributes: object_store::Attributes::default(),
    }
}

// ---------------------------------------------------------------------------
// build phase (excluded from read timing)
// ---------------------------------------------------------------------------

/// How many of the ~8 category values exist (used to pick a present value for
/// `Q_grid` and to size selectivity).
const CATEGORIES: [&str; 8] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
];

const TOTAL_ROWS: i64 = 15_000;
const BATCHES: i64 = 3;

/// Build the `grid` Iceberg table on GCS via the real lakehouse engine: SlateDB
/// at `<prefix>/slatedb`, Iceberg at `<prefix>/lakehouse`. INSERT TOTAL_ROWS rows
/// in BATCHES sealed batches → BATCHES Parquet data files. Returns nothing; the
/// table is read back COLD in the read phase.
async fn build_grid(store: Arc<dyn ObjectStore>, prefix: &str) {
    let db = Database::new(Arc::new(
        slatedb::Db::open(format!("{prefix}/slatedb"), store.clone())
            .await
            .expect("open slatedb on gcs"),
    ));
    let file_io = object_store_file_io(store.clone(), "");
    let cdc = CdcConfig::default();
    let engine = Arc::new(
        LakehouseEngine::reopen(
            file_io,
            format!("{prefix}/lakehouse"),
            "_",
            db.clone(),
            cdc.clone(),
        )
        .await
        .expect("reopen lakehouse engine on gcs"),
    );
    engine
        .enable_table("grid")
        .await
        .expect("enable table grid");
    {
        let mut g = Glue::new(db.connection_serialized());
        // We go through `Glue` directly (not the server's `execute_sql`), spelling
        // the POST-rewrite types the substrate actually stores. TWO forced
        // deviations from the prompt's `id BIGINT … amount DECIMAL(12,2) … created
        // DATE`, both consequences of bluedb's CURRENT seal path (not the spike):
        //   * `INTEGER` = gluesql `DataType::Int` = i64 — bluedb-sql's target for
        //     `BIGINT`. The lakehouse maps it to Iceberg `Long`, i.e. a genuine
        //     64-bit `id` column in the Parquet DataFusion reads.
        //   * `amount`/`created`: bluedb's v1 seal writer (`writer.rs`
        //     `to_arrow_array`) has NO Decimal128 or Date32 arm — it handles only
        //     Int32/Int64/Float32/Float64/Utf8/Binary, and errors ("arrow column
        //     type … not supported by the v1 writer yet") on anything else. So a
        //     DECIMAL/DATE column simply cannot be SEALED today (slice 1's
        //     `union.rs` proved decimal/date fidelity by driving the iceberg writer
        //     DIRECTLY, bypassing this seal path). For a READ-latency / cache spike
        //     we use writer-supported stand-ins: `amount FLOAT` (f64 money — still
        //     real for SUM/ORDER BY) and `created INTEGER` (days-since-epoch, the
        //     physical backing of Date32). The MISS-vs-HIT result (round-trips +
        //     bytes + foyer behavior over ~15k rows in 3 Parquet files) is
        //     unaffected by these logical column types.
        g.execute(
            "CREATE TABLE grid (id INTEGER PRIMARY KEY, name TEXT, category TEXT, \
             amount FLOAT, created INTEGER);",
        )
        .await
        .expect("create table grid");
    }

    let per_batch = TOTAL_ROWS / BATCHES;
    let mut next_id: i64 = 1;
    for b in 0..BATCHES {
        // One multi-row INSERT per batch (keeps it to a few large statements),
        // then a seal so each batch lands as its own Parquet data file.
        let mut values = String::new();
        for _ in 0..per_batch {
            let id = next_id;
            next_id += 1;
            let cat = CATEGORIES[(id as usize) % CATEGORIES.len()];
            // amount: money-ish f64 (whole.frac), pseudo-spread, deterministic.
            let cents = 100 + (id * 37) % 15_000;
            // created: days-since-epoch, spread over ~3 years (Date32 backing).
            let day = 18_000 + (id % 1_000);
            if !values.is_empty() {
                values.push(',');
            }
            values.push_str(&format!(
                "({id}, 'name{id}', '{cat}', {}.{:02}, {day})",
                cents / 100,
                cents % 100,
            ));
        }
        let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
        g.execute(&format!("INSERT INTO grid VALUES {values};"))
            .await
            .expect("insert batch");
        engine.seal().await.expect("seal batch");
        eprintln!(
            "  [build] batch {}/{BATCHES} sealed ({} rows, ids {}..{})",
            b + 1,
            per_batch,
            next_id - per_batch,
            next_id - 1
        );
    }
    db.flush().await.ok();
}

// ---------------------------------------------------------------------------
// cold static-table load through the CachingObjectStore-wrapped store
// ---------------------------------------------------------------------------

/// The `grid` table's on-disk root. The engine maps the default tenant `_` to
/// namespace `default` (see `namespace_for_tenant`), so the table lives at
/// `<root>/default/grid`, NOT `<root>/_/grid`.
fn grid_table_root(lakehouse_root: &str) -> String {
    format!("{lakehouse_root}/{}/grid", namespace_for_tenant("_"))
}

/// Load the `grid` table COLD: read `version-hint.text` + `vN.metadata.json`
/// through `file_io` (which wraps the CachingObjectStore), deserialize
/// `TableMetadata`, and build an iceberg `Table` over the wrapped FileIO — so
/// every subsequent data/metadata read the DataFusion scan issues goes through
/// foyer→MeasuredStore→GCS. Mirrors `LakehouseWriter::open`'s read path.
async fn load_grid_cold(file_io: iceberg::io::FileIO, lakehouse_root: &str) -> Table {
    let table_root = grid_table_root(lakehouse_root);
    let hint_path = format!("{table_root}/metadata/version-hint.text");
    let raw = file_io
        .new_input(&hint_path)
        .expect("hint input")
        .read()
        .await
        .expect("read version-hint");
    let version: u64 = String::from_utf8_lossy(&raw)
        .trim()
        .parse()
        .expect("parse version-hint");
    let md_path = format!("{table_root}/metadata/v{version}.metadata.json");
    let bytes = file_io
        .new_input(&md_path)
        .expect("metadata input")
        .read()
        .await
        .expect("read metadata.json");
    let metadata: TableMetadata = serde_json::from_slice(&bytes).expect("parse TableMetadata");
    let ident = TableIdent::from_strs([namespace_for_tenant("_"), "grid".into()]).expect("ident");
    Table::builder()
        .identifier(ident)
        .file_io(file_io)
        .metadata(metadata)
        .build()
        .expect("build cold iceberg table")
}

// ---------------------------------------------------------------------------
// read phase
// ---------------------------------------------------------------------------

struct QueryRun {
    label: &'static str,
    /// latency, GCS-op delta, foyer-time delta
    miss: PassMetrics,
    hit: PassMetrics,
    rows: usize,
}

struct PassMetrics {
    latency: Duration,
    gcs: OpSnapshot,
    cache_hits: u64,
    cache_misses: u64,
    foyer_hit_nanos: u64,
    foyer_insert_nanos: u64,
}

/// Run `sql` once, returning (row_count, latency, gcs-op delta, cache deltas).
#[allow(clippy::too_many_arguments)]
async fn run_pass(
    ctx: &SessionContext,
    sql: &str,
    op_stats: &OpStats,
    cache_stats: &CacheStats,
) -> (usize, PassMetrics) {
    let gcs_before = op_stats.snapshot();
    let hits_before = cache_stats.hits.load(Ordering::Relaxed);
    let misses_before = cache_stats.misses.load(Ordering::Relaxed);
    let hit_ns_before = cache_stats.hit_nanos.load(Ordering::Relaxed);
    let ins_ns_before = cache_stats.insert_nanos.load(Ordering::Relaxed);

    let t = Instant::now();
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan sql")
        .collect()
        .await
        .expect("collect");
    let latency = t.elapsed();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let gcs_after = op_stats.snapshot();
    let metrics = PassMetrics {
        latency,
        gcs: gcs_after.since(&gcs_before),
        cache_hits: cache_stats.hits.load(Ordering::Relaxed) - hits_before,
        cache_misses: cache_stats.misses.load(Ordering::Relaxed) - misses_before,
        foyer_hit_nanos: cache_stats.hit_nanos.load(Ordering::Relaxed) - hit_ns_before,
        foyer_insert_nanos: cache_stats.insert_nanos.load(Ordering::Relaxed) - ins_ns_before,
    };
    (rows, metrics)
}

/// MISS pass then HIT pass for one query (fresh SessionContext each pass so
/// DataFusion holds no in-process buffers — the ONLY thing warm between passes
/// is foyer).
async fn measure_query(
    label: &'static str,
    sql: &str,
    file_io: &iceberg::io::FileIO,
    lakehouse_root: &str,
    op_stats: &OpStats,
    cache_stats: &CacheStats,
) -> QueryRun {
    // MISS: cold-load table + run (fetches from GCS, populates foyer).
    let table = load_grid_cold(file_io.clone(), lakehouse_root).await;
    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .expect("static provider (miss)");
    let ctx = SessionContext::new();
    ctx.register_table("grid", Arc::new(provider)).unwrap();
    let (rows_miss, miss) = run_pass(&ctx, sql, op_stats, cache_stats).await;

    // HIT: fresh context + freshly cold-loaded table again (metadata now served
    // from foyer), re-run the SAME query → data/metadata served from foyer.
    let table = load_grid_cold(file_io.clone(), lakehouse_root).await;
    let provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .expect("static provider (hit)");
    let ctx = SessionContext::new();
    ctx.register_table("grid", Arc::new(provider)).unwrap();
    let (rows_hit, hit) = run_pass(&ctx, sql, op_stats, cache_stats).await;

    assert_eq!(
        rows_miss, rows_hit,
        "{label}: miss and hit must return the same row count"
    );
    QueryRun {
        label,
        miss,
        hit,
        rows: rows_miss,
    }
}

fn report_query(q: &QueryRun) {
    let speedup = if q.hit.latency.as_secs_f64() > 0.0 {
        q.miss.latency.as_secs_f64() / q.hit.latency.as_secs_f64()
    } else {
        f64::INFINITY
    };
    println!("\n[{}]  rows={}", q.label, q.rows);
    println!(
        "  MISS (cold, GCS): {:.0}ms   GCS ops={} (get={} range={} head={} list={})  GCS bytes={}  time-in-GCS={:.0}ms  foyer-insert={:.1}ms",
        ms(q.miss.latency),
        q.miss.gcs.read_ops(),
        q.miss.gcs.get_calls,
        q.miss.gcs.get_range_calls,
        q.miss.gcs.head_calls,
        q.miss.gcs.list_calls,
        q.miss.gcs.bytes,
        q.miss.gcs.nanos as f64 / 1e6,
        q.miss.foyer_insert_nanos as f64 / 1e6,
    );
    let miss_compute = q.miss.latency.as_secs_f64() * 1e3
        - q.miss.gcs.nanos as f64 / 1e6
        - q.miss.foyer_insert_nanos as f64 / 1e6;
    println!(
        "    miss breakdown: GCS {:.0}ms + foyer-insert {:.1}ms + compute/plan {:.0}ms",
        q.miss.gcs.nanos as f64 / 1e6,
        q.miss.foyer_insert_nanos as f64 / 1e6,
        miss_compute.max(0.0),
    );
    println!(
        "  HIT  (warm, foyer): {:.1}ms   GCS ops={} (get={} range={} head={} list={})  foyer hits={} (foyer-get time={:.2}ms)  misses={}",
        ms(q.hit.latency),
        q.hit.gcs.read_ops(),
        q.hit.gcs.get_calls,
        q.hit.gcs.get_range_calls,
        q.hit.gcs.head_calls,
        q.hit.gcs.list_calls,
        q.hit.cache_hits,
        q.hit.foyer_hit_nanos as f64 / 1e6,
        q.hit.cache_misses,
    );
    println!(
        "  >>> SPEEDUP (miss/hit) = {speedup:.1}x   [hit is local: GCS read-ops dropped {} -> {}]",
        q.miss.gcs.read_ops(),
        q.hit.gcs.read_ops()
    );
}

#[tokio::test]
#[ignore = "real GCS — set BLUEDB_GCS_TEST_{BUCKET,SA,PREFIX}"]
async fn read_latency_miss_vs_hit_against_real_gcs() {
    let Some((bucket, sa, prefix)) = gcs_env() else {
        eprintln!("skipping: set BLUEDB_GCS_TEST_{{BUCKET,SA,PREFIX}}");
        return;
    };
    println!("\n=== bluedb HTAP read-latency: cache MISS (GCS) vs HIT (foyer) vs REAL GCS ===");
    println!("bucket={bucket}  prefix={prefix}");

    // ---- build phase: plain GCS store, NOT measured/cached ----
    let plain = gcs_store(&bucket, &sa);
    println!("\n--- build phase (excluded from read timing) ---");
    let build_t = Instant::now();
    build_grid(plain.clone(), &prefix).await;
    println!(
        "  built grid (~{TOTAL_ROWS} rows, {BATCHES} Parquet files) in {:.1}s",
        build_t.elapsed().as_secs_f64()
    );

    // ---- read store: CachingObjectStore { MeasuredStore { Gcs } } ----
    let op_stats = Arc::new(OpStats::default());
    let cache_stats = Arc::new(CacheStats::default());
    let measured = Arc::new(MeasuredStore {
        inner: gcs_store(&bucket, &sa),
        stats: op_stats.clone(),
    });

    // foyer HybridCache: DRAM (64 MiB) + NVMe/disk tier (a tempdir, 256 MiB,
    // psync IO engine + block engine — honors the DRAM+NVMe design).
    let cache_dir = tempfile::tempdir().expect("foyer tempdir");
    let cache: ReadCache = HybridCacheBuilder::new()
        .with_name("bluedb-read-cache")
        .memory(64 * 1024 * 1024)
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(
                FsDeviceBuilder::new(cache_dir.path())
                    .with_capacity(256 * 1024 * 1024)
                    .build()
                    .expect("foyer fs device"),
            )
            .with_block_size(1024 * 1024),
        )
        .build()
        .await
        .expect("build foyer hybrid cache");
    println!(
        "\n--- read phase: CachingObjectStore {{ MeasuredStore {{ Gcs }} }}, foyer DRAM=64MiB + NVMe={} ---",
        cache_dir.path().display()
    );

    let caching = Arc::new(CachingObjectStore {
        inner: measured.clone(),
        cache,
        stats: cache_stats.clone(),
    });
    let file_io = object_store_file_io(caching.clone() as Arc<dyn ObjectStore>, "");
    let lakehouse_root = format!("{prefix}/lakehouse");

    // ---- raw per-op RTT floor: a single get + a single get_range straight to
    //      GCS through the wrapped store (cold key), to confirm per-op latency. ----
    raw_op_floor(&file_io, &lakehouse_root).await;

    // ---- the three queries: MISS then HIT each ----
    let mid_id = TOTAL_ROWS / 2;
    let present_cat = CATEGORIES[(mid_id as usize) % CATEGORIES.len()];
    let q_agg = "SELECT category, COUNT(*) AS n, SUM(amount) AS total \
                 FROM grid GROUP BY category ORDER BY n DESC";
    let q_grid = format!(
        "SELECT id, name, amount FROM grid WHERE category = '{present_cat}' \
         ORDER BY amount DESC LIMIT 50"
    );
    let q_point = format!("SELECT * FROM grid WHERE id = {mid_id}");

    let runs = vec![
        measure_query(
            "Q_agg  (full GROUP BY + SUM)",
            q_agg,
            &file_io,
            &lakehouse_root,
            &op_stats,
            &cache_stats,
        )
        .await,
        measure_query(
            "Q_grid (selective filter + top-50)",
            &q_grid,
            &file_io,
            &lakehouse_root,
            &op_stats,
            &cache_stats,
        )
        .await,
        measure_query(
            "Q_point (PK equality)",
            &q_point,
            &file_io,
            &lakehouse_root,
            &op_stats,
            &cache_stats,
        )
        .await,
    ];

    println!("\n=== RESULTS: MISS (cold, GCS) vs HIT (warm, foyer) ===");
    for q in &runs {
        report_query(q);
    }

    // ---- headline ----
    let avg_hit = runs.iter().map(|q| ms(q.hit.latency)).sum::<f64>() / runs.len() as f64;
    let avg_miss = runs.iter().map(|q| ms(q.miss.latency)).sum::<f64>() / runs.len() as f64;
    let total_hit_gcs_ops: u64 = runs.iter().map(|q| q.hit.gcs.read_ops()).sum();
    println!("\n=== HEADLINE ===");
    println!(
        "  avg MISS (GCS-bound) = {avg_miss:.0}ms ; avg HIT (foyer-local) = {avg_hit:.1}ms ; mean speedup ~{:.0}x",
        avg_miss / avg_hit.max(0.001)
    );
    println!(
        "  total GCS read-ops across the 3 HIT passes = {total_hit_gcs_ops} (≈0 ⇒ the warm tier is served ENTIRELY from local foyer — region-independent)"
    );

    println!(
        "\n=== done — DELETE gs://{bucket}/{prefix} (SlateDB + Iceberg both live under it) ===\n"
    );
}

/// Raw single-op floor: one cold `read` (whole metadata.json) + one cold ranged
/// read through the wrapped store, to confirm per-op RTT to GCS. Run BEFORE the
/// queries so these keys are genuinely cold the first time.
async fn raw_op_floor(file_io: &iceberg::io::FileIO, lakehouse_root: &str) {
    let table_root = grid_table_root(lakehouse_root);
    let hint_path = format!("{table_root}/metadata/version-hint.text");
    let raw = file_io.new_input(&hint_path).unwrap().read().await.unwrap();
    let version: u64 = String::from_utf8_lossy(&raw).trim().parse().unwrap();
    let md_path = format!("{table_root}/metadata/v{version}.metadata.json");

    let input = file_io.new_input(&md_path).unwrap();
    let t = Instant::now();
    let bytes = input.read().await.unwrap();
    let whole = t.elapsed();
    let len = bytes.len();

    // A 1 KiB ranged read of the same object (cold range key).
    let reader = file_io.new_input(&md_path).unwrap().reader().await.unwrap();
    let end = (len as u64).min(1024);
    let t = Instant::now();
    let _ = reader.read(0..end).await.unwrap();
    let ranged = t.elapsed();

    println!(
        "\n--- raw per-op floor (cold, straight to GCS) ---\n  whole-object read ({len}B metadata.json): {:.0}ms   ranged read (first {end}B): {:.0}ms",
        ms(whole),
        ms(ranged),
    );
}
