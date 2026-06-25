//! `bluedb-cache` — foyer-backed `CachingObjectStore` for the bluedb HTAP
//! analytical read tier.
//!
//! Iceberg/Parquet data files written by the lakehouse are **immutable**
//! (write-once, then sealed).  A read cache over them is trivially coherent:
//! once a byte-range is cached it is valid forever; the cache only evicts,
//! never invalidates.
//!
//! # Design
//!
//! ```text
//! DataFusion / Iceberg reader
//!          │  get / get_range / get_opts
//!          ▼
//!  CachingObjectStore  ─── foyer HybridCache (DRAM + optional NVMe)
//!          │ miss only
//!          ▼
//!  inner Arc<dyn ObjectStore>  (e.g. GCS / S3 / memory)
//! ```
//!
//! * **Cache key** — a plain `String`:
//!   - whole-object `get`:   `"obj|<path>"`
//!   - byte-range `get_range`: `"rng|<path>|<start>-<end>"`
//! * **Cache value** — `Vec<u8>` (foyer's serde blanket `Code` impl handles
//!   bincode (de)serialization for the disk tier; SlateDB enables foyer's
//!   `serde` feature workspace-wide).
//! * `head` / `list` / `put*` / `delete` / `copy` / rename — pass straight
//!   through; NOT cached.
//! * Conditional `get_opts` (if-match / version / etc.) — pass through; only
//!   plain unconditional reads are cached.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use bluedb_cache::{CachingObjectStore, CachingObjectStoreBuilder};
//! use object_store::memory::InMemory;
//!
//! # #[tokio::main] async fn main() {
//! let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
//! let caching = CachingObjectStoreBuilder::new(inner)
//!     .dram_bytes(64 * 1024 * 1024) // 64 MiB DRAM-only cache
//!     .build()
//!     .await
//!     .unwrap();
//! # }
//! ```

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use foyer::{HybridCache, HybridCacheBuilder};
use futures::stream::BoxStream;
use object_store::{path::Path as OsPath, Attributes};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// The foyer cache type used internally: key = descriptive String,
/// value = raw bytes of the cached object/range.
///
/// `String + Vec<u8>` both satisfy foyer's serde blanket `Code` impl
/// (activated workspace-wide by SlateDB's `features = ["serde"]` on foyer).
type ReadCache = HybridCache<String, Vec<u8>>;

// ---------------------------------------------------------------------------
// CacheStats — hit/miss counters exposed for tests and observability
// ---------------------------------------------------------------------------

/// Atomic hit/miss counters for a [`CachingObjectStore`].
///
/// All fields are `u64` counters updated with `Relaxed` ordering (each is
/// independent; the caller just snapshots them for metrics/assertions).
#[derive(Default)]
pub struct CacheStats {
    /// Number of cache hits (inner store NOT called).
    pub hits: AtomicU64,
    /// Number of cache misses (inner store WAS called; bytes inserted into cache).
    pub misses: AtomicU64,
}

impl CacheStats {
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// CachingObjectStore
// ---------------------------------------------------------------------------

/// An [`ObjectStore`] wrapper that caches `get` and `get_range` results in a
/// foyer [`HybridCache`] (DRAM ± NVMe disk tier).
///
/// Construct via [`CachingObjectStoreBuilder`].
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: ReadCache,
    /// Shared stats handle; clone the `Arc` to inspect from tests.
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
    // --- cache key helpers --------------------------------------------------

    fn whole_key(location: &OsPath) -> String {
        format!("obj|{location}")
    }

    fn range_key(location: &OsPath, range: &Range<u64>) -> String {
        format!("rng|{location}|{}-{}", range.start, range.end)
    }

    // --- cache probe --------------------------------------------------------

    /// Returns `Some(bytes)` on HIT; `None` on MISS (or any foyer error, which
    /// is treated as a transparent miss so the caller falls through to `inner`).
    async fn cache_get(&self, key: &str) -> Option<Vec<u8>> {
        match self.cache.get(key).await {
            Ok(Some(entry)) => {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value().clone())
            }
            _ => None,
        }
    }

    /// Insert `bytes` under `key`; increment miss counter.
    ///
    /// `HybridCache::insert` is synchronous — the disk tier write happens in
    /// the background.  Dropping the returned handle is fine.
    fn cache_put(&self, key: String, bytes: &[u8]) {
        let _ = self.cache.insert(key, bytes.to_vec());
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }

    // --- stats accessor -----------------------------------------------------

    /// Returns a cloneable `Arc` to the cache hit/miss counters.
    pub fn stats(&self) -> Arc<CacheStats> {
        self.stats.clone()
    }
}

// ---------------------------------------------------------------------------
// ObjectStore implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectStore for CachingObjectStore {
    // --- cacheable reads ----------------------------------------------------

    async fn get(&self, location: &OsPath) -> OsResult<GetResult> {
        let key = Self::whole_key(location);
        if let Some(bytes) = self.cache_get(&key).await {
            return Ok(get_result_from_bytes(location, Bytes::from(bytes)));
        }
        // MISS: fetch the full object, cache its bytes, then return.
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
        // MISS: fetch the range, cache it, return.
        let bytes = self.inner.get_range(location, range).await?;
        self.cache_put(key, &bytes);
        Ok(bytes)
    }

    async fn get_ranges(&self, location: &OsPath, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        // Per-range cache lookups so each immutable slice is cached individually.
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.get_range(location, r.clone()).await?);
        }
        Ok(out)
    }

    async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
        // Only cache plain unconditional reads (no if-match / version / etc.).
        // Conditional requests must always reach the inner store.
        let plain = options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
            && options.version.is_none();
        if plain && !options.head {
            if options.range.is_none() {
                return self.get(location).await;
            }
            // Ranged get_opts: uncommon but cacheable via get_range.
            if let Some(range) = &options.range {
                use object_store::GetRange;
                match range {
                    GetRange::Bounded(r) => {
                        let bytes = self.get_range(location, r.clone()).await?;
                        return Ok(get_result_from_bytes(location, bytes));
                    }
                    _ => { /* suffix / offset-from — pass through */ }
                }
            }
        }
        // Head or non-cacheable: straight to inner.
        self.inner.get_opts(location, options).await
    }

    // --- non-cached reads ---------------------------------------------------

    async fn head(&self, location: &OsPath) -> OsResult<ObjectMeta> {
        self.inner.head(location).await
    }

    fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    // --- writes: pass through (Iceberg data files are immutable after seal) --

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
// Helper: reconstruct a GetResult from bytes we already hold
// ---------------------------------------------------------------------------

fn get_result_from_bytes(location: &OsPath, bytes: Bytes) -> GetResult {
    let meta = ObjectMeta {
        location: location.clone(),
        last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
        size: bytes.len() as u64,
        e_tag: None,
        version: None,
    };
    get_result_from_meta(meta, bytes)
}

fn get_result_from_meta(meta: ObjectMeta, bytes: Bytes) -> GetResult {
    let range = 0..bytes.len() as u64;
    GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
            async move { Ok(bytes) },
        ))),
        meta,
        range,
        attributes: Attributes::default(),
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`CachingObjectStore`].
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use bluedb_cache::CachingObjectStoreBuilder;
/// # use object_store::memory::InMemory;
/// # #[tokio::main] async fn main() {
/// let store = CachingObjectStoreBuilder::new(Arc::new(InMemory::new()))
///     .dram_bytes(32 * 1024 * 1024)
///     .build()
///     .await
///     .unwrap();
/// # }
/// ```
pub struct CachingObjectStoreBuilder {
    inner: Arc<dyn ObjectStore>,
    dram_bytes: usize,
}

impl CachingObjectStoreBuilder {
    /// Create a builder wrapping `inner`.  Defaults: 64 MiB DRAM-only.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            dram_bytes: 64 * 1024 * 1024,
        }
    }

    /// Set DRAM tier capacity in bytes.
    pub fn dram_bytes(mut self, bytes: usize) -> Self {
        self.dram_bytes = bytes;
        self
    }

    /// Build the [`CachingObjectStore`].  Starts foyer's background threads.
    pub async fn build(self) -> Result<CachingObjectStore, foyer::Error> {
        // `.storage().build()` without an engine config → "noop" disk tier →
        // DRAM-only hybrid cache.  The builder typestate requires `.storage()`
        // before `.build()`.
        let cache: ReadCache = HybridCacheBuilder::new()
            .with_name("bluedb-read-cache")
            .memory(self.dram_bytes)
            .storage()
            .build()
            .await?;

        Ok(CachingObjectStore {
            inner: self.inner,
            cache,
            stats: Arc::new(CacheStats::default()),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    // -----------------------------------------------------------------------
    // CountingStore — records every get / get_range call that REACHES it.
    // Layered BELOW the CachingObjectStore; a cache hit never descends here.
    // -----------------------------------------------------------------------

    struct CountingStore {
        inner: InMemory,
        gets: AtomicU64,
        get_ranges: AtomicU64,
    }

    impl CountingStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                gets: AtomicU64::new(0),
                get_ranges: AtomicU64::new(0),
            })
        }
        fn gets(&self) -> u64 {
            self.gets.load(Ordering::Relaxed)
        }
        fn get_ranges(&self) -> u64 {
            self.get_ranges.load(Ordering::Relaxed)
        }
    }

    impl std::fmt::Debug for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingStore")
        }
    }
    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn get(&self, location: &OsPath) -> OsResult<GetResult> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(location).await
        }

        async fn get_opts(&self, location: &OsPath, options: GetOptions) -> OsResult<GetResult> {
            if !options.head && options.range.is_none() {
                self.gets.fetch_add(1, Ordering::Relaxed);
            } else if options.range.is_some() {
                self.get_ranges.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_range(&self, location: &OsPath, range: Range<u64>) -> OsResult<Bytes> {
            self.get_ranges.fetch_add(1, Ordering::Relaxed);
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(
            &self,
            location: &OsPath,
            ranges: &[Range<u64>],
        ) -> OsResult<Vec<Bytes>> {
            self.get_ranges
                .fetch_add(ranges.len() as u64, Ordering::Relaxed);
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &OsPath) -> OsResult<ObjectMeta> {
            self.inner.head(location).await
        }

        fn list(&self, prefix: Option<&OsPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&OsPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

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

    // -----------------------------------------------------------------------
    // Helper: build a DRAM-only CachingObjectStore wrapping a CountingStore
    // -----------------------------------------------------------------------

    async fn make_caching(counting: Arc<CountingStore>) -> CachingObjectStore {
        CachingObjectStoreBuilder::new(counting as Arc<dyn ObjectStore>)
            .dram_bytes(4 * 1024 * 1024) // 4 MiB is plenty for tests
            .build()
            .await
            .expect("build CachingObjectStore")
    }

    // -----------------------------------------------------------------------
    // Test 1: `get` called twice → inner store sees exactly ONE real get;
    //         second call is served from foyer; bytes match both times.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_second_call_served_from_cache() {
        let counting = CountingStore::new();

        // PUT an object into the inner (InMemory) store.
        let path: OsPath = "data/file.parquet".into();
        let payload = Bytes::from_static(b"parquet-bytes-for-testing");
        counting
            .put_opts(
                &path,
                PutPayload::from(payload.clone()),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let caching = make_caching(counting.clone()).await;
        let stats = caching.stats();

        // First get — MISS; inner store gets called once.
        let result1 = caching.get(&path).await.unwrap();
        let bytes1 = result1.bytes().await.unwrap();
        assert_eq!(bytes1, payload, "first get: bytes must match");

        // Second get — HIT; inner store must NOT be called again.
        let result2 = caching.get(&path).await.unwrap();
        let bytes2 = result2.bytes().await.unwrap();
        assert_eq!(bytes2, payload, "second get: bytes must match");

        assert_eq!(
            counting.gets(),
            1,
            "inner store should have seen exactly 1 get (second served from foyer)"
        );
        assert_eq!(stats.hits(), 1, "cache: 1 hit");
        assert_eq!(stats.misses(), 1, "cache: 1 miss");
    }

    // -----------------------------------------------------------------------
    // Test 2: `get_range` cached by range; inner store sees only ONE range call.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_range_second_call_served_from_cache() {
        let counting = CountingStore::new();

        let path: OsPath = "data/columns.parquet".into();
        let content = Bytes::from(vec![0u8; 1024]);
        counting
            .put_opts(
                &path,
                PutPayload::from(content.clone()),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let caching = make_caching(counting.clone()).await;
        let stats = caching.stats();

        let range = 100u64..200u64;

        // First range get — MISS.
        let slice1 = caching.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(
            slice1,
            content.slice(100..200),
            "first range: bytes must match"
        );

        // Second range get — HIT; no new inner call.
        let slice2 = caching.get_range(&path, range.clone()).await.unwrap();
        assert_eq!(
            slice2,
            content.slice(100..200),
            "second range: bytes must match"
        );

        assert_eq!(
            counting.get_ranges(),
            1,
            "inner store should have seen exactly 1 get_range"
        );
        assert_eq!(stats.hits(), 1, "cache: 1 hit");
        assert_eq!(stats.misses(), 1, "cache: 1 miss");
    }

    // -----------------------------------------------------------------------
    // Test 3: distinct keys miss then hit; hit/miss counters are correct
    //         across multiple keys and a mix of get/get_range.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn distinct_keys_miss_then_hit_counters_correct() {
        let counting = CountingStore::new();

        let path_a: OsPath = "meta/a.json".into();
        let path_b: OsPath = "data/b.parquet".into();
        let bytes_a = Bytes::from_static(b"aaaa");
        let bytes_b = Bytes::from(vec![0xbb_u8; 512]);

        counting
            .put_opts(
                &path_a,
                PutPayload::from(bytes_a.clone()),
                PutOptions::default(),
            )
            .await
            .unwrap();
        counting
            .put_opts(
                &path_b,
                PutPayload::from(bytes_b.clone()),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let caching = make_caching(counting.clone()).await;
        let stats = caching.stats();

        // --- misses: two distinct whole-object gets -------------------------
        let r_a1 = caching.get(&path_a).await.unwrap().bytes().await.unwrap();
        assert_eq!(r_a1, bytes_a);
        let r_b1 = caching.get(&path_b).await.unwrap().bytes().await.unwrap();
        assert_eq!(r_b1, bytes_b);

        assert_eq!(counting.gets(), 2, "2 misses → 2 inner gets");
        assert_eq!(stats.misses(), 2, "2 misses recorded");
        assert_eq!(stats.hits(), 0, "no hits yet");

        // --- hits: same keys again -----------------------------------------
        let r_a2 = caching.get(&path_a).await.unwrap().bytes().await.unwrap();
        assert_eq!(r_a2, bytes_a);
        let r_b2 = caching.get(&path_b).await.unwrap().bytes().await.unwrap();
        assert_eq!(r_b2, bytes_b);

        assert_eq!(counting.gets(), 2, "inner still 2: hits served from foyer");
        assert_eq!(stats.hits(), 2, "2 hits");
        assert_eq!(stats.misses(), 2, "misses unchanged");

        // --- a range on path_b: distinct cache key → miss + hit -------------
        let range = 0u64..16u64;
        let rb_range1 = caching.get_range(&path_b, range.clone()).await.unwrap();
        assert_eq!(rb_range1, bytes_b.slice(0..16));
        assert_eq!(counting.get_ranges(), 1, "1 inner get_range (miss)");
        assert_eq!(stats.misses(), 3, "3rd miss");

        let rb_range2 = caching.get_range(&path_b, range.clone()).await.unwrap();
        assert_eq!(rb_range2, bytes_b.slice(0..16));
        assert_eq!(counting.get_ranges(), 1, "still 1 inner get_range (hit)");
        assert_eq!(stats.hits(), 3, "3rd hit");
    }
}
