// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Adapted for bluedb: this is a dependency-free replacement for
// `quickwit-storage/src/metrics.rs`. The upstream file wires Prometheus
// counters/gauges through `quickwit-config` + `quickwit-metrics`. The byte
// range cache only ever calls `.inc()`, `.inc_by()`, `.dec()`, `.dec_by()` and
// `.get()` on those handles, so we back them with plain `AtomicU64` counters
// that expose the exact field/method names `ByteRangeCache` uses. No metrics
// are actually exported — they are kept purely so the cache code compiles and
// runs unchanged.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

/// Monotonic counter. Mirrors the subset of `quickwit_metrics::Counter` used by
/// the byte range cache.
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_by(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
}

/// Up/down gauge. Mirrors the subset of `quickwit_metrics::Gauge` used by the
/// byte range cache. Values are tracked as `f64` (matching the upstream API)
/// but stored in the bit pattern of an `AtomicU64` so the gauge stays lock-free
/// and `Sync`.
#[derive(Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    #[inline]
    fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }

    #[inline]
    fn add(&self, delta: f64) {
        // Relaxed read-modify-write loop; metrics need not be perfectly
        // consistent under contention.
        let mut current = self.0.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(current) + delta).to_bits();
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub fn inc(&self) {
        self.add(1.0);
    }

    #[inline]
    pub fn dec(&self) {
        self.add(-1.0);
    }

    #[inline]
    pub fn inc_by(&self, delta: f64) {
        self.add(delta);
    }

    #[inline]
    pub fn dec_by(&self, delta: f64) {
        self.add(-delta);
    }

    #[inline]
    pub fn get(&self) -> f64 {
        self.load()
    }
}

/// Per-cache counters and gauges tracking items in cache, hits, misses, and
/// evictions. Field names match `quickwit_storage::metrics::SingleCacheMetrics`.
#[derive(Default)]
pub struct SingleCacheMetrics {
    /// Current number of items stored in the cache.
    pub(crate) in_cache_count: Gauge,
    /// Current number of bytes stored in the cache.
    pub(crate) in_cache_num_bytes: Gauge,
    /// Total number of cache hits (items).
    pub(crate) hits_num_items: Counter,
    /// Total number of cache hit bytes.
    pub(crate) hits_num_bytes: Counter,
    /// Total number of cache misses (items).
    pub(crate) misses_num_items: Counter,
    /// Total number of evicted items.
    pub(crate) evict_num_items: Counter,
    /// Total number of evicted bytes.
    pub(crate) evict_num_bytes: Counter,
}

/// Metrics for a named cache component. Field/method names match
/// `quickwit_storage::metrics::CacheMetrics`.
pub struct CacheMetrics {
    #[allow(dead_code)]
    component_name: String,
    pub(crate) cache_metrics: SingleCacheMetrics,
}

impl CacheMetrics {
    /// Creates a new `CacheMetrics` for the given component name.
    pub fn for_component(component_name: &str) -> Self {
        CacheMetrics {
            component_name: component_name.to_string(),
            cache_metrics: SingleCacheMetrics::default(),
        }
    }
}

/// Cache metrics for short-lived byte range caches (used during caching
/// directory warmup). Referenced by `CachingDirectory::new_unbounded`.
pub static SHORTLIVED_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("shortlived"));

// Adapted for bluedb: removed the test-only `CACHE_METRICS_FOR_TESTS` static.
