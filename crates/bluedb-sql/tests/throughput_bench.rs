//! Write-throughput + latency bench for the autocommit **group-commit** path.
//!
//! Not part of the normal test run (each case is `#[ignore]`). Run explicitly:
//!
//! ```text
//! cargo test --release -p bluedb-sql --test throughput_bench -- --ignored --nocapture
//! ```
//!
//! Two backends:
//! - **memory** (`InMemory`) — no object-store PUT, so it isolates SlateDB's WAL
//!   `flush_interval` + group-commit coalescing. The *optimistic ceiling*.
//! - **local-disk** (`LocalFileSystem` in a temp dir) — adds a real filesystem
//!   PUT per flush. Still faster than networked object storage (S3/GCS/Azure add
//!   the wire round-trip on top), so treat these as a local lower-bound on
//!   latency, not the cloud number.
//!
//! Drives concurrent single-row autocommit `INSERT`s through one shared
//! [`Database`] (shared `SeqAllocator`), which is exactly the group-commit path
//! the HTTP write route exercises — throughput rises with concurrency as the WAL
//! coalesces concurrent writers into one flush.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bluedb_sql::Database;
use futures::future::join_all;
use gluesql_core::prelude::Glue;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, Settings};

const RUN_SECS: u64 = 3;

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    LocalDisk,
}

impl Backend {
    fn label(self) -> &'static str {
        match self {
            Backend::Memory => "memory (ceiling)",
            Backend::LocalDisk => "local-disk",
        }
    }
}

/// Unique temp-dir suffix per local-disk `Db` so concurrent/looped opens don't
/// collide (no `rand`/clock available in this harness).
static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// Open a fresh SlateDB on `backend`. `flush_ms = None` uses the crate default
/// (100ms); `Some(ms)` overrides `flush_interval` via the builder.
async fn open_db(backend: Backend, flush_ms: Option<u64>) -> Arc<Db> {
    let store: Arc<dyn ObjectStore> = match backend {
        Backend::Memory => Arc::new(InMemory::new()),
        Backend::LocalDisk => {
            // PID + seq keeps each Db's dir unique *across* runs too (a bare seq
            // restarts at 0 each process and would reopen a prior run's tables).
            let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("bluedb-bench-{}-{seq}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create bench dir");
            Arc::new(LocalFileSystem::new_with_prefix(&dir).expect("local fs"))
        }
    };
    let db = match flush_ms {
        None => Db::open("bench", store).await.expect("open"),
        Some(ms) => {
            let mut settings = Settings::default();
            settings.flush_interval = Some(Duration::from_millis(ms));
            Db::builder("bench", store)
                .with_settings(settings)
                .build()
                .await
                .expect("open w/ settings")
        }
    };
    Arc::new(db)
}

/// One concurrency level's result.
struct LoadResult {
    per_sec: f64,
    p50: Duration,
    p99: Duration,
    p999: Duration,
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Drive `concurrency` connections doing single-row autocommit INSERTs for
/// `RUN_SECS`. All share one `Database`, so they group-commit. Records per-insert
/// latency and returns throughput + percentiles.
async fn run_load(db: Arc<Db>, concurrency: usize) -> LoadResult {
    let database = Arc::new(Database::new(db));
    {
        let mut glue = Glue::new(database.connection());
        glue.execute("CREATE TABLE t (id INTEGER, body TEXT);")
            .await
            .expect("create table");
    }

    let start = Instant::now();
    let deadline = start + Duration::from_secs(RUN_SECS);

    let workers = (0..concurrency).map(|w| {
        let database = database.clone();
        async move {
            let mut glue = Glue::new(database.connection());
            let mut lats: Vec<Duration> = Vec::new();
            let mut n: u64 = 0;
            while Instant::now() < deadline {
                let id = (w as u64) * 1_000_000_000 + n;
                let sql = format!(
                    "INSERT INTO t (id, body) VALUES ({id}, 'lorem ipsum dolor sit amet');"
                );
                let t0 = Instant::now();
                glue.execute(&sql).await.expect("insert");
                lats.push(t0.elapsed());
                n += 1;
            }
            lats
        }
    });

    let mut all: Vec<Duration> = join_all(workers).await.into_iter().flatten().collect();
    let elapsed = start.elapsed().as_secs_f64();
    let total = all.len() as f64;
    all.sort_unstable();
    LoadResult {
        per_sec: total / elapsed,
        p50: percentile(&all, 0.50),
        p99: percentile(&all, 0.99),
        p999: percentile(&all, 0.999),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput bench; run with --ignored --nocapture"]
async fn concurrency_sweep() {
    // bluedb-server ships `flush_interval = 25ms` (DEFAULT_FLUSH_INTERVAL_MS),
    // overriding SlateDB's 100ms — so the sweep runs at 25ms to mirror the wire.
    for backend in [Backend::Memory, Backend::LocalDisk] {
        println!(
            "\n=== concurrency sweep · {} · flush_interval=25ms (bluedb default) · await_durable=true ===",
            backend.label()
        );
        println!(
            "{:>12} | {:>12} | {:>9} | {:>9} | {:>9}",
            "concurrency", "inserts/sec", "p50", "p99", "p99.9"
        );
        println!(
            "{:-<12}-+-{:-<12}-+-{:-<9}-+-{:-<9}-+-{:-<9}",
            "", "", "", "", ""
        );
        for c in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
            let db = open_db(backend, Some(25)).await;
            let r = run_load(db, c).await;
            println!(
                "{c:>12} | {:>12.1} | {:>8.2?} | {:>8.2?} | {:>8.2?}",
                r.per_sec, r.p50, r.p99, r.p999
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput bench; run with --ignored --nocapture"]
async fn flush_interval_sweep() {
    let concurrency = 32usize;
    println!(
        "\n=== flush_interval sweep · local-disk · concurrency={concurrency} · await_durable=true ===\n\
         (shorter interval = higher throughput, at the cost of more object-store PUTs)"
    );
    println!(
        "{:>16} | {:>12} | {:>9} | {:>9}",
        "flush_interval", "inserts/sec", "p50", "p99"
    );
    println!("{:-<16}-+-{:-<12}-+-{:-<9}-+-{:-<9}", "", "", "", "");
    for ms in [100u64, 50, 25, 10] {
        let db = open_db(Backend::LocalDisk, Some(ms)).await;
        let r = run_load(db, concurrency).await;
        println!(
            "{:>14}ms | {:>12.1} | {:>8.2?} | {:>8.2?}",
            ms, r.per_sec, r.p50, r.p99
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput bench; run with --ignored --nocapture"]
async fn durability_microbench() {
    // SlateDB-layer single serial writer: the relaxed-durability ceiling vs the
    // strong-durability floor bluedb's SQL path actually uses (await_durable=true).
    const N: u64 = 300;
    println!("\n=== durability micro-bench · local-disk · single serial writer · flush_interval=100ms ===");
    println!("{:>22} | {:>12} | {:>16}", "mode", "ops/sec", "avg latency");
    println!("{:-<22}-+-{:-<12}-+-{:-<16}", "", "", "");
    for (label, await_durable) in [("await_durable=true", true), ("await_durable=false", false)] {
        let db = open_db(Backend::LocalDisk, None).await;
        let put = PutOptions::default();
        let mut wo = WriteOptions::default();
        wo.await_durable = await_durable;

        let start = Instant::now();
        for i in 0..N {
            let key = format!("k{i:08}");
            db.put_with_options(key.as_bytes(), b"lorem ipsum dolor sit amet", &put, &wo)
                .await
                .expect("put");
        }
        let elapsed = start.elapsed();
        println!(
            "{label:>22} | {:>12.1} | {:>13.2?}",
            N as f64 / elapsed.as_secs_f64(),
            elapsed / N as u32
        );
        if !await_durable {
            db.flush().await.expect("flush");
        }
    }
}
