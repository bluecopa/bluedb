//! Seal-latency harness against **real GCS** — the make-or-break measurement for
//! the unified-query (HTAP) design, whose read-your-writes latency *is* the
//! mirror's seal latency. The design needs sub-second seals; this measures
//! whether real GCS delivers that.
//!
//! It is `#[ignore]`d and env-gated (no creds committed), mirroring the
//! `gcs_real_round_trip` pattern in `crates/bluedb-server/tests/objstore_emulators.rs`.
//! Run it (writes to a real, authorized bucket — use a UNIQUE throwaway prefix so
//! cleanup is one recursive delete):
//!
//! ```text
//! BLUEDB_GCS_TEST_BUCKET=copa-assistant.firebasestorage.app \
//! BLUEDB_GCS_TEST_SA=/path/to/sa.json \
//! BLUEDB_GCS_TEST_PREFIX=bluedb-seal-latency-spike/run-$(date +%s) \
//!   cargo test -p bluedb-lakehouse --test seal_latency_gcs -- --ignored --nocapture 2>&1
//! ```
//!
//! Both the SlateDB data and the Iceberg tables live under the one prefix; delete
//! `gs://$BUCKET/$PREFIX` afterward.
//!
//! ## What it measures (two numbers)
//!
//! 1. **Seal-op floor** — wall time of ONE bare `engine.seal().await` (the GCS
//!    round-trips to write the data file, manifest, manifest-list, metadata.json,
//!    version-hint, plus the CDC scan/GC). The insert is excluded. Repeated, and
//!    reported as min/median/max.
//! 2. **End-to-end ack→seal** — under the real event-driven seal loop
//!    (`LakehouseEngine::spawn_seal_loop`, the same debounce code as
//!    `manager.rs`), time from an INSERT being acked to that row's CDC seq landing
//!    in a sealed Iceberg snapshot's `bluedb.cdc_watermark`. The loop is
//!    instrumented to record `(watermark, Instant)` after each `seal()` into a
//!    shared buffer; each insert is matched offline to the first sealed event whose
//!    watermark ≥ its seq — so the hot path takes NO extra GCS metadata reads.
//!    Run at two cadences: a fast target (50ms / 500ms) and the production default.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bluedb_lakehouse::{object_store_file_io, LakehouseConfig, LakehouseEngine};
use bluedb_sql::{CdcConfig, Database};
use gluesql_core::prelude::Glue;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use slatedb::Db;
use tokio::time::Instant;

/// One `seal()` completion: the watermark the mirror now durably reflects and the
/// instant the seal returned. Recorded by the instrumented loop, matched offline.
type SealEvent = (i64, Instant);

/// Read the env-gated config, or `None` (test skips) when unset.
fn gcs_env() -> Option<(String, String, String)> {
    Some((
        std::env::var("BLUEDB_GCS_TEST_BUCKET").ok()?,
        std::env::var("BLUEDB_GCS_TEST_SA").ok()?,
        std::env::var("BLUEDB_GCS_TEST_PREFIX").ok()?,
    ))
}

/// Build the real-GCS object store shared by SlateDB and the Iceberg mirror.
fn gcs_store(bucket: &str, sa: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        GoogleCloudStorageBuilder::new()
            .with_bucket_name(bucket.to_string())
            .with_service_account_path(sa.to_string())
            .build()
            .expect("build GCS object store"),
    )
}

/// Stand up a fresh engine + db over real GCS under `<prefix>` (SlateDB at
/// `<prefix>/slatedb`, Iceberg at `<prefix>/lakehouse`), with table `t` mirrored
/// and created. Returns `(db, cdc, engine)`.
async fn setup(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> (Database, CdcConfig, Arc<LakehouseEngine>) {
    let db = Database::new(Arc::new(
        Db::open(format!("{prefix}/slatedb"), store.clone())
            .await
            .expect("open slatedb on gcs"),
    ));
    let file_io = object_store_file_io(store.clone(), "");
    let cdc = CdcConfig::default();
    let engine = Arc::new(
        LakehouseEngine::reopen(file_io, format!("{prefix}/lakehouse"), "_", db.clone(), cdc.clone())
            .await
            .expect("reopen lakehouse engine on gcs"),
    );
    engine.enable_table("t").await.expect("enable table t");
    {
        let mut g = Glue::new(db.connection_serialized());
        g.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .expect("create table t");
    }
    (db, cdc, engine)
}

/// Insert one row with CDC on (so the commit signals the seal loop), returning the
/// wall time the INSERT took.
async fn insert_one(db: &Database, cdc: &CdcConfig, id: i64) -> Duration {
    let mut g = Glue::new(db.connection_with_cdc(cdc.clone()));
    let t0 = Instant::now();
    g.execute(&format!("INSERT INTO t VALUES ({id}, 'v{id}');"))
        .await
        .expect("insert");
    t0.elapsed()
}

/// The current max CDC seq for the default tenant (the seq just assigned to the
/// latest serial insert). Read AFTER `t_acked` is stamped, so it never inflates
/// the ack→seal interval.
async fn current_seq(db: &Database) -> i64 {
    db.scan_cdc("_", 0)
        .await
        .expect("scan_cdc")
        .iter()
        .map(|(seq, _)| *seq)
        .max()
        .unwrap_or(0)
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// min / median / max of a slice of millisecond samples.
fn min_med_max(samples: &mut [f64]) -> (f64, f64, f64) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples.len();
    (samples[0], samples[n / 2], samples[n - 1])
}

/// The value at percentile `p` (0..=100) of an already-collected sample set
/// (nearest-rank). `samples` is sorted in place.
fn pct(samples: &mut [f64], p: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples.len();
    let rank = ((p / 100.0) * n as f64).ceil() as usize;
    samples[rank.saturating_sub(1).min(n - 1)]
}

/// **Scenario 1 — seal-op floor.** Insert one row, then time a bare
/// `engine.seal()` (insert excluded). Repeat `reps` times; report min/median/max.
async fn measure_seal_floor(db: &Database, cdc: &CdcConfig, engine: &LakehouseEngine, reps: usize) {
    let mut samples = Vec::with_capacity(reps);
    for i in 0..reps {
        // A unique id each round so every seal has exactly one row to publish.
        insert_one(db, cdc, 1_000 + i as i64).await;
        let t0 = Instant::now();
        engine.seal().await.expect("seal");
        samples.push(ms(t0.elapsed()));
    }
    let (mn, md, mx) = min_med_max(&mut samples);
    println!(
        "\n[scenario 1] SEAL-OP FLOOR (one bare engine.seal(), insert excluded), n={reps}\n  \
         min={mn:.1}ms  median={md:.1}ms  max={mx:.1}ms\n  \
         samples(ms)={:?}",
        samples.iter().map(|v| (v * 10.0).round() / 10.0).collect::<Vec<_>>()
    );
}

/// **Scenario 2 — end-to-end ack→seal under the event-driven loop.** Spawns the
/// real `spawn_seal_loop` at the given cadence, instrumented to push a
/// `(watermark, Instant)` SealEvent after every `seal()`. Bursts `n` serial
/// single-row inserts, recording each one's `(seq, t_acked)`; matches each offline
/// to the first SealEvent with `watermark >= seq`. Reports the ack→seal
/// distribution and the observed effective seal cadence.
async fn measure_ack_to_seal(
    db: &Database,
    cdc: &CdcConfig,
    engine: Arc<LakehouseEngine>,
    label: &str,
    cfg: LakehouseConfig,
    n: usize,
    id_base: i64,
) {
    // Shared buffer the instrumented loop appends to after each seal.
    let events: Arc<Mutex<Vec<SealEvent>>> = Arc::new(Mutex::new(Vec::new()));

    // Replicate manager.rs:240-256's debounce loop verbatim, recording the
    // watermark + completion instant after each seal (instead of `seal_all`).
    let loop_handle = {
        let engine = engine.clone();
        let cdc = cdc.clone();
        let events = events.clone();
        tokio::spawn(async move {
            loop {
                cdc.wait_for_changes().await;
                let deadline = Instant::now() + cfg.seal_max_interval;
                loop {
                    tokio::select! {
                        _ = cdc.wait_for_changes() => {
                            if Instant::now() >= deadline { break; }
                        }
                        _ = tokio::time::sleep(cfg.seal_debounce) => break,
                    }
                }
                if let Err(err) = engine.seal().await {
                    eprintln!("seal loop: {err}");
                    continue;
                }
                // Read back the watermark the just-published snapshot reflects.
                let wm = match engine.fetch_schema("t").await {
                    Ok(schema) => match engine.writer_for("t", &schema, &[]).await {
                        Ok(w) => w.current_watermark().unwrap_or(0),
                        Err(_) => 0,
                    },
                    Err(_) => 0,
                };
                events.lock().unwrap().push((wm, Instant::now()));
            }
        })
    };

    // Burst: serial single-row inserts. For each, stamp t_acked the instant the
    // INSERT returns, THEN read its seq (the seq read is after the stamp, so it
    // can't inflate ack→seal).
    let mut inserts: Vec<(i64, Instant)> = Vec::with_capacity(n);
    let burst_start = Instant::now();
    for i in 0..n {
        insert_one(db, cdc, id_base + i as i64).await;
        let t_acked = Instant::now();
        let seq = current_seq(db).await;
        inserts.push((seq, t_acked));
    }
    let burst_dur = burst_start.elapsed();

    // Wait until the last insert's seq is sealed (bounded), so every insert has a
    // matching SealEvent. Poll the shared buffer (no GCS reads) — the loop already
    // did the metadata read once per seal.
    let last_seq = inserts.iter().map(|(s, _)| *s).max().unwrap_or(0);
    let wait_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let sealed_max = events.lock().unwrap().iter().map(|(w, _)| *w).max().unwrap_or(0);
        if sealed_max >= last_seq || Instant::now() >= wait_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    loop_handle.abort();

    let seal_events = events.lock().unwrap().clone();
    // Effective cadence: gaps between consecutive seals that actually advanced the
    // watermark (a no-op seal that didn't advance is dropped from the cadence).
    let mut advancing: Vec<Instant> = Vec::new();
    let mut prev_wm = -1;
    for (wm, t) in &seal_events {
        if *wm > prev_wm {
            advancing.push(*t);
            prev_wm = *wm;
        }
    }
    let mut cadence_ms: Vec<f64> = advancing.windows(2).map(|w| ms(w[1] - w[0])).collect();

    // Match each insert to the first SealEvent whose watermark >= its seq.
    let mut ack_to_seal: Vec<f64> = Vec::with_capacity(n);
    let mut unmatched = 0;
    for (seq, t_acked) in &inserts {
        match seal_events
            .iter()
            .filter(|(wm, _)| *wm >= *seq)
            .map(|(_, t)| *t)
            .min()
        {
            Some(t_sealed) if t_sealed >= *t_acked => ack_to_seal.push(ms(t_sealed - *t_acked)),
            // Sealed before we stamped t_acked (shouldn't happen serially) → 0.
            Some(_) => ack_to_seal.push(0.0),
            None => unmatched += 1,
        }
    }

    println!("\n[scenario 2 — {label}] END-TO-END ack->seal under the event-driven loop");
    println!(
        "  cadence cfg: seal_debounce={:?}, seal_max_interval={:?}",
        cfg.seal_debounce, cfg.seal_max_interval
    );
    println!(
        "  inserts={n} (serial), burst wall={:.0}ms ({:.1} insert/s), sealed snapshots={}, advancing seals={}, unmatched={unmatched}",
        ms(burst_dur),
        n as f64 / burst_dur.as_secs_f64(),
        seal_events.len(),
        advancing.len(),
    );
    if !ack_to_seal.is_empty() {
        let n_ok = ack_to_seal.len();
        let p50 = pct(&mut ack_to_seal, 50.0);
        let p95 = pct(&mut ack_to_seal, 95.0);
        let p99 = pct(&mut ack_to_seal, 99.0);
        let (mn, _, mx) = min_med_max(&mut ack_to_seal);
        println!(
            "  ack->seal (ms), matched={n_ok}: p50={p50:.0}  p95={p95:.0}  p99={p99:.0}  min={mn:.0}  max={mx:.0}"
        );
    }
    if !cadence_ms.is_empty() {
        let (mn, md, mx) = min_med_max(&mut cadence_ms);
        println!(
            "  effective seal cadence between watermark-advancing seals (ms): min={mn:.0}  median={md:.0}  max={mx:.0}"
        );
    }
}

/// **Diagnostic** — localize where a seal's wall time goes. Measures raw
/// object_store op latency (PUT / GET / HEAD / LIST of a small blob) against the
/// bucket, then a fully-warm single `engine.seal()` broken into its phases. Run:
///
/// ```text
/// BLUEDB_GCS_TEST_{BUCKET,SA,PREFIX}=… cargo test -p bluedb-lakehouse \
///   --test seal_latency_gcs gcs_op_and_seal_phase_breakdown -- --ignored --nocapture 2>&1
/// ```
#[tokio::test]
#[ignore = "real GCS — set BLUEDB_GCS_TEST_{BUCKET,SA,PREFIX}"]
async fn gcs_op_and_seal_phase_breakdown() {
    use futures::StreamExt;
    use object_store::path::Path as OsPath;

    let Some((bucket, sa, prefix)) = gcs_env() else {
        eprintln!("skipping: set BLUEDB_GCS_TEST_{{BUCKET,SA,PREFIX}}");
        return;
    };
    println!("\n=== raw GCS op latency + seal phase breakdown ===");
    let store = gcs_store(&bucket, &sa);

    // --- Raw op latency: a tiny blob, PUT/GET/HEAD/LIST a few times. ---
    let key = OsPath::from(format!("{prefix}/probe/blob.bin"));
    let payload = bytes::Bytes::from(vec![7u8; 4096]);
    let mut put = Vec::new();
    let mut get = Vec::new();
    let mut head = Vec::new();
    let mut list = Vec::new();
    for _ in 0..5 {
        let t = Instant::now();
        store.put(&key, payload.clone().into()).await.expect("put");
        put.push(ms(t.elapsed()));
        let t = Instant::now();
        store.get(&key).await.expect("get").bytes().await.expect("bytes");
        get.push(ms(t.elapsed()));
        let t = Instant::now();
        store.head(&key).await.expect("head");
        head.push(ms(t.elapsed()));
        let t = Instant::now();
        let _ = store
            .list(Some(&OsPath::from(format!("{prefix}/probe"))))
            .collect::<Vec<_>>()
            .await;
        list.push(ms(t.elapsed()));
    }
    for (name, mut s) in [("PUT", put), ("GET", get), ("HEAD", head), ("LIST", list)] {
        let (mn, md, mx) = min_med_max(&mut s);
        println!("  raw {name}: min={mn:.0}ms median={md:.0}ms max={mx:.0}ms  samples={s:?}");
    }

    // --- Seal phase breakdown: one warm seal, timed by phase. ---
    let (db, cdc, engine) = setup(store.clone(), &format!("{prefix}/phases")).await;
    insert_one(&db, &cdc, 1).await; // one row to seal
    engine.seal().await.expect("warm-up seal (table now exists)");
    insert_one(&db, &cdc, 2).await; // a second row for the timed seal

    let t = Instant::now();
    let entries = db.scan_cdc("_", 0).await.expect("scan_cdc");
    let t_scan = ms(t.elapsed());
    let watermark = entries.iter().map(|(s, _)| *s).max().unwrap_or(0);

    let t = Instant::now();
    let schema = engine.fetch_schema("t").await.expect("schema");
    let t_schema = ms(t.elapsed());

    let t = Instant::now();
    let mut writer = engine.writer_for("t", &schema, &[]).await.expect("writer_for");
    let t_writer_open = ms(t.elapsed());

    let row = (
        gluesql_core::data::Key::I32(2),
        gluesql_core::store::DataRow::Vec(vec![
            gluesql_core::data::Value::I32(2),
            gluesql_core::data::Value::Str("v2".into()),
        ]),
    );
    let t = Instant::now();
    writer.upsert(&[row]).await.expect("upsert");
    let t_upsert = ms(t.elapsed()); // writes data parquet + stages delete parquet

    let t = Instant::now();
    writer.commit_snapshot(watermark).await.expect("commit");
    let t_commit = ms(t.elapsed()); // manifests + manifest-list + metadata.json + version-hint

    let t = Instant::now();
    db.gc_cdc("_", watermark).await.expect("gc_cdc");
    let t_gc = ms(t.elapsed());

    println!("\n  seal phase breakdown (one warm seal, ms):");
    println!("    scan_cdc (SlateDB read)        = {t_scan:.0}");
    println!("    fetch_schema (SlateDB read)    = {t_schema:.0}");
    println!("    writer_for/open (GCS head+get) = {t_writer_open:.0}");
    println!("    upsert (write data+delete pq)  = {t_upsert:.0}");
    println!("    commit_snapshot (5 GCS puts)   = {t_commit:.0}");
    println!("    gc_cdc (SlateDB write)         = {t_gc:.0}");
    println!(
        "    TOTAL                          = {:.0}",
        t_scan + t_schema + t_writer_open + t_upsert + t_commit + t_gc
    );
    db.flush().await.ok();
    println!("\n=== diagnostic done — delete gs://{bucket}/{prefix} ===\n");
}

/// The production default cadence the **server** ships when no env override is
/// set: `BLUEDB_LAKEHOUSE_SEAL_DEBOUNCE_MS` default 2 s,
/// `BLUEDB_LAKEHOUSE_SEAL_MAX_INTERVAL_MS` default 10 s (see
/// `bluedb-server/src/lib.rs`). A 2 s debounce means a row waits up to ~2 s of
/// coalescing before its seal even starts — on top of the seal-op cost. This is
/// the "seconds-fresh mirror" the server is tuned for, not a read-your-writes
/// latency target.
fn default_cfg() -> LakehouseConfig {
    LakehouseConfig {
        seal_debounce: Duration::from_secs(2),
        seal_max_interval: Duration::from_secs(10),
        compaction_interval: Duration::from_secs(60),
        max_data_files: 64,
        max_delete_files: 16,
    }
}

fn fast_cfg() -> LakehouseConfig {
    LakehouseConfig {
        seal_debounce: Duration::from_millis(50),
        seal_max_interval: Duration::from_millis(500),
        compaction_interval: Duration::from_secs(60),
        max_data_files: 64,
        max_delete_files: 16,
    }
}

#[tokio::test]
#[ignore = "real GCS — set BLUEDB_GCS_TEST_{BUCKET,SA,PREFIX}"]
async fn seal_latency_against_real_gcs() {
    let Some((bucket, sa, prefix)) = gcs_env() else {
        eprintln!("skipping: set BLUEDB_GCS_TEST_{{BUCKET,SA,PREFIX}}");
        return;
    };
    println!("\n=== bluedb lakehouse seal-latency vs REAL GCS ===");
    println!("bucket={bucket}  prefix={prefix}");
    let store = gcs_store(&bucket, &sa);

    // --- Scenario 1: seal-op floor (own engine/prefix). ---
    {
        let (db, cdc, engine) = setup(store.clone(), &format!("{prefix}/floor")).await;
        measure_seal_floor(&db, &cdc, &engine, 5).await;
        db.flush().await.ok();
    }

    // --- Scenario 2a: fast cadence (50ms / 500ms). ---
    {
        let (db, cdc, engine) = setup(store.clone(), &format!("{prefix}/e2e-fast")).await;
        measure_ack_to_seal(&db, &cdc, engine, "fast 50ms/500ms", fast_cfg(), 50, 1).await;
        db.flush().await.ok();
    }

    // --- Scenario 2b: default cadence. ---
    {
        let (db, cdc, engine) = setup(store.clone(), &format!("{prefix}/e2e-default")).await;
        measure_ack_to_seal(&db, &cdc, engine, "default 250ms/2s", default_cfg(), 50, 1).await;
        db.flush().await.ok();
    }

    println!("\n=== done — remember to delete gs://{bucket}/{prefix} ===\n");
}

/// Stat helpers are pure — guard the percentile math so a refactor can't silently
/// skew the reported numbers.
#[cfg(test)]
mod stat_tests {
    use super::{min_med_max, pct};

    #[test]
    fn percentiles_nearest_rank() {
        let mut s: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(pct(&mut s, 50.0), 50.0);
        assert_eq!(pct(&mut s, 95.0), 95.0);
        assert_eq!(pct(&mut s, 99.0), 99.0);
        assert_eq!(pct(&mut s, 100.0), 100.0);
    }

    #[test]
    fn min_med_max_basic() {
        let mut s = vec![5.0, 1.0, 3.0];
        assert_eq!(min_med_max(&mut s), (1.0, 3.0, 5.0));
    }
}
