//! Live round-trip tests against the three object-store emulators.
//!
//! Ignored by default — they need the emulator stack running:
//!
//! ```text
//! docker compose -f crates/bluedb-server/tests/emulators/docker-compose.yml up -d
//! cargo test -p bluedb-server --test objstore_emulators -- --ignored --nocapture
//! docker compose -f crates/bluedb-server/tests/emulators/docker-compose.yml down -v
//! ```
//!
//! Each test proves bluedb's actual substrate — a SlateDB `Db` — can open on the
//! backend, write durably, reopen, and read the value back. That exercises the
//! real read/write/list/conditional-put paths, not just client construction, so
//! a pass means the cloud genuinely works end-to-end through
//! [`bluedb_server::objstore::build_object_store`].

use std::sync::Arc;

use bluedb_server::objstore::{build_object_store, ObjectStoreConfig};
use object_store::ObjectStore;
use slatedb::Db;

/// Well-known Azurite emulator account key (valid base64 shared key).
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Open a fresh SlateDB at `prefix` on `store`, write a key durably, then reopen
/// a new handle and read it back — proving the bytes round-tripped through the
/// object store (not just an in-process memtable).
async fn slatedb_round_trip(store: Arc<dyn ObjectStore>, prefix: &str) {
    let db = Db::open(prefix, store.clone()).await.expect("open db");
    db.put(b"emul-key", b"emul-value").await.expect("put");
    db.flush().await.expect("flush");
    db.close().await.expect("close");

    let db2 = Db::open(prefix, store).await.expect("reopen db");
    let got = db2.get(b"emul-key").await.expect("get");
    assert_eq!(
        got.as_deref(),
        Some(&b"emul-value"[..]),
        "value must survive a close + reopen via the object store"
    );
    db2.close().await.expect("close2");
}

#[tokio::test]
#[ignore = "needs the emulator stack (see module docs)"]
async fn s3_minio_round_trip() {
    let cfg = ObjectStoreConfig::S3 {
        bucket: "bluedb".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://localhost:9000".into()),
        access_key_id: Some("minioadmin".into()),
        secret_access_key: Some("minioadmin".into()),
    };
    let store = build_object_store(&cfg).expect("build s3 store");
    slatedb_round_trip(store, "emul-roundtrip-s3").await;
}

#[tokio::test]
#[ignore = "needs the emulator stack (see module docs)"]
async fn azure_azurite_round_trip() {
    let cfg = ObjectStoreConfig::Azure {
        container: "bluedb".into(),
        account: Some("devstoreaccount1".into()),
        access_key: Some(AZURITE_KEY.into()),
        endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
    };
    let store = build_object_store(&cfg).expect("build azure store");
    slatedb_round_trip(store, "emul-roundtrip-azure").await;
}

/// GCS round-trip against `fake-gcs-server`.
///
/// NOTE: `fake-gcs-server` does not faithfully implement the operations SlateDB
/// performs (notably the conditional/manifest writes), so this round-trip
/// **hangs** against it — object_store retries the unsupported response with
/// backoff. This is an emulator-fidelity limitation, not a bluedb bug: the GCS
/// bridge ([`build_object_store`]) uses the same `object_store` GCS client that
/// real GCS serves, and the S3 (MinIO) and Azure (Azurite) round-trips above
/// exercise the identical bridge end-to-end. The emulator endpoint is supplied
/// via the service-account JSON's `gcs_base_url` + `disable_oauth`
/// (`tests/emulators/gcs-fake-sa.json`) — object_store's idiomatic GCS-emulator
/// hook — and the `bluedb` bucket is created by the compose `fake-gcs-init`.
/// Re-enable once a higher-fidelity GCS emulator (or real GCS) is wired up.
#[tokio::test]
#[ignore = "fake-gcs-server can't service SlateDB's round-trip (hangs); see fn docs"]
async fn gcs_fake_round_trip() {
    let sa = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/emulators/gcs-fake-sa.json");
    let cfg = ObjectStoreConfig::Gcs { bucket: "bluedb".into(), service_account: Some(sa.into()) };
    let store = build_object_store(&cfg).expect("build gcs store");
    slatedb_round_trip(store, "emul-roundtrip-gcs").await;
}

/// Real-GCS round-trip — the definitive verification of the GCS target, since no
/// local emulator faithfully serves object_store's GCS *XML* API (fake-gcs is
/// JSON-only; storage-testbench lacks XML list/delete). Env-gated; **no creds are
/// committed** — supply a real bucket, a service-account JSON path, and a unique
/// throwaway prefix:
///
/// ```text
/// BLUEDB_GCS_TEST_BUCKET=my-bucket \
/// BLUEDB_GCS_TEST_SA=/path/to/sa.json \
/// BLUEDB_GCS_TEST_PREFIX=bluedb-gcs-roundtrip-test/run1 \
///   cargo test -p bluedb-server --test objstore_emulators gcs_real_round_trip -- --ignored --nocapture
/// ```
///
/// Delete the prefix afterward (e.g. `gcloud storage rm -r gs://$BUCKET/$PREFIX`).
#[tokio::test]
#[ignore = "real GCS — set BLUEDB_GCS_TEST_{BUCKET,SA,PREFIX}"]
async fn gcs_real_round_trip() {
    let bucket = std::env::var("BLUEDB_GCS_TEST_BUCKET").expect("set BLUEDB_GCS_TEST_BUCKET");
    let sa = std::env::var("BLUEDB_GCS_TEST_SA").expect("set BLUEDB_GCS_TEST_SA (service-account JSON path)");
    let prefix = std::env::var("BLUEDB_GCS_TEST_PREFIX").expect("set BLUEDB_GCS_TEST_PREFIX (unique throwaway prefix)");
    let cfg = ObjectStoreConfig::Gcs { bucket, service_account: Some(sa) };
    let store = build_object_store(&cfg).expect("build gcs store");
    slatedb_round_trip(store, &prefix).await;
}
