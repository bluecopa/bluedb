//! Multi-cloud object-store construction from environment config.
//!
//! `bluedb-server` can run its SlateDB substrate on S3 (AWS), Azure Blob, or GCS
//! — selected by environment, first match wins — plus local-fs and in-memory for
//! dev/tests. The same image and manifests target any cloud; only the env differs.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;

/// Which object-store backend to build, with its parsed config. Selection is
/// "first env var present wins", in the order S3 → Azure → GCS → local → memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    S3 {
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    },
    Azure {
        container: String,
        account: Option<String>,
        access_key: Option<String>,
        /// Override the blob endpoint (full account URL) — for Azurite or any
        /// Azure-compatible store. Implies plain HTTP is allowed.
        endpoint: Option<String>,
    },
    Gcs {
        bucket: String,
        service_account: Option<String>,
    },
    Local {
        dir: String,
    },
    Memory,
}

/// Parse the object-store backend from a config source (`get(key) -> value`).
/// Pure: no env access, no I/O — `main` passes `|k| std::env::var(k).ok()`.
pub fn parse_object_store_config(get: impl Fn(&str) -> Option<String>) -> ObjectStoreConfig {
    if let Some(bucket) = get("BLUEDB_S3_BUCKET") {
        ObjectStoreConfig::S3 {
            bucket,
            region: get("BLUEDB_S3_REGION"),
            endpoint: get("BLUEDB_S3_ENDPOINT"),
            access_key_id: get("BLUEDB_S3_ACCESS_KEY_ID"),
            secret_access_key: get("BLUEDB_S3_SECRET_ACCESS_KEY"),
        }
    } else if let Some(container) = get("BLUEDB_AZURE_CONTAINER") {
        ObjectStoreConfig::Azure {
            container,
            account: get("BLUEDB_AZURE_ACCOUNT"),
            access_key: get("BLUEDB_AZURE_ACCESS_KEY"),
            endpoint: get("BLUEDB_AZURE_ENDPOINT"),
        }
    } else if let Some(bucket) = get("BLUEDB_GCS_BUCKET") {
        ObjectStoreConfig::Gcs {
            bucket,
            service_account: get("BLUEDB_GCS_SERVICE_ACCOUNT"),
        }
    } else if let Some(dir) = get("BLUEDB_DATA_DIR") {
        ObjectStoreConfig::Local { dir }
    } else {
        ObjectStoreConfig::Memory
    }
}

impl ObjectStoreConfig {
    /// The fully-qualified storage base URI the lakehouse publishes Iceberg
    /// tables under, so the `metadata.json` a warehouse loads contains resolvable
    /// locations (e.g. `s3://bucket`, `file:///abs/dir`). Empty for the in-memory
    /// store (no external reader). The lakehouse FileIO strips this prefix to
    /// recover object-store keys.
    pub fn base_uri(&self) -> String {
        match self {
            ObjectStoreConfig::S3 { bucket, .. } => format!("s3://{bucket}"),
            ObjectStoreConfig::Gcs { bucket, .. } => format!("gs://{bucket}"),
            ObjectStoreConfig::Azure { container, .. } => format!("abfss://{container}"),
            ObjectStoreConfig::Local { dir } => {
                let abs =
                    std::fs::canonicalize(dir).unwrap_or_else(|_| std::path::PathBuf::from(dir));
                format!("file://{}", abs.display())
            }
            ObjectStoreConfig::Memory => String::new(),
        }
    }
}

/// Build the configured object store. S3/Azure/GCS construct their client
/// without a network round-trip — credentials resolve lazily on first request;
/// local creates the directory; memory is ephemeral.
pub fn build_object_store(cfg: &ObjectStoreConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match cfg {
        ObjectStoreConfig::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        } => {
            let mut b = AmazonS3Builder::new()
                .with_bucket_name(bucket.clone())
                .with_region(region.clone().unwrap_or_else(|| "us-east-1".to_string()));
            if let Some(e) = endpoint {
                // MinIO / non-AWS S3: custom endpoint, allow plain HTTP.
                b = b.with_endpoint(e.clone()).with_allow_http(true);
            }
            if let Some(k) = access_key_id {
                b = b.with_access_key_id(k.clone());
            }
            if let Some(s) = secret_access_key {
                b = b.with_secret_access_key(s.clone());
            }
            eprintln!("bluedb-server: object store = S3 (bucket={bucket})");
            Ok(Arc::new(b.build()?))
        }
        ObjectStoreConfig::Azure {
            container,
            account,
            access_key,
            endpoint,
        } => {
            let mut b = MicrosoftAzureBuilder::new().with_container_name(container.clone());
            if let Some(a) = account {
                b = b.with_account(a.clone());
            }
            if let Some(k) = access_key {
                b = b.with_access_key(k.clone());
            }
            if let Some(e) = endpoint {
                // Azurite / Azure-compatible: full account URL + plain HTTP.
                b = b.with_endpoint(e.clone()).with_allow_http(true);
            }
            eprintln!("bluedb-server: object store = Azure Blob (container={container})");
            Ok(Arc::new(b.build()?))
        }
        ObjectStoreConfig::Gcs {
            bucket,
            service_account,
        } => {
            let mut b = GoogleCloudStorageBuilder::new().with_bucket_name(bucket.clone());
            if let Some(sa) = service_account {
                b = b.with_service_account_path(sa.clone());
            }
            eprintln!("bluedb-server: object store = GCS (bucket={bucket})");
            Ok(Arc::new(b.build()?))
        }
        ObjectStoreConfig::Local { dir } => {
            std::fs::create_dir_all(dir)?;
            eprintln!("bluedb-server: object store = local fs at {dir}");
            Ok(Arc::new(LocalFileSystem::new_with_prefix(dir)?))
        }
        ObjectStoreConfig::Memory => {
            eprintln!("bluedb-server: object store = in-memory (ephemeral, single-node)");
            Ok(Arc::new(InMemory::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a `get` closure from a fixed set of key/value pairs.
    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn s3_selected_when_bucket_present_with_all_fields() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_S3_BUCKET", "b"),
            ("BLUEDB_S3_REGION", "us-west-2"),
            ("BLUEDB_S3_ENDPOINT", "http://minio:9000"),
            ("BLUEDB_S3_ACCESS_KEY_ID", "ak"),
            ("BLUEDB_S3_SECRET_ACCESS_KEY", "sk"),
        ]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::S3 {
                bucket: "b".into(),
                region: Some("us-west-2".into()),
                endpoint: Some("http://minio:9000".into()),
                access_key_id: Some("ak".into()),
                secret_access_key: Some("sk".into()),
            }
        );
    }

    #[test]
    fn s3_optional_fields_default_to_none() {
        let cfg = parse_object_store_config(env(&[("BLUEDB_S3_BUCKET", "b")]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::S3 {
                bucket: "b".into(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
            }
        );
    }

    #[test]
    fn azure_selected_when_only_container_present() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_AZURE_CONTAINER", "c"),
            ("BLUEDB_AZURE_ACCOUNT", "acct"),
            ("BLUEDB_AZURE_ACCESS_KEY", "key"),
        ]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::Azure {
                container: "c".into(),
                account: Some("acct".into()),
                access_key: Some("key".into()),
                endpoint: None,
            }
        );
    }

    #[test]
    fn azure_endpoint_captured_when_present() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_AZURE_CONTAINER", "c"),
            ("BLUEDB_AZURE_ACCOUNT", "devstoreaccount1"),
            (
                "BLUEDB_AZURE_ENDPOINT",
                "http://azurite:10000/devstoreaccount1",
            ),
        ]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::Azure {
                container: "c".into(),
                account: Some("devstoreaccount1".into()),
                access_key: None,
                endpoint: Some("http://azurite:10000/devstoreaccount1".into()),
            }
        );
    }

    #[test]
    fn gcs_selected_when_only_gcs_bucket_present() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_GCS_BUCKET", "g"),
            ("BLUEDB_GCS_SERVICE_ACCOUNT", "/sa.json"),
        ]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::Gcs {
                bucket: "g".into(),
                service_account: Some("/sa.json".into()),
            }
        );
    }

    #[test]
    fn local_selected_when_only_data_dir_present() {
        let cfg = parse_object_store_config(env(&[("BLUEDB_DATA_DIR", "/data")]));
        assert_eq!(
            cfg,
            ObjectStoreConfig::Local {
                dir: "/data".into()
            }
        );
    }

    #[test]
    fn memory_when_nothing_present() {
        let cfg = parse_object_store_config(env(&[]));
        assert_eq!(cfg, ObjectStoreConfig::Memory);
    }

    #[test]
    fn s3_takes_precedence_over_azure_and_gcs() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_S3_BUCKET", "b"),
            ("BLUEDB_AZURE_CONTAINER", "c"),
            ("BLUEDB_GCS_BUCKET", "g"),
        ]));
        assert!(matches!(cfg, ObjectStoreConfig::S3 { .. }));
    }

    #[test]
    fn azure_takes_precedence_over_gcs_and_local() {
        let cfg = parse_object_store_config(env(&[
            ("BLUEDB_AZURE_CONTAINER", "c"),
            ("BLUEDB_GCS_BUCKET", "g"),
            ("BLUEDB_DATA_DIR", "/data"),
        ]));
        assert!(matches!(cfg, ObjectStoreConfig::Azure { .. }));
    }

    // --- build_object_store: each backend constructs without a network call ---

    #[test]
    fn builds_memory() {
        assert!(build_object_store(&ObjectStoreConfig::Memory).is_ok());
    }

    #[test]
    fn builds_local_dir() {
        let dir = std::env::temp_dir().join("bluedb-objstore-test-local");
        let cfg = ObjectStoreConfig::Local {
            dir: dir.to_string_lossy().into_owned(),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn builds_s3_with_explicit_creds() {
        let cfg = ObjectStoreConfig::S3 {
            bucket: "b".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            access_key_id: Some("ak".into()),
            secret_access_key: Some("sk".into()),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn builds_s3_with_minio_endpoint() {
        let cfg = ObjectStoreConfig::S3 {
            bucket: "bluedb".into(),
            region: None,
            endpoint: Some("http://localhost:9000".into()),
            access_key_id: Some("minioadmin".into()),
            secret_access_key: Some("minioadmin".into()),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn builds_azure_with_shared_key() {
        // The well-known Azurite emulator key — a valid base64 shared key, so the
        // builder's eager `AzureAccessKey::try_new` decode succeeds.
        let cfg = ObjectStoreConfig::Azure {
            container: "c".into(),
            account: Some("devstoreaccount1".into()),
            access_key: Some(
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            ),
            endpoint: None,
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn builds_azure_with_endpoint() {
        // Azurite-style: explicit endpoint (account URL) + shared key.
        let cfg = ObjectStoreConfig::Azure {
            container: "c".into(),
            account: Some("devstoreaccount1".into()),
            access_key: Some(
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            ),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn builds_gcs_with_bucket_only() {
        // No service account / ADC present in tests → credentials resolve to None
        // and build() still succeeds (deferred to request time).
        let cfg = ObjectStoreConfig::Gcs {
            bucket: "g".into(),
            service_account: None,
        };
        assert!(build_object_store(&cfg).is_ok());
    }
}
