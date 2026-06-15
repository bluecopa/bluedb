//! Object-store-backed Iceberg [`FileIO`], so the lakehouse mirror writes its
//! Iceberg tables into the **same** bucket as SlateDB — which is exactly where
//! BigQuery/Databricks/Snowflake read them from.
//!
//! iceberg-rust 0.9.1 ships only local-fs and in-memory storage, but its
//! `Storage`/`StorageFactory` traits are pluggable, so this bridges them onto an
//! [`object_store::ObjectStore`] handle (the one the server already configured
//! for S3/Azure/GCS/local/memory). Iceberg paths are used directly as
//! object-store keys, so configure the lakehouse root as a bare key prefix
//! (e.g. `"lakehouse"`).
//!
//! The traits require `typetag::serde`, but a live object-store handle isn't
//! serializable and we never (de)serialize the FileIO — the serde impls
//! deliberately error, satisfying the trait bound without faking a round-trip.

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result as IceResult};
use object_store::path::Path as OsPath;
use object_store::ObjectStore;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A [`FileIO`] that reads/writes through `store`, sharing the bucket with
/// SlateDB.
///
/// `strip_base` is the fully-qualified location prefix the tables are published
/// under (e.g. `s3://bucket` or `file:///abs/root`); it is stripped from every
/// Iceberg path to recover the object-store key. Pass `""` to treat paths as
/// bare keys (e.g. an in-memory store with a bare-prefix root). A fully-qualified
/// base means the `metadata.json` a warehouse loads contains resolvable URIs.
pub fn object_store_file_io(store: Arc<dyn ObjectStore>, strip_base: impl Into<String>) -> FileIO {
    FileIOBuilder::new(Arc::new(ObjStoreFactory {
        store,
        strip_base: strip_base.into(),
    }))
    .build()
}

fn os_err(e: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("object_store: {e}"))
}

/// Recover the object-store key from an iceberg path: drop the `strip_base`
/// prefix (if present) and any leading separators.
fn key(path: &str, strip_base: &str) -> OsPath {
    let rest = path.strip_prefix(strip_base).unwrap_or(path);
    OsPath::from(rest.trim_start_matches('/'))
}

struct ObjStoreFactory {
    store: Arc<dyn ObjectStore>,
    strip_base: String,
}

impl Debug for ObjStoreFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObjStoreFactory")
    }
}

// Never serialized in our flow (see module docs) — error rather than carry the
// live handle through serde.
impl Serialize for ObjStoreFactory {
    fn serialize<S: Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("ObjStoreFactory is not serializable"))
    }
}
impl<'de> Deserialize<'de> for ObjStoreFactory {
    fn deserialize<D: Deserializer<'de>>(_d: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("ObjStoreFactory is not deserializable"))
    }
}

#[typetag::serde]
impl StorageFactory for ObjStoreFactory {
    fn build(&self, _config: &StorageConfig) -> IceResult<Arc<dyn Storage>> {
        Ok(Arc::new(ObjStoreStorage {
            store: self.store.clone(),
            strip_base: self.strip_base.clone(),
        }))
    }
}

struct ObjStoreStorage {
    store: Arc<dyn ObjectStore>,
    strip_base: String,
}

impl ObjStoreStorage {
    fn key(&self, path: &str) -> OsPath {
        key(path, &self.strip_base)
    }
}

impl Debug for ObjStoreStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObjStoreStorage")
    }
}
impl Serialize for ObjStoreStorage {
    fn serialize<S: Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("ObjStoreStorage is not serializable"))
    }
}
impl<'de> Deserialize<'de> for ObjStoreStorage {
    fn deserialize<D: Deserializer<'de>>(_d: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("ObjStoreStorage is not deserializable"))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for ObjStoreStorage {
    async fn exists(&self, path: &str) -> IceResult<bool> {
        match self.store.head(&self.key(path)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(os_err(e)),
        }
    }

    async fn metadata(&self, path: &str) -> IceResult<FileMetadata> {
        let meta = self.store.head(&self.key(path)).await.map_err(os_err)?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> IceResult<Bytes> {
        let res = self.store.get(&self.key(path)).await.map_err(os_err)?;
        res.bytes().await.map_err(os_err)
    }

    async fn reader(&self, path: &str) -> IceResult<Box<dyn FileRead>> {
        Ok(Box::new(ObjRead {
            store: self.store.clone(),
            path: self.key(path),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> IceResult<()> {
        self.store.put(&self.key(path), bs.into()).await.map_err(os_err)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> IceResult<Box<dyn FileWrite>> {
        Ok(Box::new(ObjWrite {
            store: self.store.clone(),
            path: self.key(path),
            buf: Vec::new(),
        }))
    }

    async fn delete(&self, path: &str) -> IceResult<()> {
        self.store.delete(&self.key(path)).await.map_err(os_err)
    }

    async fn delete_prefix(&self, path: &str) -> IceResult<()> {
        let prefix = self.key(path);
        let mut stream = self.store.list(Some(&prefix));
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(os_err)?;
            self.store.delete(&meta.location).await.map_err(os_err)?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> IceResult<InputFile> {
        Ok(InputFile::new(
            Arc::new(ObjStoreStorage {
                store: self.store.clone(),
                strip_base: self.strip_base.clone(),
            }),
            path.to_string(),
        ))
    }

    fn new_output(&self, path: &str) -> IceResult<OutputFile> {
        Ok(OutputFile::new(
            Arc::new(ObjStoreStorage {
                store: self.store.clone(),
                strip_base: self.strip_base.clone(),
            }),
            path.to_string(),
        ))
    }
}

/// Ranged reader over one object-store key.
struct ObjRead {
    store: Arc<dyn ObjectStore>,
    path: OsPath,
}

#[async_trait]
impl FileRead for ObjRead {
    async fn read(&self, range: Range<u64>) -> IceResult<Bytes> {
        self.store.get_range(&self.path, range).await.map_err(os_err)
    }
}

/// Buffering writer: object stores want the whole object at once, so we collect
/// the writes and `put` on close. iceberg's rolling writer caps each file at the
/// target size, so peak buffer ≈ one target-sized file.
struct ObjWrite {
    store: Arc<dyn ObjectStore>,
    path: OsPath,
    buf: Vec<u8>,
}

#[async_trait]
impl FileWrite for ObjWrite {
    async fn write(&mut self, bs: Bytes) -> IceResult<()> {
        self.buf.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> IceResult<()> {
        let payload = std::mem::take(&mut self.buf);
        self.store
            .put(&self.path, payload.into())
            .await
            .map_err(os_err)?;
        Ok(())
    }
}
