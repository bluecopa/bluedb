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

// Adapted for bluedb: trimmed `quickwit_storage::Storage` to the READ-ONLY
// methods that the vendored `quickwit-directories` read path actually calls
// (`get_slice`, `get_all`, `file_num_bytes`, `exists`, `uri`). All write-path
// methods (`put`, `copy_to`, `copy_to_file`, `delete`, `bulk_delete`,
// `get_slice_stream`) and the `PutPayload` / `SendableAsync` types were
// dropped. The original `uri()` returned `&quickwit_common::uri::Uri`; here it
// returns `&str` so we don't have to vendor `Uri`. A blanket
// `impl<B: BlobStore> Storage for B` bridges any `bluedb_storage::BlobStore`
// into this trait, converting `bytes::Bytes` into tantivy's `OwnedBytes`.

use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;

use super::storage_error::{StorageErrorKind, StorageResult};
use super::OwnedBytes;

/// Storage meant to receive and serve quickwit's split (READ-ONLY subset).
///
/// This is the trimmed version of `quickwit_storage::Storage`: only the methods
/// exercised by the directory read path are kept. Object storage is the primary
/// target implementation, accessed through [`bluedb_storage::BlobStore`] via the
/// blanket impl below.
///
/// Adapted for bluedb: the upstream `Storage` had a `fmt::Debug` supertrait. The
/// read path never relies on it (`StorageDirectory`/`BundleStorage` `Debug`
/// impls only use `uri()` / a static string), so it is dropped here — otherwise
/// the blanket `impl<B: BlobStore> Storage for B` would force `B: Debug`, which
/// `BlobStore` does not require.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Downloads a slice of a file from the storage, and returns an in memory buffer.
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes>;

    /// Downloads the entire content of a "small" file, returns an in memory buffer.
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes>;

    /// Returns whether a file exists or not.
    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        match self.file_num_bytes(path).await {
            Ok(_) => Ok(true),
            Err(storage_err) if storage_err.kind() == StorageErrorKind::NotFound => Ok(false),
            Err(other_storage_err) => Err(other_storage_err),
        }
    }

    /// Returns a file size.
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64>;

    /// Returns an URI identifying the storage.
    fn uri(&self) -> &str;
}

/// Blanket bridge: any [`bluedb_storage::BlobStore`] is a (read-only) [`Storage`].
///
/// `BlobStore` is keyed by `&str`; tantivy hands us `&Path`. We render the path
/// with `to_string_lossy()` (split contents are tantivy file names — plain
/// ASCII, so this is lossless in practice).
#[async_trait]
impl<B: bluedb_storage::BlobStore> Storage for B {
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let path_str = path.to_string_lossy();
        let bytes = bluedb_storage::BlobStore::get_range(self, path_str.as_ref(), range)
            .await
            .map_err(|err| StorageErrorKind::Io.with_error(err))?;
        Ok(OwnedBytes::new(bytes.to_vec()))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let path_str = path.to_string_lossy();
        let bytes = bluedb_storage::BlobStore::get_all(self, path_str.as_ref())
            .await
            .map_err(|err| StorageErrorKind::Io.with_error(err))?;
        Ok(OwnedBytes::new(bytes.to_vec()))
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let path_str = path.to_string_lossy();
        let len = bluedb_storage::BlobStore::len(self, path_str.as_ref())
            .await
            .map_err(|err| StorageErrorKind::Io.with_error(err))?;
        Ok(len as u64)
    }

    fn uri(&self) -> &str {
        "blob://"
    }
}
