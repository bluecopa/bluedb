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

// Adapted for bluedb: `quickwit_storage::{BundleStorageFileOffsets, OwnedBytes,
// Storage, StorageResult}` repointed to the local `super::` modules, and
// `crate::read_only_directory!` to `super::`. The upstream `#[cfg(test)]` module
// was dropped because it exercised the write path (`PutPayload`,
// `SplitPayloadBuilder`) which is not vendored.

use std::convert::TryInto;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice};
use tantivy::{Directory, HasLen};

use super::{BundleStorageFileOffsets, OwnedBytes, Storage, StorageResult};

/// BundleDirectory is a read-only directory that makes it possible to
/// open a split and serve the file it contains via tantivy's `Directory`.
///
/// It is the `Directory` equivalent of `BundleStorage`.
///
/// Split Format:
/// `[Files][FilesMetadata][FilesMetadata length 8 byte Little endian][Hotcache][Hotcache length 8
/// byte Little endian]`
#[derive(Clone)]
pub struct BundleDirectory {
    file: FileSlice,
    file_offsets: BundleStorageFileOffsets,
}

impl Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BundleDirectory")
    }
}

/// Loads the split footer from a storage and path.
///
/// Returns (SplitFooter, BundleFooter)
/// SplitFooter [BundleMetadata, BundleMetadata Len, Hotcache, Hotcache len]
/// BundleFooter [BundleMetadata, BundleMetadata Len]
pub async fn read_split_footer(
    storage: Arc<dyn Storage>,
    path: &Path,
) -> StorageResult<(OwnedBytes, OwnedBytes)> {
    let file_len = storage.file_num_bytes(path).await? as usize;

    let hotcache_len_bytes = storage.get_slice(path, file_len - 8..file_len).await?;
    let hotcache_len = u64::from_le_bytes(hotcache_len_bytes.as_ref().try_into().unwrap()) as usize;

    let second_footer_start = file_len - 8 - hotcache_len - 8;
    let second_footer_bytes = storage
        .get_slice(path, second_footer_start..second_footer_start + 8)
        .await?;
    let second_footer_len =
        u64::from_le_bytes(second_footer_bytes.as_ref().try_into().unwrap()) as usize;

    let split_footer = storage
        .get_slice(path, second_footer_start - second_footer_len..file_len)
        .await?;
    let only_bundle_footer = split_footer.slice(0..second_footer_len + 8);

    Ok((split_footer, only_bundle_footer))
}

/// Return two slices for given split: `[body and bundle meta data] [hotcache]`
fn split_footer(file_slice: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let (body_and_footer_slice, footer_len_slice) = file_slice.split_from_end(4);
    let footer_len_bytes = footer_len_slice.read_bytes()?;
    let footer_len = u32::from_le_bytes(footer_len_bytes.as_slice().try_into().unwrap());
    Ok(body_and_footer_slice.split_from_end(footer_len as usize))
}

/// Return two slices for given split: `[body and bundle meta data] [hotcache]`
pub fn get_hotcache_from_split(data: OwnedBytes) -> io::Result<OwnedBytes> {
    let split_file = FileSlice::new(Arc::new(data));
    let (_, hotcache) = split_footer(split_file)?;
    hotcache.read_bytes()
}

impl BundleDirectory {
    /// Get files and their sizes in a split.
    pub fn get_stats_split(data: OwnedBytes) -> anyhow::Result<Vec<(PathBuf, u64)>> {
        let split_file = FileSlice::new(Arc::new(data));
        let (body_and_bundle_metadata, hot_cache) = split_footer(split_file)?;
        let file_offsets = BundleStorageFileOffsets::open(body_and_bundle_metadata)?;

        let mut files_and_size: Vec<(_, _)> = file_offsets
            .files
            .into_iter()
            .map(|(file, range)| (file, range.end - range.start))
            .collect();

        files_and_size.push((
            PathBuf::from("hotcache".to_string()),
            hot_cache.len() as u64,
        ));

        files_and_size.sort();
        Ok(files_and_size)
    }

    /// Opens a split file.
    pub fn open_split(split_file: FileSlice) -> io::Result<BundleDirectory> {
        // First we remove the hotcache from our file slice.
        let (body_and_bundle_metadata, _hot_cache) = split_footer(split_file)?;
        BundleDirectory::open_bundle(body_and_bundle_metadata).map_err(io::Error::other)
    }

    /// Opens a BundleDirectory, given a file containing the bundle data.
    pub fn open_bundle(file: FileSlice) -> anyhow::Result<BundleDirectory> {
        let file_offsets = BundleStorageFileOffsets::open(file.clone())?;
        Ok(BundleDirectory { file, file_offsets })
    }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let byte_range = self
            .file_offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(self
            .file
            .slice(byte_range.start as usize..byte_range.end as usize))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        let payload = file_slice
            .read_bytes()
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(payload.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.file_offsets.exists(path))
    }

    super::read_only_directory!();
}

// Adapted for bluedb: the upstream `#[cfg(test)] mod tests` lived here. It built
// splits with `quickwit_storage::SplitPayloadBuilder` / `PutPayload` (the write
// path, intentionally not vendored), so it was removed.
#[cfg(test)]
mod tests {
    // The upstream tests built splits via the (non-vendored) write path
    // (`SplitPayloadBuilder` / `PutPayload`) and asserted on `SPLIT_FIELDS_FILE_NAME`.
    // They are intentionally omitted; this reference keeps the constant exercised.
    #[allow(unused_imports)]
    use super::super::shared_consts::SPLIT_FIELDS_FILE_NAME;
}
