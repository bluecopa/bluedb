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

// Adapted for bluedb: this is the READ-ONLY slice of
// `quickwit-storage/src/bundle_storage.rs`. Only `BundleStorageFileOffsets`
// (the split file-offset metadata read by `BundleDirectory`) and its
// `BundleStorageFileOffsetsVersions` codec are kept. The `BundleStorage` struct,
// its `Storage`/`HasLen`/`Debug` impls, the write path, and the
// `chunk_range`/`SendableAsync`/`PutPayload` dependencies were all dropped.

use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tantivy::directory::FileSlice;

use super::versioned_component::VersionedComponent;
use super::OwnedBytes;

const SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES: usize = std::mem::size_of::<u32>();
const BUNDLE_METADATA_LENGTH_NUM_BYTES: usize = std::mem::size_of::<u32>();

#[derive(Copy, Clone, Default)]
#[repr(u32)]
pub enum BundleStorageFileOffsetsVersions {
    #[default]
    V1 = 1,
}

impl VersionedComponent for BundleStorageFileOffsetsVersions {
    const MAGIC_NUMBER: u32 = 403_881_646u32;

    type Component = BundleStorageFileOffsets;

    fn to_version_code(self) -> u32 {
        self as u32
    }

    fn try_from_version_code_impl(version_code: u32) -> Option<Self> {
        match version_code {
            1 => Some(Self::V1),
            _ => None,
        }
    }

    fn serialize_impl(component: &BundleStorageFileOffsets, output: &mut Vec<u8>) {
        let metadata_json = serde_json::to_string(component).unwrap();
        output.extend_from_slice(metadata_json.as_bytes());
    }

    fn deserialize_impl(&self, bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component> {
        serde_json::from_reader(bytes).context("deserializing bundle storage file offsets failed")
    }
}

/// Returns the file offsets in the file bundle.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BundleStorageFileOffsets {
    /// The files and their offsets in the body
    pub files: HashMap<PathBuf, Range<u64>>,
}

impl BundleStorageFileOffsets {
    /// File need to include split data (with hotcache at the end).
    /// See docs/internals/split-format.md
    /// [Files, FileMetadata, FileMetadata Len, HotCache, HotCache Len]
    /// Returns (Hotcache, Self)
    pub fn open_from_split_data(file: FileSlice) -> anyhow::Result<(FileSlice, Self)> {
        let (bundle_and_hotcache_bytes, hotcache_num_bytes_data) =
            file.split_from_end(SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES);
        let hotcache_num_bytes: u32 = u32::from_le_bytes(
            hotcache_num_bytes_data
                .read_bytes()?
                .as_ref()
                .try_into()
                .unwrap(),
        );
        let (bundle, hotcache) =
            bundle_and_hotcache_bytes.split_from_end(hotcache_num_bytes as usize);
        Ok((hotcache, Self::open(bundle)?))
    }

    /// FileSlice needs to end with the bundle (without hotcache from the split at the end).
    /// See docs/internals/split-format.md
    /// [Files, FileMetadata, FileMetadata Len]
    pub fn open(file: FileSlice) -> anyhow::Result<Self> {
        let (tantivy_files_data, num_bytes_file_metadata) =
            file.split_from_end(BUNDLE_METADATA_LENGTH_NUM_BYTES);
        let footer_num_bytes: u32 = u32::from_le_bytes(
            num_bytes_file_metadata
                .read_bytes()?
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let mut bundle_storage_file_offsets_data = tantivy_files_data
            .slice_from_end(footer_num_bytes as usize)
            .read_bytes()?;
        BundleStorageFileOffsetsVersions::try_read_component(&mut bundle_storage_file_offsets_data)
    }

    /// Returns file offsets for given path.
    pub fn get(&self, path: &Path) -> Option<Range<u64>> {
        self.files.get(path).cloned()
    }

    /// Returns whether file exists in metadata.
    pub fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}
