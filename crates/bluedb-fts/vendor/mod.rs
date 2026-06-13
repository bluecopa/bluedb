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

// Adapted for bluedb: this was `quickwit-directories/src/lib.rs`, turned into a
// module (`vendor::mod`). The directory modules are unchanged; alongside them we
// host the trimmed read-only `Storage` trait (`storage`), the no-dependency
// metrics stub (`metrics`), the byte range cache, the versioned-component codec,
// the `BundleStorageFileOffsets` slice of `bundle_storage`, and the
// `SPLIT_FIELDS_FILE_NAME` constant from `shared_consts`.

//! This crate contains all of the building pieces that make quickwit's IO possible.
//!
//! - The `StorageDirectory` just wraps a `Storage` trait to make it compatible with tantivy's
//!   Directory API.
//! - The `BundleDirectory` bundles multiple files into a single file.
//! - The `HotDirectory` wraps another directory with a static cache.
//! - The `CachingDirectory` wraps a Directory with a dynamic cache.
//! - The `DebugDirectory` acts as a proxy to another directory to instrument it and record all of
//!   its IO.

// Re-export of tantivy's OwnedBytes, which upstream `quickwit-storage` re-exported.
// The vendored files refer to it as `super::OwnedBytes` (or via tantivy directly).
pub use tantivy::directory::OwnedBytes;

// --- storage support slice (was quickwit-storage / quickwit-common) ---
pub mod bundle_storage;
pub mod byte_range_cache;
pub mod metrics;
pub mod shared_consts;
pub mod storage;
pub mod storage_error;
pub mod versioned_component;

pub use self::bundle_storage::BundleStorageFileOffsets;
pub use self::byte_range_cache::ByteRangeCache;
pub use self::storage::Storage;
pub use self::storage_error::{StorageError, StorageErrorKind, StorageResult};
pub use self::versioned_component::VersionedComponent;

// --- directory read path (was quickwit-directories) ---
mod bundle_directory;
mod caching_directory;
mod debug_proxy_directory;
mod hot_directory;
mod storage_directory;
mod union_directory;

pub use self::bundle_directory::{get_hotcache_from_split, read_split_footer, BundleDirectory};
pub use self::caching_directory::CachingDirectory;
pub use self::debug_proxy_directory::{DebugProxyDirectory, ReadOperation};
pub use self::hot_directory::{write_hotcache, HotDirectory};
pub use self::storage_directory::StorageDirectory;
pub use self::union_directory::UnionDirectory;

macro_rules! read_only_directory {
    () => {
        fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
            unimplemented!("read-only")
        }

        fn delete(&self, _path: &Path) -> Result<(), tantivy::directory::error::DeleteError> {
            unimplemented!("read-only")
        }

        fn open_write(
            &self,
            _path: &Path,
        ) -> Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
            unimplemented!("read-only")
        }

        fn sync_directory(&self) -> io::Result<()> {
            unimplemented!("read-only")
        }

        fn watch(
            &self,
            _watch_callback: tantivy::directory::WatchCallback,
        ) -> tantivy::Result<tantivy::directory::WatchHandle> {
            Ok(tantivy::directory::WatchHandle::empty())
        }

        fn acquire_lock(
            &self,
            _lock: &tantivy::directory::Lock,
        ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
            Ok(tantivy::directory::DirectoryLock::from(Box::new(|| {})))
        }
    };
}
pub(crate) use read_only_directory;
