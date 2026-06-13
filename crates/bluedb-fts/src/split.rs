//! Minimal split **writer** — the indexer's packing step.
//!
//! Produces a Quickwit-compatible *split* that the vendored
//! [`crate::vendor::BundleDirectory`] can open. Split layout (the sync read
//! path in `bundle_directory.rs` / `bundle_storage.rs`):
//!
//! ```text
//! [ files ][ files-metadata ][ files-metadata len: u32 LE ][ hotcache ][ hotcache len: u32 LE ]
//! ```
//!
//! - `files` — the tantivy index files concatenated; each file's `[start,end)`
//!   offset into this region is recorded in `files-metadata`.
//! - `files-metadata` — a `VersionedComponent`-encoded
//!   [`BundleStorageFileOffsets`] (`magic u32` + `version u32` + JSON).
//! - [`pack_split`] writes an **empty hotcache**. The hotcache only matters for
//!   [`crate::vendor::HotDirectory`]'s lazy *async* reads; when the whole split
//!   is fetched into memory, `BundleDirectory` serves sync reads directly, so no
//!   hotcache is needed to open and query.
//! - [`pack_split_with_hotcache`] writes a **real hotcache** (produced by
//!   [`crate::vendor::write_hotcache`]), enabling the lazy open path
//!   ([`crate::open::open_split_lazy`]) that range-fetches only the footer +
//!   hotcache instead of loading the whole split.
//!
//! All footer lengths are `u32` LE (matching the vendored
//! [`crate::vendor::BundleStorageFileOffsets`] reader, which uses
//! `size_of::<u32>()` footers).

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;

use crate::vendor::bundle_storage::BundleStorageFileOffsetsVersions;
use crate::vendor::{BundleStorageFileOffsets, VersionedComponent};

/// Pack named files into a split byte buffer (with an empty hotcache).
///
/// `files` is `(name, bytes)` for each tantivy index file (e.g. `meta.json`,
/// `<segment>.idx`, …). The resulting buffer can be written to object storage
/// and re-opened with [`crate::vendor::BundleDirectory::open_split`].
pub fn pack_split(files: &[(PathBuf, Vec<u8>)]) -> Vec<u8> {
    pack_split_with_hotcache(files, &[])
}

/// Pack named files into a split byte buffer with a **real** `hotcache`.
///
/// Identical layout to [`pack_split`], but the hotcache region carries the
/// caller-supplied bytes (produced by [`crate::vendor::write_hotcache`] over the
/// freshly built index) instead of being empty. The hotcache lets
/// [`crate::vendor::HotDirectory`] serve the small startup reads needed to
/// `Index::open` from memory, so a reader can open the split by range-fetching
/// only the footer + hotcache rather than the whole split.
///
/// Passing an empty `hotcache` is exactly [`pack_split`] (the back-compat path).
pub fn pack_split_with_hotcache(files: &[(PathBuf, Vec<u8>)], hotcache: &[u8]) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    let mut offsets: HashMap<PathBuf, Range<u64>> = HashMap::new();
    for (name, bytes) in files {
        let start = body.len() as u64;
        body.extend_from_slice(bytes);
        let end = body.len() as u64;
        offsets.insert(name.clone(), start..end);
    }

    let file_offsets = BundleStorageFileOffsets { files: offsets };
    let metadata = BundleStorageFileOffsetsVersions::serialize(&file_offsets);

    let mut split = body;
    // [ files-metadata ][ files-metadata len: u32 LE ]
    split.extend_from_slice(&metadata);
    split.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    // [ hotcache ][ hotcache len: u32 LE ]
    split.extend_from_slice(hotcache);
    split.extend_from_slice(&(hotcache.len() as u32).to_le_bytes());
    split
}
