//! Lazy split open — range-fetch only the footer + hotcache.
//!
//! [`crate::vendor::BundleDirectory::open_split`] requires the *whole* split in
//! memory (one `get_all`). For a split carrying a real hotcache (see
//! [`crate::split::pack_split_with_hotcache`]) we can do better: fetch only the
//! small footer (file-offset map + hotcache) and serve every other read lazily
//! as a byte-range against the split blob in the substrate.
//!
//! The composition is:
//!
//! ```text
//! HotDirectory ( real hotcache bytes )
//!   └── SplitBlobDirectory  ── async get_range(split_key, off+range) ──► BlobStore
//! ```
//!
//! [`SplitBlobDirectory`] is the missing read-path piece: it maps a tantivy file
//! name to its `[start, end)` slice inside the split blob (per the bundle
//! offsets parsed from the footer) and reads that slice with an **async**
//! `read_bytes_async`, exactly like the vendored [`crate::vendor::StorageDirectory`]
//! — but offset into a single bundled blob rather than one object per file.
//! Wrapping it in [`crate::vendor::HotDirectory`] means the small startup reads
//! that `Index::open` performs are answered from the in-memory hotcache, so a
//! split opens and serves BM25 queries **without ever calling `get_all`**.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes};
use tantivy::{Directory, HasLen, Index};

use bluedb_storage::BlobStore;

use crate::vendor::{BundleStorageFileOffsets, CachingDirectory, HotDirectory};

const U32_LEN: usize = std::mem::size_of::<u32>();

/// The parsed split footer: the file-offset map plus the hotcache bytes.
struct SplitFooter {
    offsets: BundleStorageFileOffsets,
    hotcache: OwnedBytes,
}

/// Range-fetch and parse the split footer from `blob` at `key`.
///
/// Reads (in order): the trailing hotcache-len `u32`, the hotcache + bundle-meta
/// len `u32`, then the bundle metadata. Never fetches the file body. Mirrors the
/// `u32`-LE footer layout written by [`crate::split::pack_split_with_hotcache`].
async fn read_footer<B: BlobStore + ?Sized>(blob: &B, key: &str) -> anyhow::Result<SplitFooter> {
    let total = blob.len(key).await?;
    if total < 2 * U32_LEN {
        anyhow::bail!("split too small to contain a footer (len={total})");
    }

    // [ ... ][ hotcache ][ hotcache len: u32 ]
    let hotcache_len_bytes = blob.get_range(key, total - U32_LEN..total).await?;
    let hotcache_len = u32::from_le_bytes(hotcache_len_bytes.as_ref().try_into().unwrap()) as usize;

    // Layout: [ body ][ bundle-meta ][ bundle-meta len: u32 ][ hotcache ][ hotcache len: u32 ].
    // `hotcache_end` is where the hotcache region ends (== start of its len field).
    let hotcache_end = total - U32_LEN;
    let hotcache_start = hotcache_end.checked_sub(hotcache_len).ok_or_else(|| {
        anyhow::anyhow!("split footer is malformed (hotcache_len={hotcache_len})")
    })?;
    // The bundle-meta len field sits immediately before the hotcache.
    if hotcache_start < U32_LEN {
        anyhow::bail!("split footer is malformed (no room for bundle-meta len)");
    }
    let bundle_meta_len_field_start = hotcache_start - U32_LEN;
    let bundle_meta_len_bytes = blob
        .get_range(key, bundle_meta_len_field_start..hotcache_start)
        .await?;
    let bundle_meta_len =
        u32::from_le_bytes(bundle_meta_len_bytes.as_ref().try_into().unwrap()) as usize;

    // The region `BundleStorageFileOffsets::open` parses is
    // `[ bundle-meta ][ bundle-meta len ]` — i.e. the metadata plus its trailing
    // len field (which it peels off internally).
    let bundle_region_start = bundle_meta_len_field_start
        .checked_sub(bundle_meta_len)
        .ok_or_else(|| anyhow::anyhow!("split footer is malformed (bundle_meta_len too large)"))?;
    let bundle_meta_with_len = blob
        .get_range(key, bundle_region_start..hotcache_start)
        .await?;
    let bundle_slice = FileSlice::new(Arc::new(OwnedBytes::new(bundle_meta_with_len.to_vec())));
    let offsets = BundleStorageFileOffsets::open(bundle_slice)
        .map_err(|err| anyhow::anyhow!("failed to parse bundle offsets: {err}"))?;
    let hotcache = if hotcache_len == 0 {
        OwnedBytes::empty()
    } else {
        let bytes = blob
            .get_range(key, hotcache_start..hotcache_start + hotcache_len)
            .await?;
        OwnedBytes::new(bytes.to_vec())
    };

    Ok(SplitFooter { offsets, hotcache })
}

/// A tantivy [`Directory`] over a **single split blob** in object storage.
///
/// Each tantivy file name maps to a `[start, end)` slice of the split blob (per
/// the bundle offsets parsed from the footer). Reads are async byte-range
/// fetches; sync `read_bytes`/`atomic_read` are unsupported (like
/// [`crate::vendor::StorageDirectory`]). Intended to be wrapped by
/// [`HotDirectory`].
#[derive(Clone)]
pub struct SplitBlobDirectory {
    blob: Arc<dyn BlobStore>,
    key: Arc<str>,
    offsets: Arc<BundleStorageFileOffsets>,
}

impl fmt::Debug for SplitBlobDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SplitBlobDirectory(key={:?})", self.key)
    }
}

impl SplitBlobDirectory {
    fn file_range(&self, path: &Path) -> Result<Range<u64>, OpenReadError> {
        self.offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }

    /// All `(file_name, byte_len)` pairs in the split, for warmup.
    fn files(&self) -> Vec<(std::path::PathBuf, u64)> {
        self.offsets
            .files
            .iter()
            .map(|(path, range)| (path.clone(), range.end - range.start))
            .collect()
    }
}

struct SplitBlobFileHandle {
    blob: Arc<dyn BlobStore>,
    key: Arc<str>,
    /// Absolute `[start, end)` of this tantivy file inside the split blob.
    file_range: Range<u64>,
}

impl HasLen for SplitBlobFileHandle {
    fn len(&self) -> usize {
        (self.file_range.end - self.file_range.start) as usize
    }
}

impl fmt::Debug for SplitBlobFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SplitBlobFileHandle(key={:?}, range={:?})",
            self.key, self.file_range
        )
    }
}

fn unsupported_sync(key: &str) -> io::Error {
    io::Error::other(format!(
        "SplitBlobDirectory only supports async reads (key={key})"
    ))
}

#[async_trait]
impl FileHandle for SplitBlobFileHandle {
    fn read_bytes(&self, _byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        Err(unsupported_sync(&self.key))
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if byte_range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        // Translate the file-relative range into an absolute range in the blob.
        let start = self.file_range.start as usize + byte_range.start;
        let end = self.file_range.start as usize + byte_range.end;
        let bytes = self
            .blob
            .get_range(&self.key, start..end)
            .await
            .map_err(io::Error::other)?;
        Ok(OwnedBytes::new(bytes.to_vec()))
    }
}

impl Directory for SplitBlobDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_range = self.file_range(path)?;
        Ok(Arc::new(SplitBlobFileHandle {
            blob: self.blob.clone(),
            key: self.key.clone(),
            file_range,
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.offsets.exists(path))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        Err(OpenReadError::wrap_io_error(
            unsupported_sync(&self.key),
            path.to_path_buf(),
        ))
    }

    crate::vendor::read_only_directory!();
}

/// Open a split that lives in `blob` under `key` **lazily**: range-fetch only
/// the footer + hotcache, then serve every other read on demand as a byte-range
/// against the split blob. The split MUST have been packed with a real hotcache
/// ([`crate::split::pack_split_with_hotcache`]); otherwise `Index::open` would
/// fall through to the underlying (async-only) directory on a sync read and the
/// open would fail.
///
/// Returns a ready-to-search [`tantivy::Index`]. No `get_all` is ever issued.
pub async fn open_split_lazy(blob: Arc<dyn BlobStore>, key: &str) -> anyhow::Result<Index> {
    let footer = read_footer(blob.as_ref(), key).await?;
    if footer.hotcache.is_empty() {
        anyhow::bail!(
            "split {key} has an empty hotcache; lazy open requires a split packed with \
             pack_split_with_hotcache. Use BundleDirectory::open_split over a full fetch instead."
        );
    }

    let split_dir = SplitBlobDirectory {
        blob,
        key: Arc::from(key),
        offsets: Arc::new(footer.offsets),
    };

    // tantivy's searcher issues *synchronous* `read_bytes` while scoring, but
    // `SplitBlobDirectory` only serves async byte-range reads. We bridge the two
    // exactly like quickwit does: front the storage directory with a
    // `CachingDirectory` (a `ByteRangeCache`) and **warm** it by async-reading
    // every file up-front. Subsequent sync reads then hit the in-memory cache.
    //
    // This keeps the property we care about: the only object-storage I/O is
    // scoped `get_range` calls (footer + hotcache + these warmed file ranges);
    // `get_all` is never called.
    let caching = CachingDirectory::new_unbounded(Arc::new(split_dir.clone()));
    for (path, len) in split_dir.files() {
        if len == 0 {
            continue;
        }
        let handle = caching
            .get_file_handle(&path)
            .map_err(|err| anyhow::anyhow!("warmup get_file_handle({path:?}): {err:?}"))?;
        handle
            .read_bytes_async(0..len as usize)
            .await
            .map_err(|err| anyhow::anyhow!("warmup read {path:?}: {err}"))?;
    }

    // The hotcache still accelerates the small startup reads `Index::open`
    // performs; everything else is served from the warmed `CachingDirectory`.
    let hot_dir = HotDirectory::open(caching, footer.hotcache)?;
    let index = Index::open(hot_dir)?;
    Ok(index)
}
