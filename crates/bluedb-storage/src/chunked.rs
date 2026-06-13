//! Chunked large-value layer over a [`BlobStoreMut`].
//!
//! SlateDB stores whole values, so [`SlateDbBlobStore::get_range`] must fetch
//! the entire value and slice it — O(value size) per range read. For large
//! objects (e.g. a multi-hundred-MB tantivy split) that is wasteful.
//!
//! [`ChunkedBlobStore`] addresses the `get_range` caveat by splitting a value
//! into fixed-size chunks stored under ordered keys, plus a tiny manifest key
//! recording the total length. A range read then pulls only the chunks that
//! overlap the requested range, not the whole object.
//!
//! ## Key layout (per logical `path`)
//! - manifest: `"{path}"` → little-endian `u64` total length
//! - chunk `i`: `"{path}\x00c\x00{i:020}"` → up to `chunk_size` bytes
//!
//! The `\x00c\x00` separator keeps chunk keys ordered *after* the manifest key
//! and contiguous, and the zero-padded index makes a prefix scan return chunks
//! in ascending order. Logical paths are assumed not to contain `\x00`.
//!
//! This layer is additive: it is built *on top of* the frozen [`BlobStore`] /
//! [`BlobStoreMut`] traits and does not touch them.

use std::ops::Range;

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};

use crate::BlobStoreMut;

/// Wraps a [`BlobStoreMut`] backend, storing large values as ordered chunks.
pub struct ChunkedBlobStore<B: BlobStoreMut> {
    inner: B,
    chunk_size: usize,
}

const CHUNK_SEP: &str = "\u{0}c\u{0}";

impl<B: BlobStoreMut> ChunkedBlobStore<B> {
    /// Wrap `inner`, splitting stored values into `chunk_size`-byte chunks.
    ///
    /// Panics if `chunk_size` is zero.
    pub fn new(inner: B, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        Self { inner, chunk_size }
    }

    /// The configured chunk size in bytes.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn chunk_key(path: &str, index: usize) -> String {
        format!("{path}{CHUNK_SEP}{index:020}")
    }

    fn chunk_prefix(path: &str) -> String {
        format!("{path}{CHUNK_SEP}")
    }

    /// Store `bytes` under `path`, split across chunk keys + a manifest.
    pub async fn put_chunked(&self, path: &str, bytes: &[u8]) -> Result<()> {
        if path.contains('\u{0}') {
            bail!("chunked path may not contain NUL: {path:?}");
        }
        // Replace any prior chunks so a smaller rewrite doesn't leave a tail.
        self.delete_chunked(path).await?;

        for (i, chunk) in bytes.chunks(self.chunk_size).enumerate() {
            self.inner
                .put(&Self::chunk_key(path, i), Bytes::copy_from_slice(chunk))
                .await?;
        }
        // Manifest written last so a reader that sees it sees a complete object.
        let len = bytes.len() as u64;
        self.inner
            .put(path, Bytes::copy_from_slice(&len.to_le_bytes()))
            .await?;
        Ok(())
    }

    /// Remove a chunked object (manifest + all chunks). Missing is a no-op.
    pub async fn delete_chunked(&self, path: &str) -> Result<()> {
        for (key, _) in self.inner.scan_prefix(&Self::chunk_prefix(path)).await? {
            self.inner.delete(&key).await?;
        }
        self.inner.delete(path).await?;
        Ok(())
    }

    /// Total length of the chunked object at `path`.
    pub async fn len_chunked(&self, path: &str) -> Result<usize> {
        self.read_manifest(path).await
    }

    async fn read_manifest(&self, path: &str) -> Result<usize> {
        let raw = self.inner.get_all(path).await?;
        let arr: [u8; 8] = raw
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("corrupt chunk manifest for {path}: len {}", raw.len()))?;
        Ok(u64::from_le_bytes(arr) as usize)
    }

    /// Read the half-open `range` of the chunked object at `path`, fetching
    /// only the chunks that overlap the range.
    pub async fn get_range_chunked(&self, path: &str, range: Range<usize>) -> Result<Bytes> {
        let total = self.read_manifest(path).await?;
        if range.start > range.end || range.end > total {
            bail!(
                "range {range:?} out of bounds for {path} (len {total})",
                range = range
            );
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }

        let first = range.start / self.chunk_size;
        let last = (range.end - 1) / self.chunk_size;

        let mut out = BytesMut::with_capacity(range.end - range.start);
        for idx in first..=last {
            let chunk = self.inner.get_all(&Self::chunk_key(path, idx)).await?;
            let chunk_start = idx * self.chunk_size;
            // Offsets within this chunk that fall inside `range`.
            let lo = range.start.saturating_sub(chunk_start);
            let hi = (range.end - chunk_start).min(chunk.len());
            out.extend_from_slice(&chunk[lo..hi]);
        }
        Ok(out.freeze())
    }

    /// Read the whole chunked object at `path`.
    pub async fn get_all_chunked(&self, path: &str) -> Result<Bytes> {
        let total = self.read_manifest(path).await?;
        self.get_range_chunked(path, 0..total).await
    }
}
