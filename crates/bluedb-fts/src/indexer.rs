//! Indexing pipeline — build a tantivy index, then pack it into a split.
//!
//! This is the writer side of the engine: given a [`tantivy::schema::Schema`]
//! and an iterator of [`tantivy::TantivyDocument`]s, build a real tantivy index
//! in a tempdir, commit it, then read the on-disk segment files back and pack
//! them into a Quickwit-compatible split via [`crate::split`].
//!
//! Two flavors of split are produced:
//! - [`build_split`] — split with an **empty** hotcache. Opened by fetching the
//!   whole split into memory ([`crate::vendor::BundleDirectory::open_split`]).
//! - [`build_split_with_hotcache`] — split carrying a **real** hotcache, so it
//!   can be opened lazily (range-fetch footer + hotcache only) via
//!   [`crate::open::open_split_lazy`].

use std::path::PathBuf;

use tantivy::schema::Schema;
use tantivy::{Index, TantivyDocument};
use tempfile::TempDir;

use crate::split::{pack_split, pack_split_with_hotcache};

/// Default tantivy writer heap budget (15 MB) — matches the test fixtures.
const DEFAULT_WRITER_HEAP_BYTES: usize = 15_000_000;

/// Builds tantivy indexes and emits splits.
///
/// Stateless aside from the writer heap budget; reusable across builds. Each
/// call to [`Indexer::build`] / [`Indexer::build_with_hotcache`] produces an
/// independent single-segment split.
pub struct Indexer {
    writer_heap_bytes: usize,
}

impl Default for Indexer {
    fn default() -> Self {
        Self {
            writer_heap_bytes: DEFAULT_WRITER_HEAP_BYTES,
        }
    }
}

impl Indexer {
    /// New indexer with the default writer heap budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the tantivy writer heap budget (bytes).
    pub fn with_writer_heap_bytes(mut self, bytes: usize) -> Self {
        self.writer_heap_bytes = bytes;
        self
    }

    /// Build a tantivy index from `docs`, commit it, and return
    /// `(index, tempdir)`. The tempdir owns the on-disk files; keep it alive
    /// for as long as you read from `index`.
    fn build_index(
        &self,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<(Index, TempDir)> {
        let dir = tempfile::tempdir()?;
        let index = Index::create_in_dir(dir.path(), schema)?;
        {
            let mut writer = index.writer(self.writer_heap_bytes)?;
            for doc in docs {
                writer.add_document(doc)?;
            }
            writer.commit()?;
        } // writer dropped -> lock released, files flushed to disk
        Ok((index, dir))
    }

    /// Build an index from `docs` and pack it into a split with an **empty**
    /// hotcache. Open with [`crate::vendor::BundleDirectory::open_split`] after
    /// fetching the whole split.
    pub fn build(
        &self,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<Vec<u8>> {
        let (_index, dir) = self.build_index(schema, docs)?;
        let files = read_index_files(dir.path())?;
        Ok(pack_split(&files))
    }

    /// Build an index from `docs` and pack it into a split carrying a **real**
    /// hotcache, computed from the freshly built index. Open lazily with
    /// [`crate::open::open_split_lazy`].
    pub fn build_with_hotcache(
        &self,
        schema: Schema,
        docs: impl IntoIterator<Item = TantivyDocument>,
    ) -> anyhow::Result<Vec<u8>> {
        let (_index, dir) = self.build_index(schema, docs)?;
        let files = read_index_files(dir.path())?;
        // Compute the hotcache from the on-disk index (the tempdir is still
        // alive). `write_hotcache` opens the directory itself (`Index::open`
        // over it, wrapping it in its own `ManagedDirectory`), so we must hand
        // it a *raw* `MmapDirectory` over the index path — NOT the index's own
        // `ManagedDirectory` (which would get double-managed and corrupt the
        // footer reads).
        let mmap_directory = tantivy::directory::MmapDirectory::open(dir.path())?;
        let mut hotcache: Vec<u8> = Vec::new();
        crate::vendor::write_hotcache(mmap_directory, &mut hotcache)?;
        Ok(pack_split_with_hotcache(&files, &hotcache))
    }
}

/// Read every regular file in `dir` (skipping `.lock` files) into
/// `(file_name, bytes)` pairs suitable for [`pack_split`].
///
/// Tantivy writes its segment files flat in the index directory; the split
/// stores them by file name, so this is the canonical "collect the index for
/// packing" step (previously inlined in `tests/query.rs`).
pub fn read_index_files(dir: &std::path::Path) -> anyhow::Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut files: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("non-utf8 file name in index dir"))?;
        if name.ends_with(".lock") {
            continue; // tantivy lock files are not part of the split
        }
        files.push((PathBuf::from(name), std::fs::read(&path)?));
    }
    Ok(files)
}

/// Convenience: build a split (empty hotcache) from a schema + docs in one call.
///
/// ```no_run
/// use bluedb_fts::indexer::build_split;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::TantivyDocument;
///
/// let mut sb = Schema::builder();
/// let body = sb.add_text_field("body", TEXT);
/// let schema = sb.build();
/// let mut doc = TantivyDocument::default();
/// doc.add_text(body, "hello world");
/// let split: Vec<u8> = build_split(schema, [doc]).unwrap();
/// ```
pub fn build_split(
    schema: Schema,
    docs: impl IntoIterator<Item = TantivyDocument>,
) -> anyhow::Result<Vec<u8>> {
    Indexer::new().build(schema, docs)
}

/// Convenience: build a split with a real hotcache from a schema + docs.
pub fn build_split_with_hotcache(
    schema: Schema,
    docs: impl IntoIterator<Item = TantivyDocument>,
) -> anyhow::Result<Vec<u8>> {
    Indexer::new().build_with_hotcache(schema, docs)
}
