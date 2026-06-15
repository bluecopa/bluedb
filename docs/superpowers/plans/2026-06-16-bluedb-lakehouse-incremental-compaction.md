# Lakehouse Incremental Bin-Packed Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add incremental bin-packed (minor) compaction that rewrites only small data files, keeping the whole-table (major) compaction as a periodic delete-reclaiming pass; target file size set via PRAGMA.

**Architecture:** Minor compaction selects a cohort of sub-target per-seal data manifests, scans them through a transient scoped snapshot (reusing iceberg-rust's merge-on-read reader — the `scan_manifest_subset` building block proven by the de-risk spike), writes one+ compacted files, and commits an `Overwrite` that carries survivor data manifests + all delete manifests forward **intact** (preserving their sequence numbers) and drops the cohort. The worker runs minor by default and major when delete files pile up.

**Tech Stack:** Rust, iceberg-rust 0.9.1, gluesql-core 0.19, tokio. Spec: `docs/superpowers/specs/2026-06-16-bluedb-lakehouse-incremental-compaction-design.md`. Spike already landed (`scan_manifest_subset`, `live_manifest_files` in `writer.rs`).

**Conventions:** commit-only (NEVER push). Trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. `git commit -F -` (no backticks in `-m`). NEVER `cargo fmt`. "bluecopa" lowercase. Tests run directly with `2>&1`, no tail/grep pipes.

---

### Task A: `PRAGMA lakehouse_target_file_bytes`

**Files:** Modify `crates/bluedb-sql/src/lakehouse.rs` (enum + parser + tests).

- [ ] **Step 1: Failing tests** — add to the `mod tests` in `lakehouse.rs`:

```rust
    #[test]
    fn parses_target_file_bytes() {
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_target_file_bytes = 134217728"),
            Some(LhPragma::TargetFileBytes(134_217_728))
        );
        assert_eq!(
            parse_lakehouse_pragma("SET lakehouse_target_file_bytes = 1048576;"),
            Some(LhPragma::TargetFileBytes(1_048_576))
        );
        assert_eq!(
            parse_lakehouse_pragma("PRAGMA lakehouse_target_file_bytes = nope"),
            None
        );
    }
```

- [ ] **Step 2: Run → fail** — `cargo test -p bluedb-sql --lib lakehouse 2>&1` (no `TargetFileBytes` variant).

- [ ] **Step 3: Implement** — add the variant and broaden the parser. In `lakehouse.rs`:

```rust
pub enum LhPragma {
    GlobalDefault(bool),
    Table(String, bool),
    /// Set the incremental-compaction bin-pack target file size, in bytes.
    TargetFileBytes(u64),
}
```

In `parse_lakehouse_pragma`, after the `pragma `/`set ` prefix check and before the `lakehouse_mirror` containment check, handle the target form (it must run before the mirror check because the mirror check early-returns `None`):

```rust
    // Target-file-size form: lakehouse_target_file_bytes = <integer>
    if let Some(rest) = body.split("lakehouse_target_file_bytes").nth(1) {
        let value = rest.trim_start_matches([' ', '=']).trim();
        let value = value.split_whitespace().next().unwrap_or(value);
        return value.parse::<u64>().ok().map(LhPragma::TargetFileBytes);
    }
    if !body.contains("lakehouse_mirror") {
        return None;
    }
```

- [ ] **Step 4: Run → pass** — `cargo test -p bluedb-sql --lib lakehouse 2>&1` (all lakehouse pragma tests).

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-sql/src/lakehouse.rs
git commit -F - <<'EOF'
feat(compaction): PRAGMA lakehouse_target_file_bytes parsing

LhPragma::TargetFileBytes(u64) + parser (PRAGMA/SET forms); sets the
incremental bin-pack target file size.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task B: Refactor the snapshot-publish tail out of `commit_internal`

**Files:** Modify `crates/bluedb-lakehouse/src/writer.rs`.

Rationale: `commit_internal` and the new incremental commit both need the
"manifest-list → snapshot → metadata → write" tail. Extract it so the new path
reuses it (DRY, lower risk).

- [ ] **Step 1: Extract `publish_snapshot`** — add a private method holding the
  tail of `commit_internal` (from building `manifest_list_path` through
  `write_metadata`), parameterized by the final `manifests` vec and the
  `Operation`:

```rust
    /// Author one snapshot from an explicit set of `manifests` (already including
    /// any carried-forward + freshly-written manifest files), tag it `operation`
    /// + `watermark`, apply the table updates to our hosted metadata, and publish
    /// the next `metadata.json`.
    async fn publish_snapshot(
        &mut self,
        manifests: Vec<ManifestFile>,
        operation: Operation,
        watermark: i64,
    ) -> Result<()> {
        let snapshot_id = fresh_snapshot_id(&self.metadata);
        let next_seq = self.metadata.next_sequence_number();
        let parent_id = self.metadata.current_snapshot_id();

        let manifest_list_path = format!("{}/snap-{snapshot_id}.avro", self.metadata_dir());
        let mut mlw = ManifestListWriter::v2(
            self.file_io.new_output(&manifest_list_path)?,
            snapshot_id,
            parent_id,
            next_seq,
        );
        mlw.add_manifests(manifests.into_iter())?;
        mlw.close().await?;

        let summary = Summary {
            operation,
            additional_properties: std::collections::HashMap::from([(
                WATERMARK_PROP.to_string(),
                watermark.to_string(),
            )]),
        };
        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_id)
            .with_sequence_number(next_seq)
            .with_summary(summary)
            .with_schema_id(self.metadata.current_schema_id())
            .with_timestamp_ms(now_ms())
            .build();

        let current_md_loc = format!("{}/v{}.metadata.json", self.metadata_dir(), self.version);
        let result = self
            .metadata
            .clone()
            .into_builder(Some(current_md_loc))
            .add_snapshot(snapshot)?
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )?
            .build()?;
        self.metadata = result.metadata;
        self.version += 1;
        self.write_metadata(self.version).await?;
        Ok(())
    }
```

- [ ] **Step 2: Rewrite `commit_internal` to build its manifest set then call `publish_snapshot`.** Keep its existing carry-forward (non-replace) + new-data/new-delete manifest construction, but replace the tail (snapshot id / manifest list / snapshot builder / into_builder / write_metadata) with:

```rust
        let operation = if replace {
            Operation::Replace
        } else if self.metadata.current_snapshot().is_none() {
            Operation::Append
        } else {
            Operation::Overwrite
        };
        self.publish_snapshot(manifests, operation, watermark).await
```

Note: the per-manifest `add_file(df, -1)` writers stay in `commit_internal`; only the publish tail moves. The data/delete manifest writers must use a `snapshot_id` — to keep them consistent with the published snapshot, change them to derive the data/delete avro paths from `self.unique_suffix()` (already unique) instead of `snapshot_id`, since `publish_snapshot` now owns the snapshot id. (The manifest file path only needs to be unique; it does not need to equal the snapshot id.)

- [ ] **Step 3: Run existing writer tests → still pass** — `cargo test -p bluedb-lakehouse --test writer 2>&1` and `cargo test -p bluedb-lakehouse --lib 2>&1` (compact / full_crud / watermark / schema-evo all green).

- [ ] **Step 4: Commit**

```bash
git add crates/bluedb-lakehouse/src/writer.rs
git commit -F - <<'EOF'
refactor(compaction): extract publish_snapshot from commit_internal

Shared manifest-list -> snapshot -> metadata tail, so the incremental
compaction path can reuse it. Behavior unchanged.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task C: `compact_incremental` on the writer

**Files:** Modify `crates/bluedb-lakehouse/src/writer.rs` (+ a `data_bytes`/cohort helper) and add an integration test in `crates/bluedb-lakehouse/tests/`.

- [ ] **Step 1: Failing correctness test** — new `crates/bluedb-lakehouse/tests/incremental_compaction.rs`. Model the harness on `tests/writer.rs` (`docs_schema`, `row`, `read_back`). Drive several seals (small files) including an update and a delete, run `compact_incremental`, and assert merged contents are unchanged and large files are untouched:

```rust
// Uses LakehouseWriter::open_local + docs_schema/row/read_back copied from writer.rs harness.
#[tokio::test]
async fn incremental_compaction_preserves_merge_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1).await.unwrap();

    // Several small seals: inserts, an update (key 1), a delete (key 2).
    w.upsert(&[row(1, "a"), row(2, "b")]).await.unwrap(); w.commit_snapshot(1).await.unwrap();
    w.upsert(&[row(3, "c")]).await.unwrap();             w.commit_snapshot(2).await.unwrap();
    w.upsert(&[row(1, "A")]).await.unwrap();             w.commit_snapshot(3).await.unwrap(); // update
    w.delete(&[Key::I64(2)]).await.unwrap();             w.commit_snapshot(4).await.unwrap(); // delete

    let before = read_back(&w).await; // {1:"A", 3:"c"}  (2 deleted)
    let files_before = w.data_file_count().await.unwrap();

    // Target huge so every small file is a candidate (force a real bin-pack).
    w.compact_incremental(1 << 30, 4).await.unwrap();

    let after = read_back(&w).await;
    assert_eq!(after, before, "merge-on-read result must be unchanged");
    assert!(w.data_file_count().await.unwrap() < files_before, "files reduced");
}
```

(Add `use gluesql_core::data::Key;` to the test.)

- [ ] **Step 2: Run → fail** — `cargo test -p bluedb-lakehouse --test incremental_compaction 2>&1` (no `compact_incremental`).

- [ ] **Step 3: Implement `compact_incremental`** — in `writer.rs`:

```rust
    /// **Incremental (minor) compaction**: bin-pack the table's small data files
    /// into fewer, larger files without rewriting the whole table.
    ///
    /// Selects the per-seal data manifests whose total data bytes are below
    /// `target_bytes` (the bin-pack candidates; already-large files are left
    /// alone), scans just those — with all delete files applied, via
    /// [`Self::scan_manifest_subset`] — and commits an `Overwrite` that carries
    /// the survivor data manifests and **all** delete manifests forward intact
    /// (preserving their sequence numbers) while dropping the compacted cohort.
    /// The rewritten rows take the new (highest) sequence with deletes already
    /// materialized, so merge-on-read is preserved. No-op for <2 candidates.
    ///
    /// Delete files are *kept* (a delete may still target a survivor); the
    /// whole-table [`Self::compact`] reclaims them.
    pub async fn compact_incremental(&mut self, target_bytes: u64, watermark: i64) -> Result<()> {
        let (data_manifests, delete_manifests) = self.live_manifest_files().await?;

        // Candidate = a data manifest whose live data bytes are below target.
        let mut cohort = Vec::new();
        let mut survivors = Vec::new();
        for mf in data_manifests {
            if self.manifest_data_bytes(&mf).await? < target_bytes {
                cohort.push(mf);
            } else {
                survivors.push(mf);
            }
        }
        if cohort.len() < 2 {
            return Ok(()); // nothing worth merging
        }

        // Read the cohort's current rows (deletes applied) and re-write them.
        let mut scoped = cohort.clone();
        scoped.extend(delete_manifests.iter().cloned());
        let batches = self.scan_manifest_subset(scoped).await?;
        let mut writer = self.new_data_file_writer().await?;
        let mut wrote = false;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            writer.write(batch).await?;
            wrote = true;
        }
        let new_files = writer.close().await?;
        if !wrote {
            // The cohort merged to nothing (all rows deleted): just drop it.
            let mut keep = survivors;
            keep.extend(delete_manifests);
            return self.publish_snapshot(keep, Operation::Replace, watermark).await;
        }
        self.pending_data = new_files;

        // Keep survivors + all deletes; commit_incremental adds the new manifest.
        let mut keep = survivors;
        keep.extend(delete_manifests);
        self.commit_incremental(keep, watermark).await
    }

    /// Total live data bytes referenced by one data manifest.
    async fn manifest_data_bytes(&self, mf: &ManifestFile) -> Result<u64> {
        let manifest = mf.load_manifest(&self.file_io).await?;
        let mut bytes = 0;
        for entry in manifest.entries() {
            if entry.is_alive() && entry.content_type() == DataContentType::Data {
                bytes += entry.data_file().file_size_in_bytes();
            }
        }
        Ok(bytes)
    }

    /// Commit an incremental compaction: `keep` (survivor data manifests + all
    /// delete manifests, carried forward intact) plus a new data manifest built
    /// from `self.pending_data`.
    async fn commit_incremental(&mut self, keep: Vec<ManifestFile>, watermark: i64) -> Result<()> {
        let schema = self.metadata.current_schema().clone();
        let partition_spec = self.metadata.default_partition_spec().as_ref().clone();
        let mut manifests = keep;
        let data_files = std::mem::take(&mut self.pending_data);
        let path = format!("{}/data-{}.avro", self.metadata_dir(), self.unique_suffix());
        let mut mw = ManifestWriterBuilder::new(
            self.file_io.new_output(&path)?,
            Some(self.metadata.current_snapshot_id().unwrap_or(0)),
            None,
            schema,
            partition_spec,
        )
        .build_v2_data();
        for df in data_files {
            mw.add_file(df, -1)?;
        }
        manifests.push(mw.write_manifest_file().await?);
        self.publish_snapshot(manifests, Operation::Replace, watermark).await
    }
```

Note on `ManifestWriterBuilder::new` snapshot-id arg: it takes the *owning* snapshot id for the manifest's added entries; passing the current snapshot id (or the soon-to-be one) is fine — entries get the new snapshot's sequence at publish via `-1`. If the existing `commit_internal` passes a specific `snapshot_id`, mirror whatever it uses; the manifest file path just needs to be unique.

- [ ] **Step 4: Run → pass** — `cargo test -p bluedb-lakehouse --test incremental_compaction 2>&1`.

- [ ] **Step 5: Major still reclaims deletes (add test)** — append to the same file:

```rust
#[tokio::test]
async fn major_after_incremental_reclaims_delete_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let mut w = LakehouseWriter::open_local(root, "main", "docs", docs_schema(), 1).await.unwrap();
    w.upsert(&[row(1, "a")]).await.unwrap(); w.commit_snapshot(1).await.unwrap();
    w.upsert(&[row(1, "A")]).await.unwrap(); w.commit_snapshot(2).await.unwrap();
    w.upsert(&[row(2, "b")]).await.unwrap(); w.commit_snapshot(3).await.unwrap();
    w.compact_incremental(1 << 30, 3).await.unwrap();
    // Incremental keeps delete files:
    assert!(w.delete_file_count().await.unwrap() > 0);
    w.compact(3).await.unwrap(); // major
    assert_eq!(w.delete_file_count().await.unwrap(), 0, "major reclaims deletes");
    let after = read_back(&w).await;
    assert_eq!(after.get(&1).map(String::as_str), Some("A"));
    assert_eq!(after.get(&2).map(String::as_str), Some("b"));
}
```

Add `delete_file_count` to `writer.rs` (mirror of `data_file_count` but `content_type() == DataContentType::EqualityDeletes`):

```rust
    /// Number of live equality-delete files in the current table state.
    pub async fn delete_file_count(&self) -> Result<usize> {
        let Some(snapshot) = self.metadata.current_snapshot() else { return Ok(0); };
        let metadata_ref = Arc::new(self.metadata.clone());
        let list = snapshot.load_manifest_list(&self.file_io, &metadata_ref).await?;
        let mut count = 0;
        for mf in list.entries() {
            let manifest = mf.load_manifest(&self.file_io).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::EqualityDeletes {
                    count += 1;
                }
            }
        }
        Ok(count)
    }
```

- [ ] **Step 6: Run → pass** — `cargo test -p bluedb-lakehouse --test incremental_compaction 2>&1` (both tests).

- [ ] **Step 7: Commit**

```bash
git add crates/bluedb-lakehouse/src/writer.rs crates/bluedb-lakehouse/tests/incremental_compaction.rs
git commit -F - <<'EOF'
feat(compaction): incremental bin-pack of small data files

compact_incremental selects sub-target per-seal data manifests, rewrites their
current rows via the scoped merge-on-read scan, and commits an Overwrite that
keeps survivors + all deletes intact (sequence-preserving) and drops the cohort.
delete_file_count added; major compaction still reclaims deletes.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task D: Engine — target-size registry, incremental compaction, policy

**Files:** Modify `crates/bluedb-lakehouse/src/engine.rs`.

- [ ] **Step 1: Failing test** — add `crates/bluedb-lakehouse/tests/` coverage or extend `tests/engine.rs`: a test that sets the target via `apply_pragma(LhPragma::TargetFileBytes(n))`, reopens the engine, and asserts the value persisted (`target_file_bytes()` accessor returns `Some(n)`). Also a `compact_incremental(table)` round-trip mirroring the writer test but through the engine + CDC seal path.

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement** — in `engine.rs`:
  - `Registry` gains `#[serde(default)] target_file_bytes: Option<u64>`.
  - `pub async fn set_target_file_bytes(&self, bytes: u64) -> Result<()>` — write state + `persist_registry`.
  - `pub fn target_file_bytes(&self) -> Option<u64>` — read state.
  - `apply_pragma` arm: `LhPragma::TargetFileBytes(n) => self.set_target_file_bytes(n).await`.
  - `pub async fn compact_incremental(&self, table: &str) -> Result<()>` — like `compact` but resolves the target (`self.target_file_bytes().unwrap_or_else(default_target_bytes)`) and calls `writer.compact_incremental(target, watermark)`.
  - `pub async fn delete_file_count(&self, table) -> Result<usize>` — open writer → `delete_file_count()`.
  - `fn default_target_bytes() -> u64` — env `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` else `128 * 1024 * 1024`.

- [ ] **Step 4: Run → pass.**

- [ ] **Step 5: Commit**

```bash
git add crates/bluedb-lakehouse/src/engine.rs crates/bluedb-lakehouse/tests/engine.rs
git commit -F - <<'EOF'
feat(compaction): engine incremental compaction + durable target size

Registry persists per-tenant target_file_bytes (set via PRAGMA through
apply_pragma); compact_incremental + delete_file_count exposed; default target
from BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES or 128 MiB.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task E: Manager — minor/major policy in the shared loop

**Files:** Modify `crates/bluedb-lakehouse/src/manager.rs` (+ `crates/bluedb-server/src/lib.rs` for the env knob).

- [ ] **Step 1: Failing test** — extend `tests/` manager coverage (or add one): with `max_delete_files` low, a table with many delete files triggers a major (delete files → 0 after); otherwise minor (data files reduced, delete files kept). If the manager loop is hard to drive deterministically in a test, test the *decision* via a small extracted helper `fn choose_compaction(data_files, delete_files, cfg) -> Compaction { None, Minor, Major }` and unit-test that.

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement** — `LakehouseConfig` gains `max_delete_files: usize`. Rewrite `compact_all`'s per-table arm:

```rust
                let deletes = engine.delete_file_count(&table).await.unwrap_or(0);
                let datas = engine.data_file_count(&table).await.unwrap_or(0);
                let result = if deletes > self.cfg.max_delete_files {
                    engine.compact(&table).await            // major: reclaim deletes
                } else if datas > self.cfg.max_data_files {
                    engine.compact_incremental(&table).await // minor: bin-pack
                } else {
                    Ok(())
                };
                if let Err(err) = result { /* eprintln as today */ }
```

(Extract `choose_compaction` if you went the unit-test route in Step 1; keep the I/O in `compact_all`.)

- [ ] **Step 4: Server env knob** — in `crates/bluedb-server/src/lib.rs`, add `BLUEDB_LAKEHOUSE_MAX_DELETE_FILES` (default e.g. 16) next to `max_data_files`, and pass it into `LakehouseConfig`.

- [ ] **Step 5: Run → pass** (`cargo test -p bluedb-lakehouse 2>&1`, `cargo test -p bluedb-server 2>&1`).

- [ ] **Step 6: Commit**

```bash
git add crates/bluedb-lakehouse/src/manager.rs crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(compaction): worker picks minor (bin-pack) vs major (reclaim deletes)

compact_all runs major when delete files exceed BLUEDB_LAKEHOUSE_MAX_DELETE_FILES,
else incremental when data files exceed max_data_files.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task F: Server PRAGMA e2e

**Files:** add a test in `crates/bluedb-server/tests/lakehouse.rs`.

- [ ] **Step 1: Test** — `PRAGMA lakehouse_target_file_bytes = N` over `/sql` returns success and (via a reopened state or an engine accessor exposed for test) the value is durable; a write→seal cycle still reads back correctly through the REST catalog. Model on the existing `lakehouse.rs` harness (`make_state`, `call`, `seal_now`).

- [ ] **Step 2: Run → fail/iterate → pass** — `cargo test -p bluedb-server --test lakehouse 2>&1`.

- [ ] **Step 3: Commit**

```bash
git add crates/bluedb-server/tests/lakehouse.rs
git commit -F - <<'EOF'
test(compaction): e2e — lakehouse_target_file_bytes PRAGMA over /sql is durable

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task G: Docs

**Files:** `docs/lakehouse/iceberg-mirror.md`.

- [ ] **Step 1:** Expand the "compaction" bullet under "How it works" into a short subsection describing minor (incremental bin-pack of small files, leaves large files alone) vs major (whole-table rewrite that reclaims delete files), and add the `PRAGMA lakehouse_target_file_bytes` to the Enabling/PRAGMA area. Add `BLUEDB_LAKEHOUSE_TARGET_FILE_BYTES` and `BLUEDB_LAKEHOUSE_MAX_DELETE_FILES` to the Configuration table. Replace the "Compaction rewrites the whole table" limitation bullet with a one-line note that incremental compaction is in place (delete reclamation still needs the periodic major pass).

- [ ] **Step 2: Commit**

```bash
git add docs/lakehouse/iceberg-mirror.md
git commit -F - <<'EOF'
docs(compaction): document minor/major compaction + target-size PRAGMA

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
```

---

### Task H: Full workspace verification

- [ ] **Step 1:** `cargo test --workspace 2>&1` → all green.
- [ ] **Step 2:** `cargo clippy -p bluedb-sql -p bluedb-lakehouse -p bluedb-server --tests 2>&1` → no new warnings on touched files. (No `cargo fmt`.)
- [ ] **Step 3:** commit any fixes.

---

## Self-Review

**Spec coverage:** minor compaction (Task C, using the spike's `scan_manifest_subset`), major retained + policy (Task E), PRAGMA target (Tasks A/D/F), memory bound (rolling writer in Task C), docs (Task G). ✓

**Placeholder scan:** Tasks D/E/F reference "model on existing harness / extend tests/engine.rs / tests/lakehouse.rs" — those are real instructions to copy concrete existing harnesses; the assertions are specified. No `TODO`/`TBD`.

**Type consistency:** `LhPragma::TargetFileBytes(u64)` (A) → `apply_pragma` arm (D) → `set_target_file_bytes(u64)`/`target_file_bytes()->Option<u64>` (D) → `compact_incremental(target_bytes: u64, watermark)` (C). `publish_snapshot(Vec<ManifestFile>, Operation, i64)` (B) reused by `commit_internal` (B) and `commit_incremental` (C). `delete_file_count()->usize` (C) used by manager policy (E). Consistent.

**Verification points to confirm during execution (don't assume):**
1. `commit_internal`'s exact data/delete manifest-writer construction (snapshot-id arg, avro path) — mirror it in `commit_incremental` (Task C Step 3).
2. `ManifestFile: Clone` (needed for cohort/survivor cloning) — confirm; if not, restructure to move.
3. The manager loop's testability — fall back to the extracted `choose_compaction` unit test (Task E Step 1).
