//! `FtsEngine` — maintains in-memory FTS live segments in lock-step with SQL
//! commits and rewrites `@@` reads against them (Spec B §4.2/§4.3, §5
//! read-your-writes).
//!
//! It is the crate that *composes* the B1 rewrite ([`extract_fts_predicate`] /
//! [`rewrite_fts_query`]) with the B2a [`LiveSegment`] and the B2c commit tap
//! ([`CommitObserver`]):
//!
//! - **DDL**: [`FtsEngine::create_fulltext_index`] declares a fulltext index on
//!   `table.text_column`, resolving the text column's ordinal from the live
//!   schema and allocating a fresh [`LiveSegment`].
//! - **Write path**: [`FtsEngine`] implements [`CommitObserver`]. Installed on a
//!   write connection (`connection().with_commit_observer(engine)`), it routes
//!   each committed [`RowChange`] to the matching segment — `Key::I64(pk)` + the
//!   indexed text column → [`LiveSegment::index`]; a delete → [`LiveSegment::tombstone`].
//! - **Read path**: [`FtsEngine::rewrite_for`] picks the segment by the
//!   predicate's `table`/`column` and runs [`rewrite_fts_query`];
//!   [`FtsEngine::execute_fts`] rewrites-or-passes-through, then runs the SQL on
//!   the parameterized single-DML surface.
//!
//! Because the segment is *shared* across connections (one `Arc<FtsEngine>`),
//! a row committed on one connection is visible to a `@@` query on another with
//! no explicit flush — read-your-writes through SQL.
//!
//! **B2c restrictions** (documented): live-segment-only (no durable-split union
//! yet — a process restart loses the in-memory index until B4's seal/replay);
//! `INTEGER PRIMARY KEY` + schema'd [`DataRow::Vec`] tables only (so the row
//! `Key` is `Key::I64` equal to the pk column value); index definitions are
//! in-memory (a durable registry is B2b).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bluedb_fts::mapping::IndexMapping;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_fts::IdField;
use bluedb_rest::Param;
use bluedb_sql::{CommitObserver, RowChange, SlateDbStorage};
use bluedb_storage::{BlobStore, BlobStoreMut, SlateDbBlobStore, Substrate};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use gluesql_core::data::{Key, Value as GValue};
use gluesql_core::prelude::{Glue, Payload};
use gluesql_core::store::{DataRow, Store};
use tantivy::schema::Field;
use tantivy::TantivyDocument;

use crate::error::{EngineError, Result};
use crate::fts::FtsIndex;
use crate::fts_sql::{extract_fts_predicate, rewrite_fts_query, FtsHit, FtsPredicate, FtsSearcher};
use crate::live_segment::{analyzer_for_config, translate_query, LiveSegment};
use crate::rest_sql;

/// Default result cap for the union search (mirrors `live_segment`'s
/// `DEFAULT_LIMIT`; the over-fetch window sizing per Spec B §9 is refined later).
const UNION_LIMIT: usize = 100;

/// Fixed blob key for the durable FTS registry — one JSON document holding the
/// full `Vec<PersistedDef>` of declared indexes. Read on [`FtsEngine::reopen`],
/// rewritten on every `create_fulltext_index` (durable mode only).
const FTS_REGISTRY_KEY: &str = "fts/_registry";

/// The persistable shape of one fulltext-index declaration. Persisting the
/// `column_ordinal` (stable for a table's lifetime) means [`FtsEngine::reopen`]
/// rebuilds the in-memory [`IndexDef`] with NO schema fetch.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedDef {
    table: String,
    column: String,
    column_ordinal: usize,
    pk_column: String,
    analyzer: String,
}

/// One declared fulltext index: the indexed text column, the table's integer
/// primary-key column, the text column's ordinal in the schema'd row, the live
/// segment that holds its terms, and (when the engine has a durable backing) the
/// durable [`FtsIndex`] tier the live segment seals into.
struct IndexDef {
    column: String,
    pk_column: String,
    column_ordinal: usize,
    /// The `to_tsvector` config string this index was declared with. Stored so
    /// the def can round-trip through the durable registry (reopen rebuilds the
    /// live segment + durable mapping from it without a schema fetch).
    analyzer: String,
    segment: Arc<LiveSegment>,
    /// The durable tier (object-storage splits). `None` for an in-memory-only
    /// engine ([`FtsEngine::new`]); then the union searcher is live-only.
    durable: Option<Arc<FtsIndex>>,
    /// The durable index's `body` field (for [`FtsIndex::search_ids`]). Only
    /// meaningful when `durable` is `Some`.
    durable_body_field: Option<Field>,
}

/// Maintains in-memory FTS live segments in lock-step with SQL commits and
/// rewrites `@@` reads against them (Spec B §4.2/§4.3, §5 read-your-writes).
/// B2c: live-segment-only (no durable split union yet), `INTEGER PRIMARY KEY`
/// tables only.
pub struct FtsEngine {
    /// `table` → its fulltext indexes.
    indexes: RwLock<HashMap<String, Vec<IndexDef>>>,
    /// Optional durable backing. When `Some`, each fulltext index gets a durable
    /// [`FtsIndex`] tier (object-storage splits over this blob store) that the
    /// live segment seals into; the union searcher then merges live ∪ durable.
    /// `None` = pure in-memory (the union is live-only) — the shape existing
    /// `ryw.rs`/`fts_engine` tests use.
    blob: Option<Arc<SlateDbBlobStore>>,
}

impl FtsEngine {
    /// A fresh in-memory-only engine with no indexes. Returns an `Arc` so the
    /// same engine can be installed as a [`CommitObserver`] on write connections
    /// and queried for rewrites on read connections (the shared state that makes
    /// read-your-writes work across connections). No durable tier — the union
    /// searcher is live-only.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            indexes: RwLock::new(HashMap::new()),
            blob: None,
        })
    }

    /// A durable engine backed by `substrate` (the same object-storage substrate
    /// as the SQL data — `Database::substrate()`). Each fulltext index declared
    /// on this engine gets a durable [`FtsIndex`] tier; [`Self::seal`] folds the
    /// live segment into it, and the union searcher merges live ∪ durable.
    pub fn new_durable(substrate: Substrate) -> Arc<Self> {
        Arc::new(Self {
            indexes: RwLock::new(HashMap::new()),
            blob: Some(Arc::new(SlateDbBlobStore::from_substrate(substrate))),
        })
    }

    /// Reopen a durable engine over `substrate`, rebuilding its in-memory index
    /// defs from the durable registry. For each persisted def the live segment is
    /// allocated fresh (empty until new writes / a replay) and the durable
    /// [`FtsIndex`] tier reconnects to its existing splits via the stable
    /// `fts/{table}/{column}` index_id — so a `@@` query served by the reopened
    /// engine finds the sealed data (Spec B B4-3, restart durability).
    ///
    /// The un-sealed live window (writes since the last seal) is NOT replayed
    /// here — SQL is the source of truth and rebuilding that tail on failover is
    /// the deferred HA-M4 concern.
    pub async fn reopen(substrate: Substrate) -> Result<Arc<Self>> {
        let blob = Arc::new(SlateDbBlobStore::from_substrate(substrate));
        let persisted = Self::load_registry(&blob).await?;

        let engine = Arc::new(Self {
            indexes: RwLock::new(HashMap::new()),
            blob: Some(blob),
        });

        for pd in persisted {
            // No schema fetch: the persisted ordinal/pk are authoritative.
            let def = engine.build_index_def(
                &pd.table,
                &pd.column,
                &pd.pk_column,
                pd.column_ordinal,
                &pd.analyzer,
            )?;
            let mut idx = engine.indexes.write().unwrap();
            let defs = idx.entry(pd.table.clone()).or_default();
            defs.retain(|d| d.column != pd.column);
            defs.push(def);
        }

        Ok(engine)
    }

    /// Load the durable registry: `get_all` the registry blob and deserialize the
    /// `Vec<PersistedDef>`. An absent blob (a never-yet-persisted engine) is an
    /// empty registry — mirrors [`FtsIndex::load_manifest`]'s present/absent
    /// handling (a parse error IS propagated; only absence resets to empty).
    async fn load_registry(blob: &SlateDbBlobStore) -> Result<Vec<PersistedDef>> {
        match blob.get_all(FTS_REGISTRY_KEY).await {
            Ok(bytes) => serde_json::from_slice::<Vec<PersistedDef>>(&bytes)
                .map_err(|e| EngineError::Other(e.into())),
            Err(_) => Ok(Vec::new()),
        }
    }

    /// Rewrite the durable registry blob from the current in-memory def map.
    /// Durable mode only — a no-op without a blob ([`FtsEngine::new`]). The map is
    /// keyed by table → its defs, so the persisted list is inherently deduped by
    /// (table, column).
    async fn persist_registry(&self) -> Result<()> {
        let Some(blob) = &self.blob else {
            return Ok(());
        };
        let defs: Vec<PersistedDef> = {
            let idx = self.indexes.read().unwrap();
            idx.iter()
                .flat_map(|(table, defs)| {
                    defs.iter().map(move |d| PersistedDef {
                        table: table.clone(),
                        column: d.column.clone(),
                        column_ordinal: d.column_ordinal,
                        pk_column: d.pk_column.clone(),
                        analyzer: d.analyzer.clone(),
                    })
                })
                .collect()
        };
        let bytes = serde_json::to_vec(&defs).map_err(|e| EngineError::Other(e.into()))?;
        blob.put(FTS_REGISTRY_KEY, Bytes::from(bytes)).await?;
        Ok(())
    }

    /// Declare a fulltext index on `table.text_column`, with `pk_column` the
    /// table's integer primary key and `analyzer` a `to_tsvector` config string.
    /// Resolves the text column's ordinal from the live schema.
    pub async fn create_fulltext_index(
        &self,
        storage: &SlateDbStorage,
        table: &str,
        text_column: &str,
        pk_column: &str,
        analyzer: &str,
    ) -> Result<()> {
        let schema = Store::fetch_schema(storage, table)
            .await
            .map_err(EngineError::from)?
            .ok_or_else(|| EngineError::Rejected(format!("no such table: {table}")))?;
        let cols = schema.column_defs.ok_or_else(|| {
            EngineError::Rejected(format!(
                "table {table} is schemaless; FTS needs a column schema"
            ))
        })?;
        let ordinal = cols
            .iter()
            .position(|c| c.name == text_column)
            .ok_or_else(|| {
                EngineError::Rejected(format!("no column {text_column} on {table}"))
            })?;

        let def = self.build_index_def(table, text_column, pk_column, ordinal, analyzer)?;
        // Replace any prior def for this (table, column) — a re-create supersedes
        // rather than duplicating, keeping the in-memory map (and thus the durable
        // registry derived from it) deduped.
        {
            let mut idx = self.indexes.write().unwrap();
            let defs = idx.entry(table.to_string()).or_default();
            defs.retain(|d| d.column != text_column);
            defs.push(def);
        }
        // Durable mode only: rewrite the registry blob so the def survives a
        // restart. A no-op without a blob (`FtsEngine::new`).
        self.persist_registry().await?;
        Ok(())
    }

    /// Build an [`IndexDef`] for `table.text_column` (pk `pk_column`, text-column
    /// `ordinal`, `analyzer` config). Allocates a fresh live segment and, in
    /// durable mode, the durable [`FtsIndex`] tier over the stable
    /// `fts/{table}/{column}` index_id — the SAME builder both `create_*` and
    /// [`Self::reopen`] use, so a reopened def reconnects to existing splits.
    fn build_index_def(
        &self,
        table: &str,
        text_column: &str,
        pk_column: &str,
        ordinal: usize,
        analyzer: &str,
    ) -> Result<IndexDef> {
        let segment = Arc::new(LiveSegment::new(analyzer)?);

        // When the engine is durable, build the durable tier: a STORED keyword
        // `id` (the pk, as a string) + a `body` text field with the SAME analyzer
        // as the live segment, so both tiers tokenize identically. The durable
        // index_id is stable per index (`fts/{table}/{column}`), so a re-created
        // FtsIndex over the same id reconnects to existing splits.
        let (durable, durable_body_field) = if let Some(blob) = &self.blob {
            let an = analyzer_for_config(analyzer);
            let mapping = IndexMapping::new().keyword("id").text("body", an);
            let schema = mapping.build_schema();
            let id_field = schema
                .get_field("id")
                .map_err(|e| EngineError::Other(e.into()))?;
            let body_field = schema
                .get_field("body")
                .map_err(|e| EngineError::Other(e.into()))?;
            let index_id = format!("fts/{table}/{text_column}");
            let index = Arc::new(FtsIndex::new(
                index_id,
                blob.clone(),
                schema,
                IdField(id_field),
                CompactionPolicy::default(),
            ));
            (Some(index), Some(body_field))
        } else {
            (None, None)
        };

        Ok(IndexDef {
            column: text_column.to_string(),
            pk_column: pk_column.to_string(),
            column_ordinal: ordinal,
            analyzer: analyzer.to_string(),
            segment,
            durable,
            durable_body_field,
        })
    }

    /// Like [`Self::create_fulltext_index`] but resolves the table's primary-key
    /// column from its schema (the column flagged `is_primary`). Errors if the
    /// table is missing, schemaless, or has no single primary-key column.
    pub async fn create_fulltext_index_auto(
        &self,
        storage: &SlateDbStorage,
        table: &str,
        text_column: &str,
        analyzer: &str,
    ) -> Result<()> {
        let schema = Store::fetch_schema(storage, table)
            .await
            .map_err(EngineError::from)?
            .ok_or_else(|| EngineError::Rejected(format!("no such table: {table}")))?;
        let cols = schema.column_defs.as_ref().ok_or_else(|| {
            EngineError::Rejected(format!(
                "table {table} is schemaless; FTS needs a column schema"
            ))
        })?;
        let pk_column = cols
            .iter()
            .find(|c| {
                matches!(
                    c.unique,
                    Some(gluesql_core::ast::ColumnUniqueOption { is_primary: true })
                )
            })
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                EngineError::Rejected(format!(
                    "table {table} has no primary key; fulltext index requires an integer primary key"
                ))
            })?;
        self.create_fulltext_index(storage, table, text_column, &pk_column, analyzer)
            .await
    }

    /// Rewrite a `@@` query against the matching index — the **union** of its
    /// live segment and (if durable) its durable splits, with the live tier
    /// authoritative for any pk it covers. `Ok(None)` when the SQL has no `@@`.
    pub async fn rewrite_for(&self, sql: &str) -> Result<Option<String>> {
        let Some(pred) = extract_fts_predicate(sql)? else {
            return Ok(None);
        };
        // Snapshot the def's handles out of the read guard and drop the guard
        // BEFORE any await (never hold a std RwLock guard across .await).
        let (segment, durable, durable_body_field, pk_column) = {
            let idx = self.indexes.read().unwrap();
            match idx
                .get(&pred.table)
                .and_then(|v| v.iter().find(|d| d.column == pred.column))
            {
                Some(def) => (
                    def.segment.clone(),
                    def.durable.clone(),
                    def.durable_body_field,
                    def.pk_column.clone(),
                ),
                None => {
                    return Err(EngineError::Rejected(format!(
                        "no fulltext index on {}.{}",
                        pred.table, pred.column
                    )))
                }
            }
        };

        let merged = union_hits(&segment, durable.as_deref(), durable_body_field, &pred).await?;
        rewrite_fts_query(sql, &pk_column, &PrecomputedSearcher { hits: merged }).await
    }

    /// Execute `sql`: rewrite `@@`/`ts_rank` against the live segment if present,
    /// else run unchanged. Goes through the parameterized single-DML surface.
    pub async fn execute_fts(
        &self,
        glue: &mut Glue<SlateDbStorage>,
        sql: &str,
        params: &[Param],
    ) -> Result<Vec<Payload>> {
        let rewritten = self.rewrite_for(sql).await?;
        let final_sql = rewritten.as_deref().unwrap_or(sql);
        rest_sql::execute_sql(glue, final_sql, params, false).await
    }

    /// Fold every durable index's live segment into its durable tier, off the
    /// commit path.
    ///
    /// For each fulltext index with a durable tier: drain the live segment's
    /// `(pk, body)` docs + tombstone set ([`LiveSegment::drain_for_seal`]),
    /// re-add the docs to the durable index ([`FtsIndex::update`], superseding any
    /// prior durable copy), and tombstone the deleted pks ([`FtsIndex::delete`]).
    /// After a seal the live segment is empty; subsequent `@@` reads of the
    /// sealed pks come from the durable tier.
    ///
    /// The engine read lock is held ONLY to snapshot the `Arc` handles — the
    /// async drain/update/delete run without it (never hold a std `RwLock` guard
    /// across `.await`).
    pub async fn seal(&self) -> Result<()> {
        // Snapshot (segment, durable) for every durable index out of the lock.
        let targets: Vec<(Arc<LiveSegment>, Arc<FtsIndex>)> = {
            let idx = self.indexes.read().unwrap();
            idx.values()
                .flat_map(|defs| defs.iter())
                .filter_map(|def| def.durable.clone().map(|d| (def.segment.clone(), d)))
                .collect()
        };

        for (segment, durable) in targets {
            let (docs, tombs) = segment.drain_for_seal()?;

            // Re-add live docs (id = pk.to_string(), body), superseding any prior
            // durable copy of that pk.
            if !docs.is_empty() {
                let old_ids: Vec<String> = docs.iter().map(|(pk, _)| pk.to_string()).collect();
                let tantivy_docs: Vec<TantivyDocument> = docs
                    .iter()
                    .map(|(pk, body)| durable_doc(&durable, *pk, body))
                    .collect::<Result<Vec<_>>>()?;
                durable.update(old_ids, tantivy_docs).await?;
            }

            // Tombstone the deleted pks in the durable tier.
            if !tombs.is_empty() {
                let dead: Vec<String> = tombs.iter().map(|pk| pk.to_string()).collect();
                durable.delete(dead).await?;
            }
        }
        Ok(())
    }
}

/// Build a durable [`TantivyDocument`] (`id` = `pk.to_string()`, `body`) for the
/// durable index `durable`, resolving the `id`/`body` fields from its schema.
fn durable_doc(durable: &FtsIndex, pk: i64, body: &str) -> Result<TantivyDocument> {
    let schema = durable.schema();
    let id_field = schema
        .get_field("id")
        .map_err(|e| EngineError::Other(e.into()))?;
    let body_field = schema
        .get_field("body")
        .map_err(|e| EngineError::Other(e.into()))?;
    let mut d = TantivyDocument::default();
    d.add_text(id_field, pk.to_string());
    d.add_text(body_field, body);
    Ok(d)
}

/// The merged hits for `pred` across the live segment and (if present) the
/// durable tier: live hits ∪ (durable hits whose pk the live segment does NOT
/// cover), sorted by descending score and truncated to [`UNION_LIMIT`].
///
/// Both tiers parse the SAME translated query string (Task 2's `translate_query`
/// emits explicit operators, so neither tier needs `set_conjunction_by_default`).
/// The live segment is authoritative: any pk it covers (the latest version, or a
/// tombstone) masks the corresponding durable hit, so a stale durable copy never
/// surfaces. Because the covered-mask guarantees disjoint pks, no extra dedup is
/// needed when merging.
async fn union_hits(
    segment: &LiveSegment,
    durable: Option<&FtsIndex>,
    durable_body_field: Option<Field>,
    pred: &FtsPredicate,
) -> Result<Vec<FtsHit>> {
    // Live tier (already filters its own tombstones).
    let mut hits = LiveSegment::search(segment, &pred.query, pred.kind, UNION_LIMIT)?;

    // Durable tier, masked by the live segment's covered set.
    if let (Some(durable), Some(body_field)) = (durable, durable_body_field) {
        let translated = translate_query(&pred.query, pred.kind);
        let covered = segment.covered();
        let durable_hits = durable
            .search_ids(&translated, &[body_field], UNION_LIMIT)
            .await?;
        for (id, score) in durable_hits {
            // The durable id is the pk rendered as a decimal string.
            let Ok(pk) = id.parse::<i64>() else { continue };
            if covered.contains(&pk) {
                continue; // live wins for this pk
            }
            hits.push(FtsHit { pk, score });
        }
    }

    // Merge by descending score, then truncate. Disjoint pks (the covered-mask),
    // so no dedup needed.
    hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    hits.truncate(UNION_LIMIT);
    Ok(hits)
}

/// A trivial [`FtsSearcher`] that returns a precomputed set of hits — the
/// already-merged live ∪ durable union. Lets [`rewrite_fts_query`] (which takes
/// `&impl FtsSearcher`) consume the union without changing its signature (the
/// B1/B2c tests depend on that signature).
struct PrecomputedSearcher {
    hits: Vec<FtsHit>,
}

#[async_trait::async_trait]
impl FtsSearcher for PrecomputedSearcher {
    async fn search(&self, _predicate: &FtsPredicate) -> Result<Vec<FtsHit>> {
        Ok(self.hits.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bluedb_sql::Database;
    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    /// `create_fulltext_index_auto` resolves the PK column from the schema, so a
    /// `@@` query after an observed insert finds the matching row by its `id` pk
    /// — i.e. it registered an index whose pk_column is `id` without being told.
    #[tokio::test]
    async fn auto_resolves_pk_column_and_indexes() {
        let db = Arc::new(Db::open("auto-pk", Arc::new(InMemory::new())).await.unwrap());
        let database = Database::new(db);
        let fts = FtsEngine::new();

        {
            let mut g = Glue::new(database.connection_serialized());
            g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
                .await
                .unwrap();
        }

        // No pk_column argument — resolved from the schema's is_primary column.
        fts.create_fulltext_index_auto(&database.connection(), "docs", "body", "english")
            .await
            .unwrap();

        {
            let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
            g.execute(
                "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny skies');",
            )
            .await
            .unwrap();
        }

        let mut g = Glue::new(database.connection_serialized());
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";
        let out = fts.execute_fts(&mut g, sql, &[]).await.unwrap();
        match out.into_iter().next().unwrap() {
            Payload::Select { rows, .. } => {
                let ids: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
                assert_eq!(
                    ids,
                    vec![GValue::I64(1)],
                    "auto-resolved pk fetched the matching row by id IN (...)"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    /// A table with no primary key is rejected: FTS needs an integer pk.
    #[tokio::test]
    async fn rejects_table_without_primary_key() {
        let db = Arc::new(Db::open("no-pk", Arc::new(InMemory::new())).await.unwrap());
        let database = Database::new(db);
        let fts = FtsEngine::new();

        {
            let mut g = Glue::new(database.connection_serialized());
            g.execute("CREATE TABLE notes (id INTEGER, body TEXT);")
                .await
                .unwrap();
        }

        let err = fts
            .create_fulltext_index_auto(&database.connection(), "notes", "body", "english")
            .await
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Rejected(_)),
            "no-PK table must be Rejected, got {err:?}"
        );
    }

    /// A missing table is rejected (no schema to resolve a pk from).
    #[tokio::test]
    async fn rejects_missing_table() {
        let db = Arc::new(Db::open("missing", Arc::new(InMemory::new())).await.unwrap());
        let database = Database::new(db);
        let fts = FtsEngine::new();

        let err = fts
            .create_fulltext_index_auto(&database.connection(), "ghost", "body", "english")
            .await
            .unwrap_err();
        assert!(
            matches!(err, EngineError::Rejected(_)),
            "missing table must be Rejected, got {err:?}"
        );
    }
}

impl CommitObserver for FtsEngine {
    fn on_commit(&self, changes: &[RowChange]) {
        let idx = self.indexes.read().unwrap();
        for ch in changes {
            let Some(defs) = idx.get(&ch.table) else {
                continue;
            };
            // B2c: integer pk only (INTEGER PRIMARY KEY → Key::I64).
            let Key::I64(pk) = ch.key else { continue };
            for def in defs {
                match &ch.row {
                    Some(DataRow::Vec(values)) => {
                        if let Some(GValue::Str(text)) = values.get(def.column_ordinal) {
                            if let Err(e) = def.segment.index(pk, text) {
                                eprintln!(
                                    "bluedb-fts: live index failed for {}.{} pk={pk}: {e}",
                                    ch.table, def.column
                                );
                            }
                        }
                    }
                    // Schemaless rows are unsupported in B2c (no column ordinal).
                    Some(DataRow::Map(_)) => {}
                    None => {
                        if let Err(e) = def.segment.tombstone(pk) {
                            eprintln!(
                                "bluedb-fts: live tombstone failed for {}.{} pk={pk}: {e}",
                                ch.table, def.column
                            );
                        }
                    }
                }
            }
        }
    }
}
