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

use bluedb_rest::Param;
use bluedb_sql::{CommitObserver, RowChange, SlateDbStorage};
use gluesql_core::data::{Key, Value as GValue};
use gluesql_core::prelude::{Glue, Payload};
use gluesql_core::store::{DataRow, Store};

use crate::error::{EngineError, Result};
use crate::fts_sql::{extract_fts_predicate, rewrite_fts_query};
use crate::live_segment::LiveSegment;
use crate::rest_sql;

/// One declared fulltext index: the indexed text column, the table's integer
/// primary-key column, the text column's ordinal in the schema'd row, and the
/// live segment that holds its terms.
struct IndexDef {
    column: String,
    pk_column: String,
    column_ordinal: usize,
    segment: Arc<LiveSegment>,
}

/// Maintains in-memory FTS live segments in lock-step with SQL commits and
/// rewrites `@@` reads against them (Spec B §4.2/§4.3, §5 read-your-writes).
/// B2c: live-segment-only (no durable split union yet), `INTEGER PRIMARY KEY`
/// tables only.
pub struct FtsEngine {
    /// `table` → its fulltext indexes.
    indexes: RwLock<HashMap<String, Vec<IndexDef>>>,
}

impl FtsEngine {
    /// A fresh engine with no indexes. Returns an `Arc` so the same engine can be
    /// installed as a [`CommitObserver`] on write connections and queried for
    /// rewrites on read connections (the shared state that makes
    /// read-your-writes work across connections).
    #[allow(clippy::new_without_default)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            indexes: RwLock::new(HashMap::new()),
        })
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
        let segment = Arc::new(LiveSegment::new(analyzer)?);
        let def = IndexDef {
            column: text_column.to_string(),
            pk_column: pk_column.to_string(),
            column_ordinal: ordinal,
            segment,
        };
        self.indexes
            .write()
            .unwrap()
            .entry(table.to_string())
            .or_default()
            .push(def);
        Ok(())
    }

    /// Rewrite a `@@` query against the matching live segment. `Ok(None)` when
    /// the SQL has no `@@`.
    pub async fn rewrite_for(&self, sql: &str) -> Result<Option<String>> {
        let Some(pred) = extract_fts_predicate(sql)? else {
            return Ok(None);
        };
        // Clone the segment Arc + pk_column out of the read guard and drop the
        // guard BEFORE the await (never hold a std RwLock guard across .await).
        let (segment, pk_column) = {
            let idx = self.indexes.read().unwrap();
            match idx
                .get(&pred.table)
                .and_then(|v| v.iter().find(|d| d.column == pred.column))
            {
                Some(def) => (def.segment.clone(), def.pk_column.clone()),
                None => {
                    return Err(EngineError::Rejected(format!(
                        "no fulltext index on {}.{}",
                        pred.table, pred.column
                    )))
                }
            }
        };
        rewrite_fts_query(sql, &pk_column, &*segment).await
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
