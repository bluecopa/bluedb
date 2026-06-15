//! `bluedb-engine` — the facade that unifies bluedb's pillars behind one API.
//!
//! The pillars are deliberately decoupled crates:
//! - [`bluedb_sql`] — SQL over SlateDB (transactions, indexes, tenants);
//! - [`bluedb_rest`] — PostgREST-style DSL → SQL *string* translation;
//! - [`bluedb_fts`] — BM25 full-text search + the index lifecycle pieces;
//! - [`bluedb_storage`] — the SlateDB blob seam.
//!
//! `bluedb-engine` is the one crate that *composes* them, so the `bluedb-server`
//! HTTP service wraps a single surface instead of re-implementing the glue:
//!
//! - [`rest_sql`] — run a REST DSL request end-to-end against a SQL connection
//!   (translate → `Glue::execute` → rows), plus `execute_sql` for one
//!   parameterized (`$N`) non-DDL statement.
//! - [`FtsIndex`] — the durable full-text engine over one logical index: ingest,
//!   delete/update, search, and a policy-driven compaction coordinator +
//!   background scheduler (load manifest/tombstones → `CompactionPolicy` →
//!   `Compactor` → persist → GC).
//! - **SQL-integrated FTS** — [`fts_sql`] is the pre-parse rewrite that turns the
//!   Postgres surface (`to_tsvector(…) @@ *_tsquery(…)`/`ts_rank`, and trigram
//!   `LIKE`) into a `pk IN (…)` query gluesql can run; [`LiveSegment`] is the
//!   in-memory tantivy NRT tier; and [`FtsEngine`] maintains it from a SQL commit
//!   tap (read-your-writes), unions live ∪ durable splits, seals live→durable in
//!   the background, and persists index definitions so they survive restart.
//!
//! Errors from any pillar fold into [`EngineError`].

pub mod error;
pub mod fts;
pub mod fts_engine;
pub mod fts_sql;
pub mod live_segment;
pub mod rest_sql;

pub use error::{EngineError, Result};
pub use fts::{CompactionSummary, FtsIndex};
pub use fts_engine::FtsEngine;
pub use fts_sql::{FtsHit, FtsPredicate, FtsSearcher, TsQueryKind};
pub use live_segment::LiveSegment;

// Re-export the SQL multi-connection handle so callers can build connections
// without depending on `bluedb-sql` directly.
pub use bluedb_sql::{Database, SlateDbStorage};
