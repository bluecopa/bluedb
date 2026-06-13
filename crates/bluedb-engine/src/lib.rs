//! `bluedb-engine` — the facade that unifies bluedb's pillars behind one API.
//!
//! The pillars are deliberately decoupled crates:
//! - [`bluedb_sql`] — SQL over SlateDB (transactions, indexes, tenants);
//! - [`bluedb_rest`] — PostgREST-style DSL → SQL *string* translation;
//! - [`bluedb_fts`] — BM25 full-text search + the index lifecycle pieces;
//! - [`bluedb_storage`] — the SlateDB blob seam.
//!
//! `bluedb-engine` is the one crate that *composes* them, so the upcoming
//! service binary and PyO3 bindings (M3) wrap a single surface instead of
//! re-implementing the glue:
//!
//! - [`rest_sql`] — run a REST DSL request end-to-end against a SQL connection
//!   (translate → `Glue::execute` → rows). Closes the loop `bluedb-rest` leaves
//!   open by design.
//! - [`FtsIndex`] — a full-text engine facade over one logical index: ingest,
//!   delete/update, search, and a policy-driven compaction coordinator +
//!   background scheduler (load manifest/tombstones → `CompactionPolicy` →
//!   `Compactor` → persist → GC).
//!
//! Errors from any pillar fold into [`EngineError`].

pub mod error;
pub mod fts;
pub mod rest_sql;

pub use error::{EngineError, Result};
pub use fts::{CompactionSummary, FtsIndex};

// Re-export the SQL multi-connection handle so callers can build connections
// without depending on `bluedb-sql` directly.
pub use bluedb_sql::{Database, SlateDbStorage};
